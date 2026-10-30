/*
Copyright 2026 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package driver

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	csi "github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilcache "k8s.io/apimachinery/pkg/util/cache"
	"k8s.io/client-go/kubernetes/fake"
	corelisters "k8s.io/client-go/listers/core/v1"
	k8stesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"
	cloud "sigs.k8s.io/gcp-filestore-csi-driver/pkg/cloud_provider"
	"sigs.k8s.io/gcp-filestore-csi-driver/pkg/cloud_provider/file"
	"sigs.k8s.io/gcp-filestore-csi-driver/pkg/metrics"
)

func testNode(name string, addresses ...v1.NodeAddress) *v1.Node {
	return &v1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Status:     v1.NodeStatus{Addresses: addresses},
	}
}

func TestInformerNodeIPResolver(t *testing.T) {
	client := fake.NewSimpleClientset(
		testNode("node-a",
			v1.NodeAddress{Type: v1.NodeExternalIP, Address: "34.1.2.3"},
			v1.NodeAddress{Type: v1.NodeInternalIP, Address: "fd00::1"},
			v1.NodeAddress{Type: v1.NodeInternalIP, Address: "10.128.0.11"},
		),
		testNode("node-ipv6", v1.NodeAddress{Type: v1.NodeInternalIP, Address: "fd00::2"}),
		testNode("node-external", v1.NodeAddress{Type: v1.NodeExternalIP, Address: "34.1.2.4"}),
	)
	nodes := newVolumePoolNodes(client)

	// Before the cache syncs, the node comes from the API server.
	if ip, err := nodes.ips.NodeIP(context.Background(), "node-a"); err != nil || ip != "10.128.0.11" {
		t.Errorf("NodeIP() before the cache synced = %q, %v; want %q, nil", ip, err, "10.128.0.11")
	}

	stopCh := make(chan struct{})
	defer close(stopCh)
	cs := &controllerServer{config: &controllerServerConfig{volumePoolNodes: nodes}}
	cs.startVolumePoolNodes(stopCh)
	nodes.informers.WaitForCacheSync(stopCh)

	tests := []struct {
		node     string
		wantIP   string
		wantCode codes.Code
	}{
		{node: "node-a", wantIP: "10.128.0.11"},
		{node: "node-ipv6", wantCode: codes.FailedPrecondition},
		{node: "node-external", wantCode: codes.FailedPrecondition},
		{node: "missing", wantCode: codes.NotFound},
	}
	for _, tc := range tests {
		t.Run(tc.node, func(t *testing.T) {
			ip, err := nodes.ips.NodeIP(context.Background(), tc.node)
			if code := status.Code(err); code != tc.wantCode {
				t.Fatalf("NodeIP(%q) error = %v (code %v), want code %v", tc.node, err, code, tc.wantCode)
			}
			if ip != tc.wantIP {
				t.Errorf("NodeIP(%q) = %q, want %q", tc.node, ip, tc.wantIP)
			}
		})
	}

	// The cache follows changes to nodes: a node whose VM GKE recreates keeps
	// its name, but can get another IP address.
	ctx := context.Background()
	nodeA, err := client.CoreV1().Nodes().Get(ctx, "node-a", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("failed to get node-a: %v", err)
	}
	nodeA.Status.Addresses = []v1.NodeAddress{{Type: v1.NodeInternalIP, Address: "10.128.0.99"}}
	if _, err := client.CoreV1().Nodes().Update(ctx, nodeA, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("failed to update node-a: %v", err)
	}
	waitForNodeIP(t, nodes.ips, "node-a", "10.128.0.99", codes.OK)

	if err := client.CoreV1().Nodes().Delete(ctx, "node-a", metav1.DeleteOptions{}); err != nil {
		t.Fatalf("failed to delete node-a: %v", err)
	}
	waitForNodeIP(t, nodes.ips, "node-a", "", codes.NotFound)

	// The resolver remembers the address that node-a had when it was
	// deleted, until another node has it.
	waitForDeletedNodeIP(t, nodes.ips, "node-a", "10.128.0.99", true)
	if _, err := client.CoreV1().Nodes().Create(ctx, testNode("node-z", v1.NodeAddress{Type: v1.NodeInternalIP, Address: "10.128.0.99"}), metav1.CreateOptions{}); err != nil {
		t.Fatalf("failed to create node-z: %v", err)
	}
	waitForDeletedNodeIP(t, nodes.ips, "node-a", "", false)
}

// waitForDeletedNodeIP waits until r reports wantIP and wantOK for the deleted
// node, which it does once the API server's change reaches the node cache. It
// fails the test if that takes more than 10 seconds.
func waitForDeletedNodeIP(t *testing.T, r nodeIPResolver, node, wantIP string, wantOK bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		ip, ok := r.DeletedNodeIP(node)
		if ip == wantIP && ok == wantOK {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("DeletedNodeIP(%q) = %q, %v; want %q, %v", node, ip, ok, wantIP, wantOK)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// fakeClock is a utilcache.Clock whose time the test sets.
type fakeClock struct{ now time.Time }

func (c *fakeClock) Now() time.Time { return c.now }

func TestInformerNodeIPResolver_DeletedNodeIP(t *testing.T) {
	// node-b, which is in the cache, now has the address that node-old had.
	indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
	if err := indexer.Add(testNode("node-b", v1.NodeAddress{Type: v1.NodeInternalIP, Address: ipB})); err != nil {
		t.Fatalf("failed to cache node-b: %v", err)
	}
	synced := true
	clock := &fakeClock{now: time.Now()}
	r := &informerNodeIPResolver{
		lister:  corelisters.NewNodeLister(indexer),
		synced:  func() bool { return synced },
		deleted: utilcache.NewLRUExpireCacheWithClock(maxDeletedVolumePoolNodes, clock),
	}
	r.nodeDeleted(testNode("node-gone", v1.NodeAddress{Type: v1.NodeInternalIP, Address: ipD}))
	// A tombstone for a node deleted while the informer was disconnected.
	r.nodeDeleted(cache.DeletedFinalStateUnknown{Key: "node-missed", Obj: testNode("node-missed", v1.NodeAddress{Type: v1.NodeInternalIP, Address: ipC})})
	r.nodeDeleted(cache.DeletedFinalStateUnknown{Key: "node-unknown"})
	r.nodeDeleted(testNode("node-old", v1.NodeAddress{Type: v1.NodeInternalIP, Address: ipB}))
	r.nodeDeleted(testNode("node-ipv6", v1.NodeAddress{Type: v1.NodeInternalIP, Address: "fd00::1"}))

	for _, tc := range []struct {
		node   string
		wantIP string
		wantOK bool
	}{
		{node: "node-gone", wantIP: ipD, wantOK: true},
		{node: "node-missed", wantIP: ipC, wantOK: true},
		{node: "node-unknown"},
		{node: "node-old"},  // node-b has its address now
		{node: "node-ipv6"}, // it had no IPv4 address
		{node: "node-b"},    // not deleted
	} {
		if ip, ok := r.DeletedNodeIP(tc.node); ip != tc.wantIP || ok != tc.wantOK {
			t.Errorf("DeletedNodeIP(%q) = %q, %v; want %q, %v", tc.node, ip, ok, tc.wantIP, tc.wantOK)
		}
	}

	// Until the cache syncs, it can't tell which nodes have the address now.
	synced = false
	if ip, ok := r.DeletedNodeIP("node-gone"); ok {
		t.Errorf("DeletedNodeIP(node-gone) before the cache synced = %q, true; want false", ip)
	}
	synced = true

	// The resolver forgets deleted nodes after a day.
	clock.now = clock.now.Add(deletedVolumePoolNodeTTL + time.Second)
	if ip, ok := r.DeletedNodeIP("node-gone"); ok {
		t.Errorf("DeletedNodeIP(node-gone) a day after the deletion = %q, true; want false", ip)
	}
}

// waitForNodeIP waits until r looks node up as wantIP with an error of code
// wantCode, which the node cache does once the API server's change reaches it.
// It fails the test if that takes more than 10 seconds.
func waitForNodeIP(t *testing.T, r nodeIPResolver, node, wantIP string, wantCode codes.Code) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		ip, err := r.NodeIP(context.Background(), node)
		if ip == wantIP && status.Code(err) == wantCode {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("NodeIP(%q) = %q, %v; want %q, code %v", node, ip, err, wantIP, wantCode)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestInformerNodeIPResolver_CacheCantAnswer(t *testing.T) {
	// The cache has node-a. The API server has node-a and node-b, which it
	// added after the cache last heard from it.
	nodeA := testNode("node-a", v1.NodeAddress{Type: v1.NodeInternalIP, Address: ipA})
	nodeB := testNode("node-b", v1.NodeAddress{Type: v1.NodeInternalIP, Address: ipB})
	forbidden := apierrors.NewForbidden(v1.Resource("nodes"), "node-b", errors.New("the service account may not get nodes"))
	for _, tc := range []struct {
		name   string
		synced bool
		node   string
		// getErr is the error that the API server returns for GETs.
		getErr   error
		wantIP   string
		wantCode codes.Code
		wantMsg  string
		wantGets int
	}{
		{name: "in the cache", synced: true, node: "node-a", wantIP: ipA},
		{name: "not in the cache yet", synced: true, node: "node-b", wantIP: ipB, wantGets: 1},
		{name: "cache not synced", node: "node-a", wantIP: ipA, wantGets: 1},
		{name: "deleted", synced: true, node: "node-c", wantCode: codes.NotFound, wantGets: 1},
		{name: "API server refuses", synced: true, node: "node-b", getErr: forbidden, wantCode: codes.Unavailable, wantMsg: "may not get nodes", wantGets: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
			if err := indexer.Add(nodeA); err != nil {
				t.Fatalf("failed to cache node-a: %v", err)
			}
			client := fake.NewSimpleClientset(nodeA, nodeB)
			gets := 0
			client.PrependReactor("get", "nodes", func(k8stesting.Action) (bool, runtime.Object, error) {
				gets++
				if tc.getErr != nil {
					return true, nil, tc.getErr
				}
				return false, nil, nil
			})
			r := &informerNodeIPResolver{
				lister: corelisters.NewNodeLister(indexer),
				synced: func() bool { return tc.synced },
				client: client,
			}

			ip, err := r.NodeIP(context.Background(), tc.node)
			if code := status.Code(err); code != tc.wantCode {
				t.Fatalf("NodeIP(%q) error = %v (code %v), want code %v", tc.node, err, code, tc.wantCode)
			}
			if msg := status.Convert(err).Message(); !strings.Contains(msg, tc.wantMsg) {
				t.Errorf("NodeIP(%q) error message = %q, want it to contain %q", tc.node, msg, tc.wantMsg)
			}
			if ip != tc.wantIP {
				t.Errorf("NodeIP(%q) = %q, want %q", tc.node, ip, tc.wantIP)
			}
			if gets != tc.wantGets {
				t.Errorf("NodeIP(%q) got the node from the API server %d times, want %d", tc.node, gets, tc.wantGets)
			}
		})
	}
}

func TestNewVolumePoolNodes_CachesOnlyAddresses(t *testing.T) {
	node := testNode("node-a", v1.NodeAddress{Type: v1.NodeInternalIP, Address: ipA})
	node.Labels = map[string]string{"cloud.google.com/gke-nodepool": "default-pool"}
	node.Status.Conditions = []v1.NodeCondition{{Type: v1.NodeReady, Status: v1.ConditionTrue}}
	node.Status.Images = []v1.ContainerImage{{Names: []string{"registry.example/agent:1"}, SizeBytes: 1 << 20}}
	nodes := newVolumePoolNodes(fake.NewSimpleClientset(node))

	stopCh := make(chan struct{})
	defer close(stopCh)
	nodes.informers.Start(stopCh)
	nodes.informers.WaitForCacheSync(stopCh)

	cached, err := nodes.informers.Core().V1().Nodes().Lister().Get("node-a")
	if err != nil {
		t.Fatalf("node-a is not in the node cache: %v", err)
	}
	want := &v1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "node-a"},
		Status:     v1.NodeStatus{Addresses: node.Status.Addresses},
	}
	// The fake API server may or may not set resource versions.
	if diff := cmp.Diff(want, cached, cmpopts.IgnoreFields(metav1.ObjectMeta{}, "ResourceVersion")); diff != "" {
		t.Errorf("cached node-a diff (-want +got):\n%s", diff)
	}
}

func TestStripVolumePoolNode(t *testing.T) {
	addresses := []v1.NodeAddress{
		{Type: v1.NodeInternalIP, Address: ipA},
		{Type: v1.NodeHostName, Address: "node-a"},
	}
	node := &v1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "node-a",
			ResourceVersion: "42",
			UID:             "0b5d6a3e",
			Labels:          map[string]string{"cloud.google.com/gke-nodepool": "default-pool"},
			Annotations:     map[string]string{"node.alpha.kubernetes.io/ttl": "0"},
			ManagedFields:   []metav1.ManagedFieldsEntry{{Manager: "kubelet"}},
		},
		Spec: v1.NodeSpec{PodCIDR: "10.4.0.0/24"},
		Status: v1.NodeStatus{
			Addresses:  slices.Clone(addresses),
			Conditions: []v1.NodeCondition{{Type: v1.NodeReady, Status: v1.ConditionTrue}},
			Images:     []v1.ContainerImage{{Names: []string{"registry.example/agent:1"}, SizeBytes: 1 << 20}},
		},
	}
	want := &v1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "node-a", ResourceVersion: "42"},
		Status:     v1.NodeStatus{Addresses: addresses},
	}

	got, err := stripVolumePoolNode(node)
	if err != nil {
		t.Fatalf("stripVolumePoolNode() error = %v", err)
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("stripVolumePoolNode() diff (-want +got):\n%s", diff)
	}
	// The copy doesn't share its addresses with the original.
	if stripped, ok := got.(*v1.Node); ok && len(stripped.Status.Addresses) > 0 {
		stripped.Status.Addresses[0].Address = ipB
		if node.Status.Addresses[0].Address != ipA {
			t.Errorf("changing the stripped Node changed the original's addresses")
		}
	}

	// A tombstone holds the node as cached, which is already stripped.
	tombstone := cache.DeletedFinalStateUnknown{Key: "node-b", Obj: testNode("node-b")}
	got, err = stripVolumePoolNode(tombstone)
	if err != nil {
		t.Fatalf("stripVolumePoolNode(tombstone) error = %v", err)
	}
	if diff := cmp.Diff(tombstone, got); diff != "" {
		t.Errorf("stripVolumePoolNode(tombstone) diff (-want +got):\n%s", diff)
	}
}

const (
	logNodeCacheSynced    = "Volume Pool volumes synced after"
	logNodeCacheNotSynced = "Volume Pool volumes has not synced after"
)

func TestWaitForVolumePoolNodeCache(t *testing.T) {
	for _, tc := range []struct {
		name string
		// syncAfter is how many times synced reports false before it
		// reports true; -1 means it never does.
		syncAfter  int
		warnAfter  time.Duration
		stopAfter  time.Duration
		wantSynced bool
		wantLogs   []string
		noLogs     []string
	}{
		{
			name:       "already synced",
			warnAfter:  time.Hour,
			wantSynced: true,
			wantLogs:   []string{logNodeCacheSynced},
			noLogs:     []string{logNodeCacheNotSynced},
		},
		{
			name:       "synced soon",
			syncAfter:  3,
			warnAfter:  time.Hour,
			wantSynced: true,
			wantLogs:   []string{logNodeCacheSynced},
			noLogs:     []string{logNodeCacheNotSynced},
		},
		{
			name:       "synced after the warning",
			syncAfter:  3,
			warnAfter:  10 * time.Millisecond,
			wantSynced: true,
			wantLogs:   []string{logNodeCacheNotSynced + " 10ms", logNodeCacheSynced},
		},
		{
			name:      "never synced",
			syncAfter: -1,
			warnAfter: 10 * time.Millisecond,
			stopAfter: 300 * time.Millisecond,
			wantLogs:  []string{logNodeCacheNotSynced + " 10ms"},
			noLogs:    []string{logNodeCacheSynced},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			logs := captureKlog(t)
			calls := 0
			synced := func() bool {
				calls++
				return tc.syncAfter >= 0 && calls > tc.syncAfter
			}
			stopCh := make(chan struct{})
			if tc.stopAfter > 0 {
				timer := time.AfterFunc(tc.stopAfter, func() { close(stopCh) })
				defer timer.Stop()
			}

			if got := waitForVolumePoolNodeCache(stopCh, synced, tc.warnAfter); got != tc.wantSynced {
				t.Errorf("waitForVolumePoolNodeCache() = %v, want %v", got, tc.wantSynced)
			}
			got := logs()
			for _, want := range tc.wantLogs {
				if !strings.Contains(got, want) {
					t.Errorf("logs don't contain %q:\n%s", want, got)
				}
			}
			for _, unwanted := range tc.noLogs {
				if strings.Contains(got, unwanted) {
					t.Errorf("logs contain %q:\n%s", unwanted, got)
				}
			}
		})
	}
}

func TestStartVolumePoolNodes_LogsCacheSync(t *testing.T) {
	logs := captureKlog(t)
	cs := &controllerServer{config: &controllerServerConfig{volumePoolNodes: newVolumePoolNodes(fake.NewSimpleClientset())}}
	stopCh := make(chan struct{})
	defer close(stopCh)
	cs.startVolumePoolNodes(stopCh)

	deadline := time.Now().Add(10 * time.Second)
	for !strings.Contains(logs(), logNodeCacheSynced) {
		if time.Now().After(deadline) {
			t.Fatalf("no log says that the node cache synced:\n%s", logs())
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// captureKlog sends klog's output to a buffer until the test ends, and returns
// a function that returns what was logged so far.
func captureKlog(t *testing.T) func() string {
	t.Helper()
	buf := &syncBuffer{}
	klog.SetOutput(buf)
	klog.LogToStderr(false)
	t.Cleanup(func() {
		klog.LogToStderr(true)
		klog.SetOutput(os.Stderr)
	})
	return func() string {
		klog.Flush()
		return buf.String()
	}
}

// syncBuffer is a bytes.Buffer that is safe for concurrent use.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func TestNewGCFSDriver_VolumePoolNodes(t *testing.T) {
	for _, tc := range []struct {
		name      string
		feature   *FeatureVolumePools
		wantNodes bool
	}{
		{name: "feature off", feature: &FeatureVolumePools{Enabled: false, KubeClient: fake.NewSimpleClientset()}},
		{name: "feature on without a client", feature: &FeatureVolumePools{Enabled: true}},
		{name: "feature on with a client", feature: &FeatureVolumePools{Enabled: true, KubeClient: fake.NewSimpleClientset()}, wantNodes: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			driver := initTestControllerDriver(t, tc.feature)
			if got := driver.cs.(*controllerServer).config.volumePoolNodes != nil; got != tc.wantNodes {
				t.Errorf("controller has a node lookup = %v, want %v", got, tc.wantNodes)
			}
		})
	}
}

// initTestControllerDriver returns a controller driver with the given Volume
// Pools feature options.
func initTestControllerDriver(t *testing.T, volumePools *FeatureVolumePools) *GCFSDriver {
	t.Helper()
	c, err := cloud.NewFakeCloud()
	if err != nil {
		t.Fatalf("failed to init cloud: %v", err)
	}
	driver, err := NewGCFSDriver(&GCFSDriverConfig{
		Name:          "test-driver",
		Version:       "test-version",
		RunController: true,
		Cloud:         c,
		FeatureOptions: &GCFSDriverFeatureOptions{
			FeatureLockRelease: &FeatureLockRelease{},
			FeatureVolumePools: volumePools,
		},
	})
	if err != nil {
		t.Fatalf("failed to init driver: %v", err)
	}
	return driver
}

// fakeNodeIPs maps node names to IP addresses. A node mapped to "" has no
// internal IPv4 address.
type fakeNodeIPs map[string]string

func (f fakeNodeIPs) NodeIP(_ context.Context, nodeName string) (string, error) {
	ip, ok := f[nodeName]
	if !ok {
		return "", status.Errorf(codes.NotFound, "node %q not found", nodeName)
	}
	if ip == "" {
		return "", status.Errorf(codes.FailedPrecondition, "node %q has no internal IPv4 address", nodeName)
	}
	return ip, nil
}

func (fakeNodeIPs) DeletedNodeIP(string) (string, bool) { return "", false }

// fakeNodeIPsWithDeleted adds nodes deleted recently, mapped to the IP
// addresses they had, to fakeNodeIPs.
type fakeNodeIPsWithDeleted struct {
	fakeNodeIPs
	deleted map[string]string
}

func (f fakeNodeIPsWithDeleted) DeletedNodeIP(nodeName string) (string, bool) {
	ip, ok := f.deleted[nodeName]
	return ip, ok
}

// unavailableNodeIPs fails every lookup, as the node lookup does when neither
// the node cache nor the API server can answer.
type unavailableNodeIPs struct{}

func (unavailableNodeIPs) NodeIP(context.Context, string) (string, error) {
	return "", status.Error(codes.Unavailable, "failed to get node: the API server can't be reached")
}

func (unavailableNodeIPs) DeletedNodeIP(string) (string, bool) { return "", false }

// publishTestService wraps volumePoolTestService to count, and optionally
// override, IP ACL updates.
type publishTestService struct {
	*volumePoolTestService
	updateIpAcls func(ctx context.Context, name string, acls []file.VolumePoolNfsExportOptions) (*file.PoolVolume, error)
}

func (s *publishTestService) UpdateVolumePoolVolumeIpAcls(ctx context.Context, name string, acls []file.VolumePoolNfsExportOptions) (*file.PoolVolume, error) {
	s.calls["UpdateVolumePoolVolumeIpAcls"]++
	if s.updateIpAcls != nil {
		return s.updateIpAcls(ctx, name, acls)
	}
	return s.volumePoolTestService.UpdateVolumePoolVolumeIpAcls(ctx, name, acls)
}

// volumeWithIpAcls returns a getVolume override that reports every volume in
// state, with IP ACLs acls.
func volumeWithIpAcls(state string, acls ...file.VolumePoolNfsExportOptions) func(context.Context, string) (*file.PoolVolume, error) {
	inState := volumeInState(state)
	return func(ctx context.Context, name string) (*file.PoolVolume, error) {
		vol, err := inState(ctx, name)
		if err != nil {
			return nil, err
		}
		vol.IpAcls = acls
		return vol, nil
	}
}

const (
	ipA = "10.128.0.11"
	ipB = "10.128.0.12"
	ipC = "10.128.0.13"
	ipD = "10.128.0.14"
)

// newPublishTestController returns a controller whose volume pool has the HOT
// volume testSourceVolumeName, and that knows nodes node-a, node-b and node-c,
// with IP addresses ipA, ipB and ipC, and node-ipv6, which has no IPv4 address.
func newPublishTestController(t *testing.T) (*controllerServer, *publishTestService) {
	t.Helper()
	cs, fs := newVolumePoolTestController(t)
	ps := &publishTestService{volumePoolTestService: fs}
	cs.config.fileService = ps
	cs.config.volumePoolNodes = &volumePoolNodes{ips: fakeNodeIPs{
		"node-a":    ipA,
		"node-b":    ipB,
		"node-c":    ipC,
		"node-ipv6": "",
	}}
	return cs, ps
}

func publishRequest(nodeID string, mode csi.VolumeCapability_AccessMode_Mode) *csi.ControllerPublishVolumeRequest {
	return &csi.ControllerPublishVolumeRequest{
		VolumeId: testSourceVolumeID,
		NodeId:   nodeID,
		VolumeCapability: &csi.VolumeCapability{
			AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{}},
			AccessMode: &csi.VolumeCapability_AccessMode{Mode: mode},
		},
	}
}

// acl returns the IP ACL entry that publishing gives the nodes with IP
// addresses ipRanges.
func acl(accessMode string, ipRanges ...string) file.VolumePoolNfsExportOptions {
	return file.VolumePoolNfsExportOptions{
		IpRanges:        ipRanges,
		AccessMode:      accessMode,
		SquashMode:      file.VolumePoolSquashModeNoRoot,
		SecurityFlavors: []string{file.VolumePoolSecurityFlavorAuthSys},
	}
}

func rw(ipRanges ...string) file.VolumePoolNfsExportOptions {
	return acl(file.VolumePoolAccessModeReadWrite, ipRanges...)
}

func ro(ipRanges ...string) file.VolumePoolNfsExportOptions {
	return acl(file.VolumePoolAccessModeReadOnly, ipRanges...)
}

// newVolumeIpAcls returns the IP ACLs that the API, and the fake, give a new
// volume: read-write access for all private IPv4 ranges.
func newVolumeIpAcls() []file.VolumePoolNfsExportOptions {
	return []file.VolumePoolNfsExportOptions{{
		IpRanges:        []string{"10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16"},
		AccessMode:      file.VolumePoolAccessModeReadWrite,
		SquashMode:      file.VolumePoolSquashModeNoRoot,
		SecurityFlavors: []string{file.VolumePoolSecurityFlavorAuthSys},
	}}
}

// setTestVolumeIpAcls sets the IP ACLs of testSourceVolumeName in the fake.
func setTestVolumeIpAcls(t *testing.T, fs *publishTestService, acls []file.VolumePoolNfsExportOptions) {
	t.Helper()
	if _, err := fs.Service.UpdateVolumePoolVolumeIpAcls(context.Background(), testSourceVolumeName, acls); err != nil {
		t.Fatalf("failed to set IP ACLs: %v", err)
	}
}

// testVolumeIpAcls returns the IP ACLs of testSourceVolumeName in the fake.
func testVolumeIpAcls(t *testing.T, fs *publishTestService) []file.VolumePoolNfsExportOptions {
	t.Helper()
	vol, err := fs.Service.GetVolumePoolVolume(context.Background(), testSourceVolumeName)
	if err != nil {
		t.Fatalf("failed to get volume: %v", err)
	}
	return vol.IpAcls
}

func TestControllerPublishVolume_VolumePool(t *testing.T) {
	ctx := context.Background()
	snw := csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER
	mnmw := csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER

	var manyRanges []file.VolumePoolNfsExportOptions
	for i := 1; i <= maxVolumePoolIpAclRanges; i++ {
		manyRanges = append(manyRanges, rw(fmt.Sprintf("10.0.0.%d", i)))
	}

	tests := []struct {
		name string
		// initialAcls replaces the IP ACLs of the new volume, which allow all
		// private IPv4 ranges, if it is set.
		initialAcls []file.VolumePoolNfsExportOptions
		setup       func(cs *controllerServer, fs *publishTestService)
		req         *csi.ControllerPublishVolumeRequest
		wantCode    codes.Code
		// For requests that reach the volume: its IP ACLs afterwards, and how
		// many times they were updated.
		wantAcls    []file.VolumePoolNfsExportOptions
		wantUpdates int
	}{
		{
			name:        "exports a new volume to the node only",
			req:         publishRequest("node-a", snw),
			wantAcls:    []file.VolumePoolNfsExportOptions{rw(ipA)},
			wantUpdates: 1,
		},
		{
			name:        "publishing again changes nothing",
			initialAcls: []file.VolumePoolNfsExportOptions{rw(ipA)},
			req:         publishRequest("node-a", snw),
			wantAcls:    []file.VolumePoolNfsExportOptions{rw(ipA)},
		},
		{
			name: "read-only publish",
			req: func() *csi.ControllerPublishVolumeRequest {
				req := publishRequest("node-a", snw)
				req.Readonly = true
				return req
			}(),
			wantAcls:    []file.VolumePoolNfsExportOptions{ro(ipA)},
			wantUpdates: 1,
		},
		{
			name:        "reader-only access mode",
			req:         publishRequest("node-a", csi.VolumeCapability_AccessMode_MULTI_NODE_READER_ONLY),
			wantAcls:    []file.VolumePoolNfsExportOptions{ro(ipA)},
			wantUpdates: 1,
		},
		{
			name:        "single-node publish replaces another node",
			initialAcls: []file.VolumePoolNfsExportOptions{rw(ipB)},
			req:         publishRequest("node-a", snw),
			wantAcls:    []file.VolumePoolNfsExportOptions{rw(ipA)},
			wantUpdates: 1,
		},
		{
			name:        "single-node publish replaces another node and wider ranges",
			initialAcls: append(newVolumeIpAcls(), rw(ipB+"/32")),
			req:         publishRequest("node-a", snw),
			wantAcls:    []file.VolumePoolNfsExportOptions{rw(ipA)},
			wantUpdates: 1,
		},
		{
			name:        "single-node publish refuses a volume that looks shared",
			initialAcls: []file.VolumePoolNfsExportOptions{rw(ipB), rw(ipC)},
			req:         publishRequest("node-a", snw),
			wantCode:    codes.FailedPrecondition,
			wantAcls:    []file.VolumePoolNfsExportOptions{rw(ipB), rw(ipC)},
		},
		{
			name:        "single-node publish refuses a volume that looks shared, with wider ranges",
			initialAcls: append(newVolumeIpAcls(), rw(ipB, ipC)),
			req:         publishRequest("node-a", snw),
			wantCode:    codes.FailedPrecondition,
			wantAcls:    append(newVolumeIpAcls(), rw(ipB, ipC)),
		},
		{
			name:        "single-node publish refuses a volume that the node shares with another",
			initialAcls: []file.VolumePoolNfsExportOptions{rw(ipA), rw(ipB)},
			req:         publishRequest("node-a", snw),
			wantCode:    codes.FailedPrecondition,
			wantAcls:    []file.VolumePoolNfsExportOptions{rw(ipA), rw(ipB)},
		},
		{
			name:        "single-node publish refuses a volume that the node shares with another, in one entry",
			initialAcls: []file.VolumePoolNfsExportOptions{rw(ipA, ipB+"/32")},
			req:         publishRequest("node-a", snw),
			wantCode:    codes.FailedPrecondition,
			wantAcls:    []file.VolumePoolNfsExportOptions{rw(ipA, ipB+"/32")},
		},
		{
			name:        "single-node publish rewrites the node's /32 entry",
			initialAcls: []file.VolumePoolNfsExportOptions{rw(ipA + "/32")},
			req:         publishRequest("node-a", snw),
			wantAcls:    []file.VolumePoolNfsExportOptions{rw(ipA)},
			wantUpdates: 1,
		},
		{
			name:        "republishing read-only to a node published read-write fails",
			initialAcls: []file.VolumePoolNfsExportOptions{rw(ipA)},
			req: func() *csi.ControllerPublishVolumeRequest {
				req := publishRequest("node-a", snw)
				req.Readonly = true
				return req
			}(),
			wantCode: codes.AlreadyExists,
			wantAcls: []file.VolumePoolNfsExportOptions{rw(ipA)},
		},
		{
			name:        "republishing read-write to a node published read-only fails",
			initialAcls: []file.VolumePoolNfsExportOptions{ro(ipA)},
			req:         publishRequest("node-a", snw),
			wantCode:    codes.AlreadyExists,
			wantAcls:    []file.VolumePoolNfsExportOptions{ro(ipA)},
		},
		{
			name:        "republishing with another access mode to the node's /32 entry fails",
			initialAcls: []file.VolumePoolNfsExportOptions{ro(ipA + "/32")},
			req:         publishRequest("node-a", snw),
			wantCode:    codes.AlreadyExists,
			wantAcls:    []file.VolumePoolNfsExportOptions{ro(ipA + "/32")},
		},
		{
			name:        "multi-node republishing with a reader-only mode fails",
			initialAcls: []file.VolumePoolNfsExportOptions{rw(ipA), rw(ipB)},
			req:         publishRequest("node-a", csi.VolumeCapability_AccessMode_MULTI_NODE_READER_ONLY),
			wantCode:    codes.AlreadyExists,
			wantAcls:    []file.VolumePoolNfsExportOptions{rw(ipA), rw(ipB)},
		},
		{
			name:        "an entry without an access mode is read-write",
			initialAcls: []file.VolumePoolNfsExportOptions{{IpRanges: []string{ipA}, SquashMode: file.VolumePoolSquashModeNoRoot}},
			req:         publishRequest("node-a", snw),
			wantAcls:    []file.VolumePoolNfsExportOptions{rw(ipA)},
			wantUpdates: 1,
		},
		{
			name: "an entry for the node without a security flavor gets AUTH_SYS",
			initialAcls: []file.VolumePoolNfsExportOptions{{
				IpRanges:   []string{ipA},
				AccessMode: file.VolumePoolAccessModeReadWrite,
				SquashMode: file.VolumePoolSquashModeNoRoot,
			}},
			req:         publishRequest("node-a", snw),
			wantAcls:    []file.VolumePoolNfsExportOptions{rw(ipA)},
			wantUpdates: 1,
		},
		{
			name:        "multi-node publish exports a new volume to the node only",
			req:         publishRequest("node-a", mnmw),
			wantAcls:    []file.VolumePoolNfsExportOptions{rw(ipA)},
			wantUpdates: 1,
		},
		{
			name:        "multi-node publish adds the node",
			initialAcls: []file.VolumePoolNfsExportOptions{rw(ipB)},
			req:         publishRequest("node-a", mnmw),
			wantAcls:    []file.VolumePoolNfsExportOptions{rw(ipB), rw(ipA)},
			wantUpdates: 1,
		},
		{
			name:        "multi-node publish keeps the other nodes but not wider ranges",
			initialAcls: append(newVolumeIpAcls(), rw(ipB)),
			req:         publishRequest("node-a", mnmw),
			wantAcls:    []file.VolumePoolNfsExportOptions{rw(ipB), rw(ipA)},
			wantUpdates: 1,
		},
		{
			name:        "multi-node publish keeps the node's place",
			initialAcls: []file.VolumePoolNfsExportOptions{rw(ipA + "/32"), rw(ipB)},
			req:         publishRequest("node-a", mnmw),
			wantAcls:    []file.VolumePoolNfsExportOptions{rw(ipA), rw(ipB)},
			wantUpdates: 1,
		},
		{
			name:        "multi-node publish again changes nothing",
			initialAcls: []file.VolumePoolNfsExportOptions{rw(ipB), rw(ipA)},
			req:         publishRequest("node-a", mnmw),
			wantAcls:    []file.VolumePoolNfsExportOptions{rw(ipB), rw(ipA)},
		},
		{
			name:        "multi-node publish over the IP range limit",
			initialAcls: manyRanges,
			req:         publishRequest("node-a", mnmw),
			wantCode:    codes.ResourceExhausted,
			wantAcls:    manyRanges,
		},
		{
			name:     "node not found",
			req:      publishRequest("node-missing", snw),
			wantCode: codes.NotFound,
		},
		{
			name:     "node without an IPv4 address",
			req:      publishRequest("node-ipv6", snw),
			wantCode: codes.FailedPrecondition,
		},
		{
			name: "no node lookup",
			setup: func(cs *controllerServer, _ *publishTestService) {
				cs.config.volumePoolNodes = nil
			},
			req:      publishRequest("node-a", snw),
			wantCode: codes.Internal,
		},
		{
			name: "node lookup unavailable",
			setup: func(cs *controllerServer, _ *publishTestService) {
				cs.config.volumePoolNodes = &volumePoolNodes{ips: unavailableNodeIPs{}}
			},
			req:      publishRequest("node-a", snw),
			wantCode: codes.Unavailable,
		},
		{
			name: "volume not found",
			req: func() *csi.ControllerPublishVolumeRequest {
				req := publishRequest("node-a", snw)
				req.VolumeId = "volumepool://test-project/us-central1/my-pool/volumes/missing?ip=10.1.1.1"
				return req
			}(),
			wantCode: codes.NotFound,
		},
		{
			name: "COLD volume",
			setup: func(_ *controllerServer, fs *publishTestService) {
				fs.getVolume = volumeInState(file.VolumePoolVolumeStateCold)
			},
			req:      publishRequest("node-a", snw),
			wantCode: codes.FailedPrecondition,
		},
		{
			name: "volume still being created",
			setup: func(_ *controllerServer, fs *publishTestService) {
				fs.getVolume = volumeInState(file.VolumePoolVolumeStateCreating)
			},
			req:      publishRequest("node-a", snw),
			wantCode: codes.Aborted,
		},
		{
			name: "WARM volume",
			setup: func(_ *controllerServer, fs *publishTestService) {
				fs.getVolume = volumeInState(file.VolumePoolVolumeStateWarm)
			},
			req:      publishRequest("node-a", snw),
			wantCode: codes.FailedPrecondition,
		},
		{
			name: "volume busy with another operation",
			setup: func(_ *controllerServer, fs *publishTestService) {
				fs.updateIpAcls = func(context.Context, string, []file.VolumePoolNfsExportOptions) (*file.PoolVolume, error) {
					return nil, status.Error(codes.FailedPrecondition, "another operation is in progress")
				}
			},
			req:         publishRequest("node-a", snw),
			wantCode:    codes.Aborted,
			wantUpdates: 1,
		},
		{
			name: "volume deleted before its IP ACLs are updated",
			setup: func(_ *controllerServer, fs *publishTestService) {
				fs.updateIpAcls = func(context.Context, string, []file.VolumePoolNfsExportOptions) (*file.PoolVolume, error) {
					return nil, status.Error(codes.NotFound, "volume not found")
				}
			},
			req:         publishRequest("node-a", snw),
			wantCode:    codes.NotFound,
			wantUpdates: 1,
		},
		{
			name: "another IP ACL change is in progress",
			setup: func(cs *controllerServer, _ *publishTestService) {
				cs.config.volumeLocks.TryAcquire(testSourceVolumeName + volumePoolIpAclsLockSuffix)
			},
			req:      publishRequest("node-a", snw),
			wantCode: codes.Aborted,
		},
		{
			name: "a snapshot holding the volume lock doesn't block publishing",
			setup: func(cs *controllerServer, _ *publishTestService) {
				cs.config.volumeLocks.TryAcquire(testSourceVolumeName)
			},
			req:         publishRequest("node-a", snw),
			wantAcls:    []file.VolumePoolNfsExportOptions{rw(ipA)},
			wantUpdates: 1,
		},
		{
			name: "Volume Pools feature disabled",
			setup: func(cs *controllerServer, _ *publishTestService) {
				cs.config.features.FeatureVolumePools.Enabled = false
			},
			req:      publishRequest("node-a", snw),
			wantCode: codes.Unimplemented,
		},
		{
			name: "malformed volume pool volume ID",
			req: func() *csi.ControllerPublishVolumeRequest {
				req := publishRequest("node-a", snw)
				req.VolumeId = "volumepool://test-project/us-central1/my-pool/volumes/src-vol/extra?ip=10.1.1.1"
				return req
			}(),
			wantCode: codes.InvalidArgument,
		},
		{
			name: "missing volume ID",
			req: func() *csi.ControllerPublishVolumeRequest {
				req := publishRequest("node-a", snw)
				req.VolumeId = ""
				return req
			}(),
			wantCode: codes.InvalidArgument,
		},
		{
			name:     "missing node ID",
			req:      publishRequest("", snw),
			wantCode: codes.InvalidArgument,
		},
		{
			name: "missing volume capability",
			req: func() *csi.ControllerPublishVolumeRequest {
				req := publishRequest("node-a", snw)
				req.VolumeCapability = nil
				return req
			}(),
			wantCode: codes.InvalidArgument,
		},
		{
			name: "block access type",
			req: func() *csi.ControllerPublishVolumeRequest {
				req := publishRequest("node-a", snw)
				req.VolumeCapability.AccessType = &csi.VolumeCapability_Block{Block: &csi.VolumeCapability_BlockVolume{}}
				return req
			}(),
			wantCode: codes.InvalidArgument,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cs, fs := newPublishTestController(t)
			if tc.initialAcls != nil {
				setTestVolumeIpAcls(t, fs, tc.initialAcls)
			}
			if tc.setup != nil {
				tc.setup(cs, fs)
			}
			before := testVolumeIpAcls(t, fs)

			resp, err := cs.ControllerPublishVolume(ctx, tc.req)
			expectCode(t, err, tc.wantCode)
			if err == nil && len(resp.GetPublishContext()) != 0 {
				t.Errorf("PublishContext = %v, want none", resp.GetPublishContext())
			}
			if got := fs.calls["UpdateVolumePoolVolumeIpAcls"]; got != tc.wantUpdates {
				t.Errorf("IP ACLs updated %d times, want %d", got, tc.wantUpdates)
			}
			after := testVolumeIpAcls(t, fs)
			if err != nil {
				// A failed publish leaves the IP ACLs as they were.
				if diff := cmp.Diff(before, after); diff != "" {
					t.Errorf("IP ACLs changed although the publish failed (-before +after):\n%s", diff)
				}
			}
			if tc.wantAcls != nil {
				if diff := cmp.Diff(tc.wantAcls, after); diff != "" {
					t.Errorf("IP ACLs mismatch (-want +got):\n%s", diff)
				}
			}
		})
	}
}

func TestControllerPublishVolume_NotVolumePool(t *testing.T) {
	cs, fs := newPublishTestController(t)
	req := publishRequest("node-a", csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER)
	req.VolumeId = "modeInstance/us-central1-c/myinstance/myshare"

	_, err := cs.ControllerPublishVolume(context.Background(), req)
	expectCode(t, err, codes.OK)
	if len(fs.calls) != 0 {
		t.Errorf("file service called %v, want no calls", fs.calls)
	}
}

// Without the Volume Pools feature, the controller doesn't advertise
// PUBLISH_UNPUBLISH_VOLUME: both calls return Unimplemented, whatever the
// request, as they did before they were implemented.
func TestControllerPublishUnpublish_FeatureOff(t *testing.T) {
	ctx := context.Background()
	featureOff := func(cs *controllerServer) { cs.config.features.FeatureVolumePools.Enabled = false }
	for _, tc := range []struct {
		name     string
		setup    func(cs *controllerServer)
		volumeID string
	}{
		{name: "volume pool volume", setup: featureOff, volumeID: testSourceVolumeID},
		{name: "instance volume", setup: featureOff, volumeID: "modeInstance/us-central1-c/myinstance/myshare"},
		{name: "no volume ID", setup: featureOff, volumeID: ""},
		{name: "no feature options", setup: func(cs *controllerServer) { cs.config.features = nil }, volumeID: testSourceVolumeID},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cs, fs := newPublishTestController(t)
			tc.setup(cs)

			pubReq := publishRequest("node-a", csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER)
			pubReq.VolumeId = tc.volumeID
			_, err := cs.ControllerPublishVolume(ctx, pubReq)
			expectCode(t, err, codes.Unimplemented)

			_, err = cs.ControllerUnpublishVolume(ctx, &csi.ControllerUnpublishVolumeRequest{VolumeId: tc.volumeID, NodeId: "node-a"})
			expectCode(t, err, codes.Unimplemented)

			if len(fs.calls) != 0 {
				t.Errorf("file service called %v, want no calls", fs.calls)
			}
		})
	}
}

func TestControllerUnpublishVolume_VolumePool(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name        string
		initialAcls []file.VolumePoolNfsExportOptions
		setup       func(cs *controllerServer, fs *publishTestService)
		volumeID    string
		nodeID      string
		wantCode    codes.Code
		wantAcls    []file.VolumePoolNfsExportOptions
		wantUpdates int
	}{
		{
			name:        "removes the node",
			initialAcls: []file.VolumePoolNfsExportOptions{rw(ipA)},
			nodeID:      "node-a",
			wantAcls:    nil,
			wantUpdates: 1,
		},
		{
			name:        "keeps the other nodes",
			initialAcls: []file.VolumePoolNfsExportOptions{rw(ipA), rw(ipB)},
			nodeID:      "node-a",
			wantAcls:    []file.VolumePoolNfsExportOptions{rw(ipB)},
			wantUpdates: 1,
		},
		{
			name:        "removes the node from an entry with several ranges",
			initialAcls: []file.VolumePoolNfsExportOptions{rw(ipA, ipB)},
			nodeID:      "node-a",
			wantAcls:    []file.VolumePoolNfsExportOptions{rw(ipB)},
			wantUpdates: 1,
		},
		{
			name:        "removes the node's /32 entry",
			initialAcls: []file.VolumePoolNfsExportOptions{rw(ipA + "/32")},
			nodeID:      "node-a",
			wantUpdates: 1,
		},
		{
			name:        "node not published",
			initialAcls: []file.VolumePoolNfsExportOptions{rw(ipB)},
			nodeID:      "node-a",
			wantAcls:    []file.VolumePoolNfsExportOptions{rw(ipB)},
		},
		{
			name:        "node not published leaves a new volume's IP ACLs",
			initialAcls: newVolumeIpAcls(),
			nodeID:      "node-a",
			wantAcls:    newVolumeIpAcls(),
		},
		{
			name:        "empty node ID removes every node",
			initialAcls: []file.VolumePoolNfsExportOptions{rw(ipA), rw(ipB)},
			nodeID:      "",
			wantAcls:    nil,
			wantUpdates: 1,
		},
		{
			name:        "empty node ID removes wider ranges too",
			initialAcls: append(newVolumeIpAcls(), rw(ipA)),
			nodeID:      "",
			wantAcls:    nil,
			wantUpdates: 1,
		},
		{
			name:        "node not found leaves the IP ACLs",
			initialAcls: []file.VolumePoolNfsExportOptions{rw(ipA)},
			nodeID:      "node-missing",
			wantAcls:    []file.VolumePoolNfsExportOptions{rw(ipA)},
		},
		{
			// node-gone is gone, but the controller saw it deleted.
			name:        "removes the address of a deleted node",
			initialAcls: []file.VolumePoolNfsExportOptions{rw(ipD)},
			setup: func(cs *controllerServer, _ *publishTestService) {
				cs.config.volumePoolNodes = &volumePoolNodes{ips: fakeNodeIPsWithDeleted{
					fakeNodeIPs: fakeNodeIPs{"node-a": ipA},
					deleted:     map[string]string{"node-gone": ipD},
				}}
			},
			nodeID:      "node-gone",
			wantAcls:    nil,
			wantUpdates: 1,
		},
		{
			// GKE deleted node-a and created it again with address ipA; it
			// had address ipD before.
			name:        "removes the old and new addresses of a node created again",
			initialAcls: []file.VolumePoolNfsExportOptions{rw(ipD), rw(ipA), rw(ipB)},
			setup: func(cs *controllerServer, _ *publishTestService) {
				cs.config.volumePoolNodes = &volumePoolNodes{ips: fakeNodeIPsWithDeleted{
					fakeNodeIPs: fakeNodeIPs{"node-a": ipA, "node-b": ipB},
					deleted:     map[string]string{"node-a": ipD},
				}}
			},
			nodeID:      "node-a",
			wantAcls:    []file.VolumePoolNfsExportOptions{rw(ipB)},
			wantUpdates: 1,
		},
		{
			name:        "node without an IPv4 address leaves the IP ACLs",
			initialAcls: []file.VolumePoolNfsExportOptions{rw(ipA)},
			nodeID:      "node-ipv6",
			wantAcls:    []file.VolumePoolNfsExportOptions{rw(ipA)},
		},
		{
			name:        "no node lookup",
			initialAcls: []file.VolumePoolNfsExportOptions{rw(ipA)},
			setup: func(cs *controllerServer, _ *publishTestService) {
				cs.config.volumePoolNodes = nil
			},
			nodeID:   "node-a",
			wantCode: codes.Internal,
		},
		{
			// Unlike a node that is gone, a lookup that fails doesn't tell
			// whether the node can still mount the volume.
			name:        "node lookup unavailable",
			initialAcls: []file.VolumePoolNfsExportOptions{rw(ipA)},
			setup: func(cs *controllerServer, _ *publishTestService) {
				cs.config.volumePoolNodes = &volumePoolNodes{ips: unavailableNodeIPs{}}
			},
			nodeID:   "node-a",
			wantCode: codes.Unavailable,
		},
		{
			name:     "volume not found",
			volumeID: "volumepool://test-project/us-central1/my-pool/volumes/missing?ip=10.1.1.1",
			nodeID:   "node-a",
		},
		{
			// A COLD volume has no IP ACLs: the API requires them to be
			// empty to freeze it. If they listed the node anyway,
			// unpublishing wouldn't update them.
			name: "COLD volume",
			setup: func(_ *controllerServer, fs *publishTestService) {
				fs.getVolume = volumeWithIpAcls(file.VolumePoolVolumeStateCold, rw(ipA))
			},
			nodeID: "node-a",
		},
		{
			// Likewise, the API requires the IP ACLs to be empty to make a
			// volume WARM.
			name: "WARM volume",
			setup: func(_ *controllerServer, fs *publishTestService) {
				fs.getVolume = volumeWithIpAcls(file.VolumePoolVolumeStateWarm, rw(ipA))
			},
			nodeID: "node-a",
		},
		{
			// A volume being created can't have been published yet, so
			// there is nothing to remove.
			name: "volume still being created",
			setup: func(_ *controllerServer, fs *publishTestService) {
				fs.getVolume = volumeInState(file.VolumePoolVolumeStateCreating)
			},
			nodeID: "node-a",
		},
		{
			name:        "volume busy with another operation",
			initialAcls: []file.VolumePoolNfsExportOptions{rw(ipA)},
			setup: func(_ *controllerServer, fs *publishTestService) {
				fs.updateIpAcls = func(context.Context, string, []file.VolumePoolNfsExportOptions) (*file.PoolVolume, error) {
					return nil, status.Error(codes.FailedPrecondition, "another operation is in progress")
				}
			},
			nodeID:      "node-a",
			wantCode:    codes.Aborted,
			wantUpdates: 1,
		},
		{
			// The fake still has the volume: the PATCH simulates its
			// deletion after the GET.
			name:        "volume deleted before its IP ACLs are updated",
			initialAcls: []file.VolumePoolNfsExportOptions{rw(ipA)},
			setup: func(_ *controllerServer, fs *publishTestService) {
				fs.updateIpAcls = func(context.Context, string, []file.VolumePoolNfsExportOptions) (*file.PoolVolume, error) {
					return nil, status.Error(codes.NotFound, "volume not found")
				}
			},
			nodeID:      "node-a",
			wantAcls:    []file.VolumePoolNfsExportOptions{rw(ipA)},
			wantUpdates: 1,
		},
		{
			name: "another IP ACL change is in progress",
			setup: func(cs *controllerServer, _ *publishTestService) {
				cs.config.volumeLocks.TryAcquire(testSourceVolumeName + volumePoolIpAclsLockSuffix)
			},
			nodeID:   "node-a",
			wantCode: codes.Aborted,
		},
		{
			name: "Volume Pools feature disabled",
			setup: func(cs *controllerServer, _ *publishTestService) {
				cs.config.features.FeatureVolumePools.Enabled = false
			},
			nodeID:   "node-a",
			wantCode: codes.Unimplemented,
		},
		{
			name:     "malformed volume pool volume ID",
			volumeID: "volumepool://test-project/us-central1/my-pool/volumes/?ip=10.1.1.1",
			nodeID:   "node-a",
			wantCode: codes.InvalidArgument,
		},
		{
			name:     "not a volume pool volume",
			volumeID: "modeInstance/us-central1-c/myinstance/myshare",
			nodeID:   "node-a",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cs, fs := newPublishTestController(t)
			if tc.initialAcls != nil {
				setTestVolumeIpAcls(t, fs, tc.initialAcls)
			}
			if tc.setup != nil {
				tc.setup(cs, fs)
			}
			volumeID := tc.volumeID
			if volumeID == "" {
				volumeID = testSourceVolumeID
			}
			before := testVolumeIpAcls(t, fs)

			_, err := cs.ControllerUnpublishVolume(ctx, &csi.ControllerUnpublishVolumeRequest{VolumeId: volumeID, NodeId: tc.nodeID})
			expectCode(t, err, tc.wantCode)
			if got := fs.calls["UpdateVolumePoolVolumeIpAcls"]; got != tc.wantUpdates {
				t.Errorf("IP ACLs updated %d times, want %d", got, tc.wantUpdates)
			}
			after := testVolumeIpAcls(t, fs)
			switch {
			case err != nil:
				// A failed unpublish leaves the IP ACLs as they were.
				if diff := cmp.Diff(before, after); diff != "" {
					t.Errorf("IP ACLs changed although the unpublish failed (-before +after):\n%s", diff)
				}
			case tc.initialAcls != nil:
				if diff := cmp.Diff(tc.wantAcls, after); diff != "" {
					t.Errorf("IP ACLs mismatch (-want +got):\n%s", diff)
				}
			}
		})
	}

	t.Run("missing volume ID", func(t *testing.T) {
		cs, _ := newPublishTestController(t)
		_, err := cs.ControllerUnpublishVolume(ctx, &csi.ControllerUnpublishVolumeRequest{NodeId: "node-a"})
		expectCode(t, err, codes.InvalidArgument)
	})
}

// An actor's life in Substrate: resumes and suspends on different nodes, and a
// crash that skips unpublishing. The volume starts with the IP ACLs that the
// API gives a new volume.
func TestControllerPublishUnpublish_ActorLifecycle(t *testing.T) {
	ctx := context.Background()
	cs, fs := newPublishTestController(t)
	if diff := cmp.Diff(newVolumeIpAcls(), testVolumeIpAcls(t, fs)); diff != "" {
		t.Fatalf("IP ACLs of the new volume mismatch (-want +got):\n%s", diff)
	}
	snw := csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER
	unpublish := func(nodeID string) *csi.ControllerUnpublishVolumeRequest {
		return &csi.ControllerUnpublishVolumeRequest{VolumeId: testSourceVolumeID, NodeId: nodeID}
	}

	steps := []struct {
		name        string
		call        func() error
		wantAcls    []file.VolumePoolNfsExportOptions
		wantUpdates int // total so far
	}{
		{"resume on node A", func() error { _, err := cs.ControllerPublishVolume(ctx, publishRequest("node-a", snw)); return err }, []file.VolumePoolNfsExportOptions{rw(ipA)}, 1},
		{"resume workflow re-entered", func() error { _, err := cs.ControllerPublishVolume(ctx, publishRequest("node-a", snw)); return err }, []file.VolumePoolNfsExportOptions{rw(ipA)}, 1},
		{"suspend on node A", func() error { _, err := cs.ControllerUnpublishVolume(ctx, unpublish("node-a")); return err }, nil, 2},
		{"resume on node B", func() error { _, err := cs.ControllerPublishVolume(ctx, publishRequest("node-b", snw)); return err }, []file.VolumePoolNfsExportOptions{rw(ipB)}, 3},
		{"crash on B, resume on node C", func() error { _, err := cs.ControllerPublishVolume(ctx, publishRequest("node-c", snw)); return err }, []file.VolumePoolNfsExportOptions{rw(ipC)}, 4},
		{"late unpublish from node B", func() error { _, err := cs.ControllerUnpublishVolume(ctx, unpublish("node-b")); return err }, []file.VolumePoolNfsExportOptions{rw(ipC)}, 4},
		{"delete on node C", func() error { _, err := cs.ControllerUnpublishVolume(ctx, unpublish("node-c")); return err }, nil, 5},
	}
	for _, step := range steps {
		if err := step.call(); err != nil {
			t.Fatalf("%s: unexpected error: %v", step.name, err)
		}
		if diff := cmp.Diff(step.wantAcls, testVolumeIpAcls(t, fs)); diff != "" {
			t.Errorf("%s: IP ACLs mismatch (-want +got):\n%s", step.name, diff)
		}
		if got := fs.calls["UpdateVolumePoolVolumeIpAcls"]; got != step.wantUpdates {
			t.Errorf("%s: IP ACLs updated %d times in total, want %d", step.name, got, step.wantUpdates)
		}
	}
}

func TestControllerCapabilities_PublishUnpublish(t *testing.T) {
	for _, tc := range []struct {
		name    string
		feature *FeatureVolumePools
		want    bool
	}{
		{name: "Volume Pools off", feature: &FeatureVolumePools{}},
		{name: "Volume Pools on", feature: &FeatureVolumePools{Enabled: true}, want: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			driver := initTestControllerDriver(t, tc.feature)
			for _, c := range []csi.ControllerServiceCapability_RPC_Type{
				csi.ControllerServiceCapability_RPC_PUBLISH_UNPUBLISH_VOLUME,
				csi.ControllerServiceCapability_RPC_PUBLISH_READONLY,
			} {
				err := driver.ValidateControllerServiceRequest(c)
				if got := err == nil; got != tc.want {
					t.Errorf("%v advertised = %v, want %v", c, got, tc.want)
				}
			}
		})
	}
}

// Only removing another node's entry counts as an eviction, which is logged as
// a warning and counted in a metric: removing wider ranges, such as those of a
// new volume, is expected.
func TestPublishedVolumePoolIpAcls_Evicted(t *testing.T) {
	for _, tc := range []struct {
		name        string
		current     []file.VolumePoolNfsExportOptions
		singleNode  bool
		wantEvicted []string
	}{
		{name: "new volume", current: newVolumeIpAcls(), singleNode: true},
		{name: "published to the node", current: []file.VolumePoolNfsExportOptions{rw(ipA)}, singleNode: true},
		{name: "published to another node", current: []file.VolumePoolNfsExportOptions{rw(ipB)}, singleNode: true, wantEvicted: []string{ipB}},
		{name: "published to another node, with wider ranges", current: append(newVolumeIpAcls(), rw(ipB+"/32")), singleNode: true, wantEvicted: []string{ipB + "/32"}},
		{name: "multi-node", current: append(newVolumeIpAcls(), rw(ipB)), singleNode: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, evicted, err := publishedVolumePoolIpAcls(tc.current, rw(ipA), tc.singleNode)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if diff := cmp.Diff(tc.wantEvicted, evicted); diff != "" {
				t.Errorf("evicted IP ranges mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

// A single-node publish that replaces another node's entry counts an eviction
// in filestorecsi_volume_pool_publish_eviction_count. Replacing the rule of a
// new volume, or publishing again, doesn't.
func TestControllerPublishVolume_CountsEvictions(t *testing.T) {
	ctx := context.Background()
	mm := metrics.NewMetricsManager()
	mm.RegisterVolumePoolPublishEvictionCountMetric()
	cs, _ := newPublishTestController(t)
	cs.config.metricsManager = mm
	// The counter is global, so other tests may have counted evictions too.
	evictions := func() float64 {
		t.Helper()
		families, err := mm.GetRegistry().Gather()
		if err != nil {
			t.Fatalf("failed to gather metrics: %v", err)
		}
		for _, f := range families {
			if f.GetName() == "filestorecsi_volume_pool_publish_eviction_count" && len(f.GetMetric()) == 1 {
				return f.GetMetric()[0].GetCounter().GetValue()
			}
		}
		t.Fatalf("the metrics don't have filestorecsi_volume_pool_publish_eviction_count: %v", families)
		return 0
	}
	start := evictions()

	for _, step := range []struct {
		node string
		// want is the number of evictions counted since the start.
		want float64
	}{
		{node: "node-a", want: 0}, // replaces the rule of a new volume
		{node: "node-a", want: 0}, // publishes again
		{node: "node-b", want: 1}, // replaces node-a
	} {
		if _, err := cs.ControllerPublishVolume(ctx, publishRequest(step.node, csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER)); err != nil {
			t.Fatalf("ControllerPublishVolume(%s) error = %v", step.node, err)
		}
		if got := evictions() - start; got != step.want {
			t.Errorf("after publishing to %s: %v evictions counted, want %v", step.node, got, step.want)
		}
	}
}

func TestIsSingleIPRange(t *testing.T) {
	for r, want := range map[string]bool{
		"10.0.0.1":       true,
		"10.0.0.1/32":    true,
		"10.0.0.0/31":    false,
		"10.0.0.0/8":     false,
		"0.0.0.0/0":      false,
		"fd00::1":        false,
		"fd00::1/128":    false,
		"not-an-address": false,
	} {
		if got := isSingleIPRange(r); got != want {
			t.Errorf("isSingleIPRange(%q) = %v, want %v", r, got, want)
		}
	}
}

func TestVolumePoolIpAclsString(t *testing.T) {
	for _, tc := range []struct {
		name string
		acls []file.VolumePoolNfsExportOptions
		want string
	}{
		{name: "no entries", want: "[]"},
		{
			name: "a published node",
			acls: []file.VolumePoolNfsExportOptions{rw(ipA)},
			want: "[10.128.0.11 access=READ_WRITE squash=NO_ROOT_SQUASH flavors=AUTH_SYS]",
		},
		{
			name: "a new volume's rule and a read-only node",
			acls: append(newVolumeIpAcls(), ro(ipB)),
			want: "[10.0.0.0/8,172.16.0.0/12,192.168.0.0/16 access=READ_WRITE squash=NO_ROOT_SQUASH flavors=AUTH_SYS; 10.128.0.12 access=READ_ONLY squash=NO_ROOT_SQUASH flavors=AUTH_SYS]",
		},
		{
			name: "unset settings",
			acls: []file.VolumePoolNfsExportOptions{{IpRanges: []string{ipA}}},
			want: "[10.128.0.11 access=unset squash=unset flavors=unset]",
		},
		{
			name: "anon IDs and network",
			acls: []file.VolumePoolNfsExportOptions{{
				IpRanges:        []string{"10.0.0.0/24"},
				AccessMode:      file.VolumePoolAccessModeReadWrite,
				SquashMode:      "ROOT_SQUASH",
				AnonUid:         1000,
				AnonGid:         1001,
				SecurityFlavors: []string{"AUTH_SYS", "KRB5"},
				Network:         "default",
			}},
			want: "[10.0.0.0/24 access=READ_WRITE squash=ROOT_SQUASH flavors=AUTH_SYS,KRB5 anonUid=1000 anonGid=1001 network=default]",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := volumePoolIpAclsString(tc.acls); got != tc.want {
				t.Errorf("volumePoolIpAclsString() = %q, want %q", got, tc.want)
			}
		})
	}
}

// The logs of a publish or unpublish that changes the IP ACLs show every
// setting of the entries, so that they show a change to a setting other than
// the IP ranges, such as the security flavor here.
func TestControllerPublishUnpublish_LogsIpAcls(t *testing.T) {
	ctx := context.Background()
	logs := captureKlog(t)
	cs, fs := newPublishTestController(t)
	setTestVolumeIpAcls(t, fs, []file.VolumePoolNfsExportOptions{{
		IpRanges:   []string{ipA},
		AccessMode: file.VolumePoolAccessModeReadWrite,
		SquashMode: file.VolumePoolSquashModeNoRoot,
	}})
	const (
		withoutFlavor = "10.128.0.11 access=READ_WRITE squash=NO_ROOT_SQUASH flavors=unset"
		nodeA         = "10.128.0.11 access=READ_WRITE squash=NO_ROOT_SQUASH flavors=AUTH_SYS"
		nodeB         = "10.128.0.12 access=READ_WRITE squash=NO_ROOT_SQUASH flavors=AUTH_SYS"
	)

	if _, err := cs.ControllerPublishVolume(ctx, publishRequest("node-a", csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER)); err != nil {
		t.Fatalf("ControllerPublishVolume() error = %v", err)
	}
	if _, err := cs.ControllerUnpublishVolume(ctx, &csi.ControllerUnpublishVolumeRequest{VolumeId: testSourceVolumeID, NodeId: "node-a"}); err != nil {
		t.Fatalf("ControllerUnpublishVolume() error = %v", err)
	}
	setTestVolumeIpAcls(t, fs, []file.VolumePoolNfsExportOptions{rw(ipA), rw(ipB)})
	if _, err := cs.ControllerUnpublishVolume(ctx, &csi.ControllerUnpublishVolumeRequest{VolumeId: testSourceVolumeID}); err != nil {
		t.Fatalf("ControllerUnpublishVolume() for every node error = %v", err)
	}

	got := logs()
	for _, want := range []string{
		fmt.Sprintf("Published Volume Pool volume %q to node %q (%s): its IP ACLs went from [%s] to [%s]", testSourceVolumeName, "node-a", ipA, withoutFlavor, nodeA),
		fmt.Sprintf("Unpublished Volume Pool volume %q from node %q (%s): its IP ACLs went from [%s] to []", testSourceVolumeName, "node-a", ipA, nodeA),
		fmt.Sprintf("Unpublished Volume Pool volume %q from every node: its IP ACLs went from [%s; %s] to []", testSourceVolumeName, nodeA, nodeB),
	} {
		if !strings.Contains(got, want) {
			t.Errorf("logs don't contain %q:\n%s", want, got)
		}
	}
}
