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
	"context"
	"fmt"
	"testing"

	csi "github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	cloud "sigs.k8s.io/gcp-filestore-csi-driver/pkg/cloud_provider"
	"sigs.k8s.io/gcp-filestore-csi-driver/pkg/cloud_provider/file"
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

	if _, err := nodes.ips.NodeIP("node-a"); status.Code(err) != codes.Unavailable {
		t.Errorf("NodeIP() before the cache synced: error = %v, want code %v", err, codes.Unavailable)
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
			ip, err := nodes.ips.NodeIP(tc.node)
			if code := status.Code(err); code != tc.wantCode {
				t.Fatalf("NodeIP(%q) error = %v (code %v), want code %v", tc.node, err, code, tc.wantCode)
			}
			if ip != tc.wantIP {
				t.Errorf("NodeIP(%q) = %q, want %q", tc.node, ip, tc.wantIP)
			}
		})
	}
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

func (f fakeNodeIPs) NodeIP(nodeName string) (string, error) {
	ip, ok := f[nodeName]
	if !ok {
		return "", status.Errorf(codes.NotFound, "node %q not found", nodeName)
	}
	if ip == "" {
		return "", status.Errorf(codes.FailedPrecondition, "node %q has no internal IPv4 address", nodeName)
	}
	return ip, nil
}

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

const (
	ipA = "10.128.0.11"
	ipB = "10.128.0.12"
	ipC = "10.128.0.13"
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

func acl(accessMode string, ipRanges ...string) file.VolumePoolNfsExportOptions {
	return file.VolumePoolNfsExportOptions{IpRanges: ipRanges, AccessMode: accessMode, SquashMode: file.VolumePoolSquashModeNoRoot}
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
		SecurityFlavors: []string{"AUTH_SYS"},
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
			name:        "single-node publish rewrites the node's /32 entry",
			initialAcls: []file.VolumePoolNfsExportOptions{rw(ipA + "/32")},
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
			initialAcls: []file.VolumePoolNfsExportOptions{ro(ipA), rw(ipB)},
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
			wantCode: codes.Aborted,
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
			wantCode: codes.FailedPrecondition,
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

			resp, err := cs.ControllerPublishVolume(ctx, tc.req)
			expectCode(t, err, tc.wantCode)
			if err == nil && len(resp.GetPublishContext()) != 0 {
				t.Errorf("PublishContext = %v, want none", resp.GetPublishContext())
			}
			if got := fs.calls["UpdateVolumePoolVolumeIpAcls"]; got != tc.wantUpdates {
				t.Errorf("IP ACLs updated %d times, want %d", got, tc.wantUpdates)
			}
			if tc.wantAcls != nil {
				if diff := cmp.Diff(tc.wantAcls, testVolumeIpAcls(t, fs)); diff != "" {
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
			wantAcls: []file.VolumePoolNfsExportOptions{rw(ipA)},
		},
		{
			name:     "volume not found",
			volumeID: "volumepool://test-project/us-central1/my-pool/volumes/missing?ip=10.1.1.1",
			nodeID:   "node-a",
		},
		{
			name: "COLD volume",
			setup: func(_ *controllerServer, fs *publishTestService) {
				fs.getVolume = volumeInState(file.VolumePoolVolumeStateCold)
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
			wantCode: codes.FailedPrecondition,
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

			_, err := cs.ControllerUnpublishVolume(ctx, &csi.ControllerUnpublishVolumeRequest{VolumeId: volumeID, NodeId: tc.nodeID})
			expectCode(t, err, tc.wantCode)
			if got := fs.calls["UpdateVolumePoolVolumeIpAcls"]; got != tc.wantUpdates {
				t.Errorf("IP ACLs updated %d times, want %d", got, tc.wantUpdates)
			}
			if tc.initialAcls != nil && tc.wantCode != codes.Aborted {
				if diff := cmp.Diff(tc.wantAcls, testVolumeIpAcls(t, fs)); diff != "" {
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
			err := driver.ValidateControllerServiceRequest(csi.ControllerServiceCapability_RPC_PUBLISH_UNPUBLISH_VOLUME)
			if got := err == nil; got != tc.want {
				t.Errorf("PUBLISH_UNPUBLISH_VOLUME advertised = %v, want %v", got, tc.want)
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
		{name: "published to another node, with wider ranges", current: append(newVolumeIpAcls(), ro(ipA, ipB+"/32")), singleNode: true, wantEvicted: []string{ipB + "/32"}},
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
