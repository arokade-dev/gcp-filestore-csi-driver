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
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	cloud "sigs.k8s.io/gcp-filestore-csi-driver/pkg/cloud_provider"
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
