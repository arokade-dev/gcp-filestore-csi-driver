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
	"net"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
)

// volumePoolNodes looks up the nodes that volume pool volumes are published
// to.
type volumePoolNodes struct {
	// informers holds the Node informer behind ips. It is nil when ips needs
	// no informer, as in tests.
	informers informers.SharedInformerFactory
	ips       nodeIPResolver
}

// newVolumePoolNodes returns a volumePoolNodes that looks nodes up through a
// Node informer on kubeClient. The informer starts with the controller.
func newVolumePoolNodes(kubeClient kubernetes.Interface) *volumePoolNodes {
	factory := informers.NewSharedInformerFactory(kubeClient, 0)
	return &volumePoolNodes{
		informers: factory,
		ips:       newInformerNodeIPResolver(factory),
	}
}

// startVolumePoolNodes starts the Node informer that publishing volume pool
// volumes relies on, if there is one.
func (s *controllerServer) startVolumePoolNodes(stopCh <-chan struct{}) {
	if n := s.config.volumePoolNodes; n != nil && n.informers != nil {
		n.informers.Start(stopCh)
	}
}

// nodeIPResolver looks up the IP address that a Kubernetes node mounts NFS
// shares from. A volume pool volume's IP ACLs must allow that address.
type nodeIPResolver interface {
	// NodeIP returns the node's internal IPv4 address. The error has code
	// NotFound if the node does not exist, FailedPrecondition if it has no
	// internal IPv4 address, and Unavailable if nodes can't be looked up yet.
	NodeIP(nodeName string) (string, error)
}

// informerNodeIPResolver looks nodes up in a Node informer's cache, so that a
// lookup makes no API call.
type informerNodeIPResolver struct {
	lister corelisters.NodeLister
	synced cache.InformerSynced
}

// newInformerNodeIPResolver returns a resolver backed by factory's Node
// informer. It answers once the factory has started and the cache has synced.
func newInformerNodeIPResolver(factory informers.SharedInformerFactory) *informerNodeIPResolver {
	nodes := factory.Core().V1().Nodes()
	return &informerNodeIPResolver{
		lister: nodes.Lister(),
		synced: nodes.Informer().HasSynced,
	}
}

func (r *informerNodeIPResolver) NodeIP(nodeName string) (string, error) {
	if !r.synced() {
		return "", status.Error(codes.Unavailable, "the node cache is not synced yet")
	}
	node, err := r.lister.Get(nodeName)
	if apierrors.IsNotFound(err) {
		return "", status.Errorf(codes.NotFound, "node %q not found", nodeName)
	}
	if err != nil {
		return "", status.Errorf(codes.Internal, "failed to get node %q: %v", nodeName, err)
	}
	return nodeInternalIPv4(node)
}

// nodeInternalIPv4 returns the first InternalIP address of node that is IPv4.
// The NFS client on the node uses that address, because volume pool volumes
// are mounted from the host network.
func nodeInternalIPv4(node *v1.Node) (string, error) {
	for _, addr := range node.Status.Addresses {
		if addr.Type != v1.NodeInternalIP {
			continue
		}
		if ip := net.ParseIP(addr.Address); ip != nil && ip.To4() != nil {
			return ip.String(), nil
		}
	}
	return "", status.Errorf(codes.FailedPrecondition, "node %q has no internal IPv4 address", node.Name)
}
