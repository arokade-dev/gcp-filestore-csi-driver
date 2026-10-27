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
	"net"
	"slices"
	"strings"
	"time"

	csi "github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"
	"sigs.k8s.io/gcp-filestore-csi-driver/pkg/cloud_provider/file"
	"sigs.k8s.io/gcp-filestore-csi-driver/pkg/util"
)

const (
	// volumePoolIpAclsLockSuffix makes the key of the lock that serializes
	// changes to one volume's IP ACLs. It differs from the volume's own lock,
	// which CreateVolume, DeleteVolume and CreateSnapshot hold, so that a long
	// snapshot doesn't hold up publishing.
	volumePoolIpAclsLockSuffix = "#acl"
	// maxVolumePoolIpAclRanges is the most IP ranges the API accepts in one
	// volume's IP ACLs.
	maxVolumePoolIpAclRanges = 64

	modeVolumePool                  = "modeVolumePool"
	methodControllerPublishVolume   = "ControllerPublishVolume"
	methodControllerUnpublishVolume = "ControllerUnpublishVolume"
)

// ControllerPublishVolume lets the node req.NodeId mount the volume. Only volume
// pool volumes restrict NFS access per node: publishing one puts the node's IP
// address in the volume's IP ACLs. Other volumes need no publishing.
func (s *controllerServer) ControllerPublishVolume(ctx context.Context, req *csi.ControllerPublishVolumeRequest) (*csi.ControllerPublishVolumeResponse, error) {
	volumeID := req.GetVolumeId()
	if volumeID == "" {
		return nil, status.Error(codes.InvalidArgument, "ControllerPublishVolume volume ID must be provided")
	}
	if req.GetNodeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "ControllerPublishVolume node ID must be provided")
	}
	if err := s.config.driver.validateVolumeCapability(req.GetVolumeCapability()); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "ControllerPublishVolume volume capability is invalid: %v", err)
	}
	if !isVolumePoolVolumeID(volumeID) {
		return &csi.ControllerPublishVolumeResponse{}, nil
	}
	if !s.volumePoolsEnabled() {
		return nil, status.Errorf(codes.FailedPrecondition, "cannot publish volume pool volume %q: Volume Pools feature is disabled", volumeID)
	}

	start := time.Now()
	err := s.publishVolumePoolVolume(ctx, req)
	s.config.metricsManager.RecordOperationMetrics(err, methodControllerPublishVolume, modeVolumePool, time.Since(start))
	if err != nil {
		return nil, err
	}
	// The mount point stays the one in the volume context, so there is no
	// publish context.
	return &csi.ControllerPublishVolumeResponse{}, nil
}

// ControllerUnpublishVolume takes away the node req.NodeId's access to the
// volume. For a volume pool volume, the node's IP address is removed from the
// volume's IP ACLs, and an empty node ID removes every entry. Other volumes
// need no unpublishing.
func (s *controllerServer) ControllerUnpublishVolume(ctx context.Context, req *csi.ControllerUnpublishVolumeRequest) (*csi.ControllerUnpublishVolumeResponse, error) {
	volumeID := req.GetVolumeId()
	if volumeID == "" {
		return nil, status.Error(codes.InvalidArgument, "ControllerUnpublishVolume volume ID must be provided")
	}
	if !isVolumePoolVolumeID(volumeID) {
		return &csi.ControllerUnpublishVolumeResponse{}, nil
	}
	if !s.volumePoolsEnabled() {
		return nil, status.Errorf(codes.FailedPrecondition, "cannot unpublish volume pool volume %q: Volume Pools feature is disabled", volumeID)
	}

	start := time.Now()
	err := s.unpublishVolumePoolVolume(ctx, req)
	s.config.metricsManager.RecordOperationMetrics(err, methodControllerUnpublishVolume, modeVolumePool, time.Since(start))
	if err != nil {
		return nil, err
	}
	return &csi.ControllerUnpublishVolumeResponse{}, nil
}

// publishVolumePoolVolume adds the IP address of node req.NodeId to the IP
// ACLs of the volume pool volume req.VolumeId.
func (s *controllerServer) publishVolumePoolVolume(ctx context.Context, req *csi.ControllerPublishVolumeRequest) error {
	volumeID, nodeID := req.GetVolumeId(), req.GetNodeId()
	name, err := volumePoolVolumeName(volumeID)
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "failed to parse composite volume ID %q: %v", volumeID, err)
	}
	unlock, err := s.lockVolumePoolIpAcls(name)
	if err != nil {
		return err
	}
	defer unlock()

	nodeIP, err := s.volumePoolNodeIP(nodeID)
	if err != nil {
		return err
	}
	vol, err := s.config.fileService.GetVolumePoolVolume(ctx, name)
	if err != nil {
		return volumePoolError(err)
	}
	if err := checkVolumePoolVolumePublishable(vol); err != nil {
		return err
	}

	mode := req.GetVolumeCapability().GetAccessMode().GetMode()
	rule := volumePoolNodeIpAcl(nodeIP, req.GetReadonly() || isReadOnlyAccessMode(mode))
	acls, evicted, err := publishedVolumePoolIpAcls(vol.IpAcls, rule, isSingleNodeAccessMode(mode))
	if err != nil {
		return status.Errorf(status.Code(err), "cannot publish volume %q to node %q: %s", name, nodeID, status.Convert(err).Message())
	}
	if equalVolumePoolIpAcls(acls, vol.IpAcls) {
		klog.V(4).Infof("Volume Pool volume %q is already published to node %q (%s)", name, nodeID, nodeIP)
		return nil
	}
	if len(evicted) > 0 {
		klog.Warningf("Publishing Volume Pool volume %q to node %q (%s) with access mode %v removes the access of %s: such a volume is exported to one node only", name, nodeID, nodeIP, mode, strings.Join(evicted, ", "))
		s.config.metricsManager.RecordVolumePoolPublishEviction()
	}
	if err := s.updateVolumePoolIpAcls(ctx, name, acls); err != nil {
		return err
	}
	klog.Infof("Published Volume Pool volume %q to node %q (%s): the IP ranges it is exported to went from [%s] to [%s]", name, nodeID, nodeIP, strings.Join(volumePoolIpAclRanges(vol.IpAcls), ", "), strings.Join(volumePoolIpAclRanges(acls), ", "))
	return nil
}

// unpublishVolumePoolVolume removes the IP address of node req.NodeId from the
// IP ACLs of the volume pool volume req.VolumeId.
func (s *controllerServer) unpublishVolumePoolVolume(ctx context.Context, req *csi.ControllerUnpublishVolumeRequest) error {
	volumeID, nodeID := req.GetVolumeId(), req.GetNodeId()
	name, err := volumePoolVolumeName(volumeID)
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "failed to parse composite volume ID %q: %v", volumeID, err)
	}
	unlock, err := s.lockVolumePoolIpAcls(name)
	if err != nil {
		return err
	}
	defer unlock()

	vol, err := s.config.fileService.GetVolumePoolVolume(ctx, name)
	if err != nil {
		if isVolumePoolNotFound(err) {
			klog.Infof("Volume Pool volume %q not found, so it is not published to node %q", name, nodeID)
			return nil
		}
		return volumePoolError(err)
	}
	if vol.State == file.VolumePoolVolumeStateCold {
		// A COLD volume is exported to no clients: the API removes its IP
		// ACLs before freezing it, and doesn't let them change until it is
		// unfrozen.
		return nil
	}

	// An empty node ID unpublishes the volume from every node.
	var acls []file.VolumePoolNfsExportOptions
	if nodeID != "" {
		nodeIP, err := s.volumePoolNodeIP(nodeID)
		if err != nil {
			if code := status.Code(err); code == codes.NotFound || code == codes.FailedPrecondition {
				klog.Warningf("Leaving the IP ACLs of Volume Pool volume %q unchanged, because the IP address of node %q is unknown: %v. Publishing the volume to another node with a single-node access mode removes the node's entry.", name, nodeID, err)
				return nil
			}
			return err
		}
		acls = volumePoolIpAclsWithout(vol.IpAcls, nodeIP)
	}
	if equalVolumePoolIpAcls(acls, vol.IpAcls) {
		klog.V(4).Infof("Volume Pool volume %q is not published to node %q", name, nodeID)
		return nil
	}
	if err := s.updateVolumePoolIpAcls(ctx, name, acls); err != nil {
		return err
	}
	klog.Infof("Unpublished Volume Pool volume %q from node %q", name, nodeID)
	return nil
}

// volumePoolVolumeName returns the resource name of the volume pool volume with
// CSI volume ID volumeID.
func volumePoolVolumeName(volumeID string) (string, error) {
	parent, volID, _, err := parseVolumePoolVolumeID(volumeID)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s/volumes/%s", parent, volID), nil
}

// lockVolumePoolIpAcls takes the lock that serializes changes to the IP ACLs
// of the volume name, and returns the function that releases it.
func (s *controllerServer) lockVolumePoolIpAcls(name string) (func(), error) {
	key := name + volumePoolIpAclsLockSuffix
	if !s.config.volumeLocks.TryAcquire(key) {
		return nil, status.Errorf(codes.Aborted, util.VolumeOperationAlreadyExistsFmt, name)
	}
	return func() { s.config.volumeLocks.Release(key) }, nil
}

// volumePoolNodeIP returns the IP address that node nodeID mounts volume pool
// volumes from.
func (s *controllerServer) volumePoolNodeIP(nodeID string) (string, error) {
	n := s.config.volumePoolNodes
	if n == nil || n.ips == nil {
		return "", status.Error(codes.Internal, "the controller has no Kubernetes client to look up nodes")
	}
	return n.ips.NodeIP(nodeID)
}

// checkVolumePoolVolumePublishable returns nil if the IP ACLs of vol can be
// changed, which the API allows only for HOT volumes.
func checkVolumePoolVolumePublishable(vol *file.PoolVolume) error {
	if vol.State == file.VolumePoolVolumeStateCold {
		return status.Errorf(codes.FailedPrecondition, "volume %q is COLD; unfreeze it before publishing it", vol.Name)
	}
	if !isVolumePoolVolumeHot(vol) {
		return status.Errorf(codes.Aborted, "volume %q cannot be published yet: its state is %q", vol.Name, vol.State)
	}
	return nil
}

// updateVolumePoolIpAcls sets the IP ACLs of the volume name to acls.
func (s *controllerServer) updateVolumePoolIpAcls(ctx context.Context, name string, acls []file.VolumePoolNfsExportOptions) error {
	_, err := s.config.fileService.UpdateVolumePoolVolumeIpAcls(ctx, name, acls)
	if err == nil {
		return nil
	}
	err = volumePoolError(err)
	if status.Code(err) == codes.FailedPrecondition {
		// The volume was HOT when it was read, so the API most likely refused
		// the update because another operation, such as a backup, holds the
		// volume. Return Aborted so that the caller retries. The API returns
		// the same code when IP ACL updates are not enabled; its message,
		// which is kept, tells the two apart.
		return status.Errorf(codes.Aborted, "cannot update the IP ACLs of volume %q now: %s", name, status.Convert(err).Message())
	}
	return err
}

// volumePoolNodeIpAcl returns the IP ACL entry that lets the node with IP
// address ip mount a volume.
func volumePoolNodeIpAcl(ip string, readOnly bool) file.VolumePoolNfsExportOptions {
	accessMode := file.VolumePoolAccessModeReadWrite
	if readOnly {
		accessMode = file.VolumePoolAccessModeReadOnly
	}
	return file.VolumePoolNfsExportOptions{
		IpRanges:   []string{ip},
		AccessMode: accessMode,
		SquashMode: file.VolumePoolSquashModeNoRoot,
	}
}

// publishedVolumePoolIpAcls returns the IP ACLs that publish a volume with IP
// ACLs current to the node of rule, and the IP addresses of the other nodes
// that lose access.
//
// The IP ACLs of a published volume list the nodes that it is published to,
// each with an entry for its IP address. Entries for wider IP ranges, such as
// the rule for all private IP ranges that the API gives a new volume, are
// removed, so that only the nodes can mount the volume.
//
// With a single-node access mode, the result is rule alone. That replaces the
// entry of a node that the volume was never unpublished from, for example
// because the node crashed. If the volume is published to two or more other
// nodes, it looks shared and an error with code FailedPrecondition is returned
// instead: replacing never removes more than one node.
//
// With a multi-node access mode, rule is added and the other nodes are kept.
func publishedVolumePoolIpAcls(current []file.VolumePoolNfsExportOptions, rule file.VolumePoolNfsExportOptions, singleNode bool) ([]file.VolumePoolNfsExportOptions, []string, error) {
	ip := rule.IpRanges[0]
	nodes := volumePoolIpAclsOfNodes(current)
	if singleNode {
		others := volumePoolIpAclRanges(volumePoolIpAclsWithout(nodes, ip))
		if len(others) > 1 {
			return nil, nil, status.Errorf(codes.FailedPrecondition, "the volume is published to %d other nodes (%s), and a volume published with a single-node access mode is exported to one node only", len(others), strings.Join(others, ", "))
		}
		return []file.VolumePoolNfsExportOptions{rule}, others, nil
	}

	acls := volumePoolIpAclsWith(nodes, rule)
	if n := len(volumePoolIpAclRanges(acls)); n > maxVolumePoolIpAclRanges {
		return nil, nil, status.Errorf(codes.ResourceExhausted, "the volume would be exported to %d IP ranges, but at most %d are allowed", n, maxVolumePoolIpAclRanges)
	}
	return acls, nil, nil
}

// volumePoolIpAclsOfNodes returns the entries of acls for nodes: acls with only
// the IP ranges that are a single IP address, dropping the entries that are
// left with no IP ranges.
func volumePoolIpAclsOfNodes(acls []file.VolumePoolNfsExportOptions) []file.VolumePoolNfsExportOptions {
	var result []file.VolumePoolNfsExportOptions
	for _, acl := range acls {
		var ranges []string
		for _, r := range acl.IpRanges {
			if isSingleIPRange(r) {
				ranges = append(ranges, r)
			}
		}
		if len(ranges) > 0 {
			acl.IpRanges = ranges
			result = append(result, acl)
		}
	}
	return result
}

// isSingleIPRange reports whether the IP range r is a single IPv4 address,
// written either as an address or as a /32 range.
func isSingleIPRange(r string) bool {
	if ip := net.ParseIP(r); ip != nil {
		return ip.To4() != nil
	}
	ip, ipNet, err := net.ParseCIDR(r)
	if err != nil || ip.To4() == nil {
		return false
	}
	ones, bits := ipNet.Mask.Size()
	return ones == bits
}

// volumePoolIpAclsWith returns acls with rule in place of any entry for rule's
// IP address, keeping the order of the other entries.
func volumePoolIpAclsWith(acls []file.VolumePoolNfsExportOptions, rule file.VolumePoolNfsExportOptions) []file.VolumePoolNfsExportOptions {
	ip := rule.IpRanges[0]
	result := make([]file.VolumePoolNfsExportOptions, 0, len(acls)+1)
	added := false
	for _, acl := range acls {
		ranges := withoutIPRange(acl.IpRanges, ip)
		if len(ranges) < len(acl.IpRanges) && !added {
			result = append(result, rule)
			added = true
		}
		if len(ranges) > 0 {
			acl.IpRanges = ranges
			result = append(result, acl)
		}
	}
	if !added {
		result = append(result, rule)
	}
	return result
}

// volumePoolIpAclsWithout returns acls without the IP address ip, dropping the
// entries that are left with no IP ranges.
func volumePoolIpAclsWithout(acls []file.VolumePoolNfsExportOptions, ip string) []file.VolumePoolNfsExportOptions {
	var result []file.VolumePoolNfsExportOptions
	for _, acl := range acls {
		if ranges := withoutIPRange(acl.IpRanges, ip); len(ranges) > 0 {
			acl.IpRanges = ranges
			result = append(result, acl)
		}
	}
	return result
}

// withoutIPRange returns ranges without the IP address ip, written either as an
// address or as a /32 range.
func withoutIPRange(ranges []string, ip string) []string {
	var result []string
	for _, r := range ranges {
		if r != ip && r != ip+"/32" {
			result = append(result, r)
		}
	}
	return result
}

// volumePoolIpAclRanges returns all the IP ranges in acls.
func volumePoolIpAclRanges(acls []file.VolumePoolNfsExportOptions) []string {
	var ranges []string
	for _, acl := range acls {
		ranges = append(ranges, acl.IpRanges...)
	}
	return ranges
}

// equalVolumePoolIpAcls reports whether a and b are the same IP ACLs, in the
// same order.
func equalVolumePoolIpAcls(a, b []file.VolumePoolNfsExportOptions) bool {
	return slices.EqualFunc(a, b, func(x, y file.VolumePoolNfsExportOptions) bool {
		return slices.Equal(x.IpRanges, y.IpRanges) &&
			x.AccessMode == y.AccessMode &&
			x.SquashMode == y.SquashMode &&
			x.AnonUid == y.AnonUid &&
			x.AnonGid == y.AnonGid &&
			slices.Equal(x.SecurityFlavors, y.SecurityFlavors) &&
			x.Network == y.Network
	})
}

// isSingleNodeAccessMode reports whether a volume published with access mode
// mode is used by one node at a time.
func isSingleNodeAccessMode(mode csi.VolumeCapability_AccessMode_Mode) bool {
	switch mode {
	case csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
		csi.VolumeCapability_AccessMode_SINGLE_NODE_READER_ONLY,
		csi.VolumeCapability_AccessMode_SINGLE_NODE_SINGLE_WRITER,
		csi.VolumeCapability_AccessMode_SINGLE_NODE_MULTI_WRITER:
		return true
	}
	return false
}

// isReadOnlyAccessMode reports whether access mode mode allows only reads.
func isReadOnlyAccessMode(mode csi.VolumeCapability_AccessMode_Mode) bool {
	return mode == csi.VolumeCapability_AccessMode_SINGLE_NODE_READER_ONLY ||
		mode == csi.VolumeCapability_AccessMode_MULTI_NODE_READER_ONLY
}

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
