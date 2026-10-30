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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	utilcache "k8s.io/apimachinery/pkg/util/cache"
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
//
// Without the Volume Pools feature, the controller doesn't advertise
// PUBLISH_UNPUBLISH_VOLUME, and the call returns Unimplemented, whatever the
// request.
func (s *controllerServer) ControllerPublishVolume(ctx context.Context, req *csi.ControllerPublishVolumeRequest) (*csi.ControllerPublishVolumeResponse, error) {
	if !s.volumePoolsEnabled() {
		return nil, status.Error(codes.Unimplemented, "ControllerPublishVolume is supported only with the Volume Pools feature")
	}
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
// volume's IP ACLs, and so is the address that the node had when it was last
// deleted, if that was recently: the node may be gone, or GKE may have created
// it again with another address. An empty node ID removes every entry. Other
// volumes need no unpublishing.
//
// Without the Volume Pools feature, the call returns Unimplemented, like
// ControllerPublishVolume.
func (s *controllerServer) ControllerUnpublishVolume(ctx context.Context, req *csi.ControllerUnpublishVolumeRequest) (*csi.ControllerUnpublishVolumeResponse, error) {
	if !s.volumePoolsEnabled() {
		return nil, status.Error(codes.Unimplemented, "ControllerUnpublishVolume is supported only with the Volume Pools feature")
	}
	volumeID := req.GetVolumeId()
	if volumeID == "" {
		return nil, status.Error(codes.InvalidArgument, "ControllerUnpublishVolume volume ID must be provided")
	}
	if !isVolumePoolVolumeID(volumeID) {
		return &csi.ControllerUnpublishVolumeResponse{}, nil
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

	nodeIP, err := s.volumePoolNodeIP(ctx, nodeID)
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
	klog.Infof("Published Volume Pool volume %q to node %q (%s): its IP ACLs went from %s to %s", name, nodeID, nodeIP, volumePoolIpAclsString(vol.IpAcls), volumePoolIpAclsString(acls))
	return nil
}

// unpublishVolumePoolVolume removes the IP addresses of node req.NodeId, its
// current one and the one it had when it was last deleted, from the IP ACLs of
// the volume pool volume req.VolumeId.
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
	if vol.State == file.VolumePoolVolumeStateCold || vol.State == file.VolumePoolVolumeStateWarm {
		// A COLD or WARM volume is exported to no clients: the API requires
		// its IP ACLs to be empty before it freezes the volume or makes it
		// WARM, and doesn't let them change until the volume is HOT again.
		klog.V(4).Infof("Volume Pool volume %q is %s, so it is not published to node %q", name, vol.State, nodeID)
		return nil
	}

	// An empty node ID unpublishes the volume from every node.
	var acls []file.VolumePoolNfsExportOptions
	nodes := "every node"
	if nodeID != "" {
		var ips []string
		nodeIP, err := s.volumePoolNodeIP(ctx, nodeID)
		switch code := status.Code(err); {
		case err == nil:
			ips = append(ips, nodeIP)
		case code != codes.NotFound && code != codes.FailedPrecondition:
			return err
		}
		// A node that is gone, or that GKE deleted and created again with
		// another address, as when it repairs the node, can have left its
		// old address in the IP ACLs.
		if deletedIP, ok := s.config.volumePoolNodes.ips.DeletedNodeIP(nodeID); ok && deletedIP != nodeIP {
			ips = append(ips, deletedIP)
		}
		if len(ips) == 0 {
			klog.Warningf("Leaving the IP ACLs of Volume Pool volume %q unchanged, because the IP address of node %q is unknown: %v. Publishing the volume to another node with a single-node access mode removes the node's entry.", name, nodeID, err)
			return nil
		}
		acls = vol.IpAcls
		for _, ip := range ips {
			acls = volumePoolIpAclsWithout(acls, ip)
		}
		nodes = fmt.Sprintf("node %q (%s)", nodeID, strings.Join(ips, ", "))
	}
	if equalVolumePoolIpAcls(acls, vol.IpAcls) {
		klog.V(4).Infof("Volume Pool volume %q is not published to node %q", name, nodeID)
		return nil
	}
	if err := s.updateVolumePoolIpAcls(ctx, name, acls); err != nil {
		if status.Code(err) == codes.NotFound {
			// The volume was deleted after it was read, for example by
			// DeleteVolume, which takes another lock. No node can mount
			// it anymore.
			klog.Infof("Volume Pool volume %q was deleted, so it is no longer published to node %q", name, nodeID)
			return nil
		}
		return err
	}
	klog.Infof("Unpublished Volume Pool volume %q from %s: its IP ACLs went from %s to %s", name, nodes, volumePoolIpAclsString(vol.IpAcls), volumePoolIpAclsString(acls))
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
func (s *controllerServer) volumePoolNodeIP(ctx context.Context, nodeID string) (string, error) {
	n := s.config.volumePoolNodes
	if n == nil || n.ips == nil {
		return "", status.Error(codes.Internal, "the controller has no Kubernetes client to look up nodes")
	}
	return n.ips.NodeIP(ctx, nodeID)
}

// checkVolumePoolVolumePublishable returns nil if the IP ACLs of vol can be
// changed, which the API allows only for HOT volumes. A COLD or WARM volume
// gives FailedPrecondition, because it stays so until it is unfrozen or made
// HOT. A volume that is still being created, or that has no mount point yet,
// gives Aborted, so that the caller retries.
func checkVolumePoolVolumePublishable(vol *file.PoolVolume) error {
	switch {
	case vol.State == file.VolumePoolVolumeStateCold:
		return status.Errorf(codes.FailedPrecondition, "volume %q is COLD; unfreeze it before publishing it", vol.Name)
	case vol.State == file.VolumePoolVolumeStateWarm:
		return status.Errorf(codes.FailedPrecondition, "volume %q is WARM, which cannot be mounted; make it HOT before publishing it", vol.Name)
	case !isVolumePoolVolumeHot(vol):
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
// address ip mount a volume. It lists AUTH_SYS, like the rule that the API
// gives a new volume, rather than no security flavor: the API passes the list
// to the share unchanged.
func volumePoolNodeIpAcl(ip string, readOnly bool) file.VolumePoolNfsExportOptions {
	accessMode := file.VolumePoolAccessModeReadWrite
	if readOnly {
		accessMode = file.VolumePoolAccessModeReadOnly
	}
	return file.VolumePoolNfsExportOptions{
		IpRanges:        []string{ip},
		AccessMode:      accessMode,
		SquashMode:      file.VolumePoolSquashModeNoRoot,
		SecurityFlavors: []string{file.VolumePoolSecurityFlavorAuthSys},
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
// If the volume is already published to the node with another access mode, an
// error with code AlreadyExists is returned, as the CSI spec requires for a
// publish that is incompatible with an earlier one: changing the mode would
// change the access of a node that may be using the volume.
//
// With a single-node access mode, the result is rule alone. That replaces the
// entry of a node that the volume was never unpublished from, for example
// because the node crashed. If the volume is published to two or more nodes,
// counting the node of rule, it is shared, and an error with code
// FailedPrecondition is returned instead. A single-node publish leaves a single
// node, so two can only come from multi-node publishes: replacing never removes
// a node that shares the volume with another.
//
// With a multi-node access mode, rule is added and the other nodes are kept.
func publishedVolumePoolIpAcls(current []file.VolumePoolNfsExportOptions, rule file.VolumePoolNfsExportOptions, singleNode bool) ([]file.VolumePoolNfsExportOptions, []string, error) {
	ip := rule.IpRanges[0]
	nodes := volumePoolIpAclsOfNodes(current)
	if mode, ok := volumePoolNodeAccessMode(nodes, ip); ok && mode != rule.AccessMode {
		return nil, nil, status.Errorf(codes.AlreadyExists, "the volume is already published to %s as %s, and the request asks for %s", ip, mode, rule.AccessMode)
	}
	if singleNode {
		if published := volumePoolIpAclRanges(nodes); len(published) > 1 {
			return nil, nil, status.Errorf(codes.FailedPrecondition, "the volume is published to %d nodes (%s), and a volume published with a single-node access mode is exported to one node only", len(published), strings.Join(published, ", "))
		}
		others := volumePoolIpAclRanges(volumePoolIpAclsWithout(nodes, ip))
		return []file.VolumePoolNfsExportOptions{rule}, others, nil
	}

	acls := volumePoolIpAclsWith(nodes, rule)
	if n := len(volumePoolIpAclRanges(acls)); n > maxVolumePoolIpAclRanges {
		return nil, nil, status.Errorf(codes.ResourceExhausted, "the volume would be exported to %d IP ranges, but at most %d are allowed", n, maxVolumePoolIpAclRanges)
	}
	return acls, nil, nil
}

// volumePoolNodeAccessMode returns the access mode of the entry in acls for
// the IP address ip, written either as an address or as a /32 range, and
// whether there is one. An entry without an access mode is READ_WRITE, the
// API's default.
func volumePoolNodeAccessMode(acls []file.VolumePoolNfsExportOptions, ip string) (string, bool) {
	for _, acl := range acls {
		if len(withoutIPRange(acl.IpRanges, ip)) == len(acl.IpRanges) {
			continue
		}
		if acl.AccessMode == "" {
			return file.VolumePoolAccessModeReadWrite, true
		}
		return acl.AccessMode, true
	}
	return "", false
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

// volumePoolIpAclsString returns acls as logs show them: each entry's IP
// ranges with all its settings, so that a log shows any change to an entry,
// not only to its IP ranges. Unset settings show as "unset"; anon IDs and the
// network show only when set.
func volumePoolIpAclsString(acls []file.VolumePoolNfsExportOptions) string {
	entries := make([]string, 0, len(acls))
	for _, acl := range acls {
		entry := fmt.Sprintf("%s access=%s squash=%s flavors=%s",
			unsetIfEmpty(strings.Join(acl.IpRanges, ",")),
			unsetIfEmpty(acl.AccessMode),
			unsetIfEmpty(acl.SquashMode),
			unsetIfEmpty(strings.Join(acl.SecurityFlavors, ",")))
		if acl.AnonUid != 0 || acl.AnonGid != 0 {
			entry += fmt.Sprintf(" anonUid=%d anonGid=%d", acl.AnonUid, acl.AnonGid)
		}
		if acl.Network != "" {
			entry += " network=" + acl.Network
		}
		entries = append(entries, entry)
	}
	return "[" + strings.Join(entries, "; ") + "]"
}

// unsetIfEmpty returns s, or "unset" if s is empty.
func unsetIfEmpty(s string) string {
	if s == "" {
		return "unset"
	}
	return s
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
// Node informer on kubeClient, and through kubeClient itself when the
// informer's cache can't answer. The informer starts with the controller.
func newVolumePoolNodes(kubeClient kubernetes.Interface) *volumePoolNodes {
	factory := informers.NewSharedInformerFactory(kubeClient, 0)
	return &volumePoolNodes{
		informers: factory,
		ips:       newInformerNodeIPResolver(factory, kubeClient),
	}
}

// startVolumePoolNodes starts the Node informer that publishing volume pool
// volumes relies on, if there is one, and logs when its cache syncs.
func (s *controllerServer) startVolumePoolNodes(stopCh <-chan struct{}) {
	n := s.config.volumePoolNodes
	if n == nil || n.informers == nil {
		return
	}
	n.informers.Start(stopCh)
	go waitForVolumePoolNodeCache(stopCh, n.informers.Core().V1().Nodes().Informer().HasSynced, volumePoolNodeCacheWarnAfter)
}

const (
	// volumePoolNodeCacheWarnAfter is how long the node cache may take to
	// sync before the controller warns about it. Listing nodes takes
	// seconds at most, so a cache that hasn't synced by then most likely
	// can't list them.
	volumePoolNodeCacheWarnAfter = time.Minute
	// volumePoolNodeCachePollPeriod is how often the controller checks
	// whether the node cache has synced.
	volumePoolNodeCachePollPeriod = 100 * time.Millisecond
)

// waitForVolumePoolNodeCache waits until synced reports that the node cache
// has synced, or until stopCh is closed, and reports whether the cache synced.
// It logs when the cache syncs, and warns if it hasn't after warnAfter: if the
// controller can't list nodes, the only other trace in its logs is client-go's
// errors about *v1.Node.
func waitForVolumePoolNodeCache(stopCh <-chan struct{}, synced cache.InformerSynced, warnAfter time.Duration) bool {
	start := time.Now()
	warn := time.NewTimer(warnAfter)
	defer warn.Stop()
	poll := time.NewTicker(volumePoolNodeCachePollPeriod)
	defer poll.Stop()
	for !synced() {
		select {
		case <-stopCh:
			return false
		case <-warn.C:
			klog.Warningf("The node cache for publishing Volume Pool volumes has not synced after %v. Until it does, ControllerPublishVolume and ControllerUnpublishVolume of Volume Pool volumes get each node from the API server. Check that the controller's service account can list and watch nodes, and look for errors about *v1.Node in these logs.", warnAfter)
		case <-poll.C:
		}
	}
	klog.Infof("The node cache for publishing Volume Pool volumes synced after %v", time.Since(start).Round(time.Millisecond))
	return true
}

// nodeIPResolver looks up the IP address that a Kubernetes node mounts NFS
// shares from. A volume pool volume's IP ACLs must allow that address.
type nodeIPResolver interface {
	// NodeIP returns the node's internal IPv4 address. The error has code
	// NotFound if the node does not exist, FailedPrecondition if it has no
	// internal IPv4 address, and Unavailable if nodes can't be looked up.
	NodeIP(ctx context.Context, nodeName string) (string, error)
	// DeletedNodeIP returns the internal IPv4 address that the node had when
	// it was last deleted, if that was recently and no node has the address
	// now: GKE can give a deleted node's address to a new node. ok is false
	// otherwise.
	DeletedNodeIP(nodeName string) (ip string, ok bool)
}

const (
	// maxDeletedVolumePoolNodes is how many deleted nodes the controller
	// remembers the address of, at most.
	maxDeletedVolumePoolNodes = 10000
	// deletedVolumePoolNodeTTL is how long the controller remembers the
	// address of a deleted node. Substrate unpublishes the volumes of a node
	// that is gone when it suspends or deletes their actors, well within it.
	deletedVolumePoolNodeTTL = 24 * time.Hour
)

// informerNodeIPResolver looks nodes up in a Node informer's cache, so that a
// lookup usually makes no API call. When the cache can't answer, because it
// hasn't synced yet or doesn't have the node yet, it gets the node from the API
// server. It also remembers the addresses of the nodes that the informer sees
// deleted.
type informerNodeIPResolver struct {
	lister corelisters.NodeLister
	synced cache.InformerSynced
	client kubernetes.Interface
	// deleted maps the names of recently deleted nodes to the internal IPv4
	// addresses they had.
	deleted *utilcache.LRUExpireCache
}

// newInformerNodeIPResolver returns a resolver backed by factory's Node
// informer, and by client when the informer's cache can't answer. The cache
// keeps only what NodeIP reads; see stripVolumePoolNode.
func newInformerNodeIPResolver(factory informers.SharedInformerFactory, client kubernetes.Interface) *informerNodeIPResolver {
	nodes := factory.Core().V1().Nodes()
	informer := nodes.Informer()
	if err := informer.SetTransform(stripVolumePoolNode); err != nil {
		// SetTransform fails only if the informer has already started, and
		// the factory starts later, with the controller.
		klog.Warningf("The node cache for publishing Volume Pool volumes keeps whole Node objects: %v", err)
	}
	r := &informerNodeIPResolver{
		lister:  nodes.Lister(),
		synced:  informer.HasSynced,
		client:  client,
		deleted: utilcache.NewLRUExpireCache(maxDeletedVolumePoolNodes),
	}
	if _, err := informer.AddEventHandler(cache.ResourceEventHandlerFuncs{DeleteFunc: r.nodeDeleted}); err != nil {
		// AddEventHandler fails only once the informer has stopped.
		klog.Warningf("Unpublishing Volume Pool volumes won't know the addresses of deleted nodes: %v", err)
	}
	return r
}

// nodeDeleted remembers the internal IPv4 address of obj, a deleted Node or the
// tombstone of one.
func (r *informerNodeIPResolver) nodeDeleted(obj interface{}) {
	if tombstone, ok := obj.(cache.DeletedFinalStateUnknown); ok {
		obj = tombstone.Obj
	}
	node, ok := obj.(*v1.Node)
	if !ok {
		return
	}
	if ip, err := nodeInternalIPv4(node); err == nil {
		r.deleted.Add(node.Name, ip, deletedVolumePoolNodeTTL)
	}
}

func (r *informerNodeIPResolver) DeletedNodeIP(nodeName string) (string, bool) {
	v, ok := r.deleted.Get(nodeName)
	if !ok || !r.synced() {
		return "", false
	}
	ip := v.(string)
	nodes, err := r.lister.List(labels.Everything())
	if err != nil {
		return "", false
	}
	for _, node := range nodes {
		if nodeIP, err := nodeInternalIPv4(node); err == nil && nodeIP == ip {
			return "", false
		}
	}
	return ip, true
}

// stripVolumePoolNode returns a copy of the Node obj with only what NodeIP
// needs: its name, resource version and addresses. A whole Node also holds its
// conditions, images, labels and annotations, many kilobytes in all, and every
// controller replica caches every node. Anything else is returned unchanged,
// such as the tombstone of a node deleted while the informer was disconnected,
// which holds the node as cached.
func stripVolumePoolNode(obj interface{}) (interface{}, error) {
	node, ok := obj.(*v1.Node)
	if !ok {
		return obj, nil
	}
	return &v1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:            node.Name,
			ResourceVersion: node.ResourceVersion,
		},
		Status: v1.NodeStatus{
			Addresses: slices.Clone(node.Status.Addresses),
		},
	}, nil
}

func (r *informerNodeIPResolver) NodeIP(ctx context.Context, nodeName string) (string, error) {
	if r.synced() {
		node, err := r.lister.Get(nodeName)
		if err == nil {
			return nodeInternalIPv4(node)
		}
		if !apierrors.IsNotFound(err) {
			return "", status.Errorf(codes.Internal, "failed to get node %q from the node cache: %v", nodeName, err)
		}
	}
	// The cache hasn't synced yet, or doesn't have the node: it was added
	// after the cache last heard from the API server, or it doesn't exist.
	// A GET without a resource version reads the node's latest state.
	node, err := r.client.CoreV1().Nodes().Get(ctx, nodeName, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return "", status.Errorf(codes.NotFound, "node %q not found", nodeName)
	}
	if err != nil {
		return "", status.Errorf(codes.Unavailable, "failed to get node %q: %v", nodeName, err)
	}
	klog.V(4).Infof("Got node %q from the API server: the node cache hasn't synced or doesn't have it yet", nodeName)
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
