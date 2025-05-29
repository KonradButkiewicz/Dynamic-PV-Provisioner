package controller

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// RBAC permissions required for the controller to function properly
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=persistentvolumeclaims,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups="",resources=persistentvolumes,verbs=get;list;create;watch
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch
// +kubebuilder:rbac:groups=storage.k8s.io,resources=storageclasses,verbs=get;list;create;watch
// +kubebuilder:rbac:groups="",resources=nodes,verbs=get;list;watch

// PersistentVolumeProvisionerReconciler is a Kubernetes controller that automatically
// creates PersistentVolumes for PersistentVolumeClaims in Pending state.
//
// Key features:
// - Only processes storage classes starting with "auto-" prefix
// - Creates HostPath-based PVs on cluster nodes using round-robin distribution
// - Ensures all PVs for a single pod are created on the same node
// - Supports custom base paths and maintains proper node affinity
type PersistentVolumeProvisionerReconciler struct {
	client.Client
	Scheme *runtime.Scheme
	PvPath string // Base path on the host filesystem where PVs will be created

	// Internal state for round-robin node selection
	nodeList     []corev1.Node
	currentIndex int

	// Track which pods are assigned to which nodes to ensure consistency
	// Key format: "namespace/podname" → node name
	podNodeAssignments map[string]string
}

// hostPathTypePtr is a utility function that converts a HostPathType value to a pointer.
// This is required by the Kubernetes API for the Type field in HostPathVolumeSource.
func hostPathTypePtr(hostPathType corev1.HostPathType) *corev1.HostPathType {
	return &hostPathType
}

// generatePVName creates a unique PersistentVolume name based on pod and PVC names.
// This naming scheme ensures that each PVC gets its own dedicated PV, even when
// multiple PVCs exist within the same pod.
//
// Format: "{podName}-{pvcName}-pv"
// Example: "test-app-0-data-storage-pv"
func generatePVName(podName, pvcName string) string {
	return fmt.Sprintf("%s-%s-pv", podName, pvcName)
}

// shouldCreatePVForStorageClass determines whether a PV should be automatically
// created for a given storage class. This controller only handles storage classes
// that start with the "auto-" prefix, allowing selective provisioning.
//
// Examples:
// - "auto-local" → true (will create PV)
// - "auto-fast" → true (will create PV)
// - "standard" → false (will be ignored)
func shouldCreatePVForStorageClass(storageClass string) bool {
	return strings.HasPrefix(storageClass, "auto-")
}

// getPodKey generates a unique identifier for a pod using namespace and name.
// This key is used internally to track pod-to-node assignments.
//
// Format: "namespace/name"
// Example: "default/test-app-0"
func getPodKey(pod corev1.Pod) string {
	return fmt.Sprintf("%s/%s", pod.Namespace, pod.Name)
}

// getNextNodeName implements round-robin node selection for load balancing.
// It maintains an internal list of cluster nodes and cycles through them
// sequentially to ensure even distribution of PVs across the cluster.
//
// The node list is lazily loaded and cached for performance.
func (r *PersistentVolumeProvisionerReconciler) getNextNodeName(ctx context.Context) (string, error) {
	// Load and cache the node list if not already done
	if len(r.nodeList) == 0 {
		var nodes corev1.NodeList
		if err := r.List(ctx, &nodes); err != nil {
			return "", fmt.Errorf("failed to list nodes: %w", err)
		}
		r.nodeList = nodes.Items
		if len(r.nodeList) == 0 {
			return "", fmt.Errorf("no nodes found in cluster")
		}
		r.currentIndex = 0
	}

	// Select the next node in round-robin fashion
	node := r.nodeList[r.currentIndex]
	r.currentIndex = (r.currentIndex + 1) % len(r.nodeList)
	return node.Name, nil
}

// getOrAssignNodeForPod ensures that all PVs for a single pod are created on the same node.
// This is critical because a pod can only run on one node and must have access to all its volumes.
//
// The function follows this priority order:
// 1. If the pod is already scheduled by Kubernetes → use that node
// 2. If we previously assigned a node for this pod → use that assignment
// 3. If any PV already exists for this pod → use the same node as existing PVs
// 4. Otherwise → assign a new node using round-robin and remember the assignment
func (r *PersistentVolumeProvisionerReconciler) getOrAssignNodeForPod(ctx context.Context, pod corev1.Pod) (string, error) {
	logger := log.FromContext(ctx)
	podKey := getPodKey(pod)

	// Initialize the assignments map if needed
	if r.podNodeAssignments == nil {
		r.podNodeAssignments = make(map[string]string)
	}

	// Priority 1: Check if pod is already scheduled by Kubernetes scheduler
	if pod.Spec.NodeName != "" {
		r.podNodeAssignments[podKey] = pod.Spec.NodeName
		logger.Info("Using already scheduled node", "pod", podKey, "nodeName", pod.Spec.NodeName)
		return pod.Spec.NodeName, nil
	}

	// Priority 2: Check if we already assigned a node for this pod
	if assignedNode, exists := r.podNodeAssignments[podKey]; exists {
		logger.Info("Using previously assigned node", "pod", podKey, "nodeName", assignedNode)
		return assignedNode, nil
	}

	// Priority 3: Check if any PV for this pod already exists and get its target node
	existingNode, err := r.getExistingNodeForPod(ctx, pod)
	if err != nil {
		return "", err
	}
	if existingNode != "" {
		r.podNodeAssignments[podKey] = existingNode
		logger.Info("Using node from existing PV", "pod", podKey, "nodeName", existingNode)
		return existingNode, nil
	}

	// Priority 4: No node assigned yet - use round-robin to select a new one
	targetNode, err := r.getNextNodeName(ctx)
	if err != nil {
		return "", err
	}

	r.podNodeAssignments[podKey] = targetNode
	logger.Info("Assigned new node using round-robin", "pod", podKey, "nodeName", targetNode)
	return targetNode, nil
}

// getExistingNodeForPod searches for any existing PVs created by this controller
// for the specified pod and returns the target node of the first PV found.
// This ensures consistency when creating additional PVs for the same pod.
func (r *PersistentVolumeProvisionerReconciler) getExistingNodeForPod(ctx context.Context, pod corev1.Pod) (string, error) {
	// Search for PVs with labels matching this pod
	pvList := &corev1.PersistentVolumeList{}
	if err := r.List(ctx, pvList, client.MatchingLabels{
		"pod-name":      pod.Name,
		"pod-namespace": pod.Namespace,
		"created-by":    "pv-provisioner",
	}); err != nil {
		return "", fmt.Errorf("failed to list existing PVs for pod %s/%s: %w", pod.Namespace, pod.Name, err)
	}

	// Return the target node of the first matching PV found
	for _, pv := range pvList.Items {
		if targetNode, exists := pv.Labels["target-node"]; exists {
			return targetNode, nil
		}
	}

	return "", nil
}

// Reconcile is the main controller function that implements the reconciliation loop.
// It's called by the controller runtime whenever:
// - A watched resource (Pod) changes
// - The RequeueAfter duration elapses
// - An external event triggers reconciliation
//
// The function processes all pending pods in the cluster and creates the necessary
// PVs for their PVCs that use "auto-" prefixed storage classes.
func (r *PersistentVolumeProvisionerReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	// Retrieve all pods in the cluster for processing
	podList := &corev1.PodList{}
	if err := r.List(ctx, podList); err != nil {
		logger.Error(err, "unable to list pods")
		return ctrl.Result{}, err
	}

	// Process each pod that's in Pending state
	for _, pod := range podList.Items {
		if pod.Status.Phase != corev1.PodPending {
			continue
		}

		// Collect all PVCs that need PVs for this pod
		// We process them together to ensure they all end up on the same node
		var pendingPVCs []struct {
			pvc    corev1.PersistentVolumeClaim
			volume corev1.Volume
		}

		// Examine each volume in the pod's specification
		for _, volume := range pod.Spec.Volumes {
			if volume.PersistentVolumeClaim == nil {
				continue // Skip non-PVC volumes
			}

			pvcName := volume.PersistentVolumeClaim.ClaimName
			namespace := pod.Namespace

			// Fetch the actual PVC object from the API
			pvc := &corev1.PersistentVolumeClaim{}
			if err := r.Get(ctx, types.NamespacedName{
				Name: pvcName, Namespace: namespace,
			}, pvc); err != nil {
				if !errors.IsNotFound(err) {
					logger.Error(err, "unable to get PVC", "pvcName", pvcName, "pod", pod.Name)
				}
				continue
			}

			// Only process PVCs that are in Pending state
			if pvc.Status.Phase != corev1.ClaimPending {
				continue
			}

			// Check if this PVC uses an "auto-" storage class
			storageClass := ""
			if pvc.Spec.StorageClassName != nil {
				storageClass = *pvc.Spec.StorageClassName
			}

			if !shouldCreatePVForStorageClass(storageClass) {
				continue // Skip PVCs that don't use our managed storage classes
			}

			// Check if a PV already exists for this PVC
			pvName := generatePVName(pod.Name, pvcName)
			existingPV := &corev1.PersistentVolume{}
			err := r.Get(ctx, types.NamespacedName{Name: pvName}, existingPV)
			if err == nil {
				logger.Info("PV already exists", "pvName", pvName)
				continue // Skip if PV already exists
			}

			// Add this PVC to the list of PVCs that need PVs
			pendingPVCs = append(pendingPVCs, struct {
				pvc    corev1.PersistentVolumeClaim
				volume corev1.Volume
			}{*pvc, volume})
		}

		// If we found PVCs that need PVs, process them all together
		if len(pendingPVCs) > 0 {
			logger.Info("Found pending Pod with PVCs needing PVs",
				"pod", pod.Name,
				"pvcCount", len(pendingPVCs))

			// Determine the target node for all PVs of this pod
			targetNode, err := r.getOrAssignNodeForPod(ctx, pod)
			if err != nil {
				logger.Error(err, "failed to get target node for pod", "pod", pod.Name)
				continue
			}

			// Create all PVs on the same target node
			for _, pendingPVC := range pendingPVCs {
				if err := r.createPersistentVolume(ctx, pod, pendingPVC.pvc, targetNode); err != nil {
					logger.Error(err, "failed to create PV", "pod", pod.Name, "pvc", pendingPVC.pvc.Name)
					continue
				}

				pvName := generatePVName(pod.Name, pendingPVC.pvc.Name)
				logger.Info("Successfully created PersistentVolume for PVC",
					"pvName", pvName,
					"pvcName", pendingPVC.pvc.Name,
					"targetNode", targetNode)
			}
		}
	}

	// Schedule the next reconciliation in 15 seconds
	// This ensures we regularly check for new PVCs that might need processing
	return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
}

// createPersistentVolume creates a new PersistentVolume for a specific PVC and pod
// on the designated target node. The PV is configured with:
// - HostPath storage pointing to a structured directory path
// - Node affinity to bind it to the specific target node
// - Pre-binding to the requesting PVC
// - Proper labels and annotations for tracking and management
//
// Parameters:
// - ctx: Context for API operations and logging
// - pod: The pod that will use this PV
// - pvc: The PVC that requested this PV
// - targetNodeName: The node where this PV should be created
func (r *PersistentVolumeProvisionerReconciler) createPersistentVolume(ctx context.Context, pod corev1.Pod, pvc corev1.PersistentVolumeClaim, targetNodeName string) error {
	logger := log.FromContext(ctx)

	// Generate a unique name for the PV
	pvName := generatePVName(pod.Name, pvc.Name)

	// Determine the release name for path structuring
	// This helps organize storage by application/release
	releaseName := "default"
	if val, ok := pod.Labels["app.kubernetes.io/instance"]; ok {
		releaseName = val
	} else if val, ok := pod.Labels["release"]; ok {
		releaseName = val
	} else if len(pod.OwnerReferences) > 0 {
		// Fallback to owner reference name (e.g., StatefulSet, Deployment)
		releaseName = pod.OwnerReferences[0].Name
	} else {
		// Final fallback to pod name
		releaseName = pod.Name
	}

	// Construct the storage path with a hierarchical structure:
	// {PvPath}/{namespace}/{releaseName-nodeName}/
	// This provides good organization and prevents path conflicts
	var localPath string

	if r.PvPath != "" && pod.Namespace != "" && releaseName != "" && targetNodeName != "" {
		// Preferred full path structure
		localPath = filepath.Join(r.PvPath, pod.Namespace, releaseName+"-"+targetNodeName)
		logger.Info("Using full path structure", "localPath", localPath)
	} else {
		// Fallback path construction with available components
		if r.PvPath != "" {
			localPath = r.PvPath

			if pod.Namespace != "" {
				localPath = filepath.Join(localPath, pod.Namespace)

				if releaseName != "" && targetNodeName != "" {
					localPath = filepath.Join(localPath, releaseName+"-"+targetNodeName)
				} else if releaseName != "" {
					localPath = filepath.Join(localPath, releaseName)
				} else if targetNodeName != "" {
					localPath = filepath.Join(localPath, targetNodeName)
				}
			}
		} else {
			// Final fallback to default path
			localPath = "/k8s-storage"
		}
		logger.Info("Using partial path structure (fallback)", "localPath", localPath)
	}

	// Extract storage class and capacity from the PVC
	var storageClassName string
	if pvc.Spec.StorageClassName != nil {
		storageClassName = *pvc.Spec.StorageClassName
	}
	storageSize := pvc.Spec.Resources.Requests[corev1.ResourceStorage]

	// Configure node affinity to bind this PV to the specific target node
	// This ensures the PV can only be used by pods scheduled on that node
	nodeAffinity := &corev1.VolumeNodeAffinity{
		Required: &corev1.NodeSelector{
			NodeSelectorTerms: []corev1.NodeSelectorTerm{
				{
					MatchExpressions: []corev1.NodeSelectorRequirement{
						{
							Key:      "kubernetes.io/hostname",
							Operator: corev1.NodeSelectorOpIn,
							Values:   []string{targetNodeName},
						},
					},
				},
			},
		},
	}

	// Create the PersistentVolume object with all necessary configuration
	pv := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: pvName,
			// Labels for identification and management
			Labels: map[string]string{
				"created-by":    "pv-provisioner",
				"pod-name":      pod.Name,
				"pod-namespace": pod.Namespace,
				"release-name":  releaseName,
				"target-node":   targetNodeName,
			},
			// Annotations for additional metadata and tracking
			Annotations: map[string]string{
				"pv-provisioner.io/created-for-pvc": fmt.Sprintf("%s/%s", pvc.Namespace, pvc.Name),
				"pv-provisioner.io/creation-time":   time.Now().Format(time.RFC3339),
				"pv-provisioner.io/target-node":     targetNodeName,
			},
		},
		Spec: corev1.PersistentVolumeSpec{
			StorageClassName: storageClassName,
			Capacity: corev1.ResourceList{
				corev1.ResourceStorage: storageSize,
			},
			AccessModes: pvc.Spec.AccessModes,
			// Retain policy prevents automatic deletion of data
			PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimRetain,
			// HostPath storage configuration
			PersistentVolumeSource: corev1.PersistentVolumeSource{
				HostPath: &corev1.HostPathVolumeSource{
					Path: localPath,
					Type: hostPathTypePtr(corev1.HostPathDirectoryOrCreate),
				},
			},
			// Pre-bind this PV to the requesting PVC
			ClaimRef: &corev1.ObjectReference{
				Kind:       "PersistentVolumeClaim",
				Namespace:  pvc.Namespace,
				Name:       pvc.Name,
				UID:        pvc.UID,
				APIVersion: "v1",
			},
			NodeAffinity: nodeAffinity,
		},
	}

	// Create the PV in the Kubernetes cluster
	logger.Info("Creating PersistentVolume", "pvName", pvName, "path", localPath, "targetNode", targetNodeName)
	if err := r.Create(ctx, pv); err != nil {
		if errors.IsAlreadyExists(err) {
			logger.Info("PV already exists", "pvName", pvName)
			return nil
		}
		return fmt.Errorf("failed to create PV: %w", err)
	}

	logger.Info("Successfully created PersistentVolume", "pvName", pvName)
	return nil
}

// SetupWithManager configures this controller with the controller manager.
// It specifies:
// - Which resource type to watch (Pods)
// - Default configuration values
// - Controller runtime integration
//
// This function is called once during controller initialization.
func (r *PersistentVolumeProvisionerReconciler) SetupWithManager(mgr ctrl.Manager) error {
	// Set default PV path if not provided
	if r.PvPath == "" {
		r.PvPath = "/k8s-storage"
	}

	// Configure the controller to watch Pod resources and trigger reconciliation
	// whenever pods are created, updated, or deleted
	return ctrl.NewControllerManagedBy(mgr).
		For(&corev1.Pod{}).
		Complete(r)
}
