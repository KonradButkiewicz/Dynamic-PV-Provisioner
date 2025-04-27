package controller

import (
	"context"
	"fmt"
	"path/filepath"
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

// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=persistentvolumeclaims,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups="",resources=persistentvolumes,verbs=get;list;create;watch
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch
// +kubebuilder:rbac:groups=storage.k8s.io,resources=storageclasses,verbs=get;list;create;watch

// PersistentVolumeProvisionerReconciler is a controller responsible for dynamically
// creating PersistentVolumes for PersistentVolumeClaims in the Pending state
// using local HostPath directories on selected cluster nodes.
type PersistentVolumeProvisionerReconciler struct {
	client.Client
	Scheme *runtime.Scheme
	// PvPath specifies the base path on the host where volumes will be created
	PvPath string
}

// hostPathTypePtr converts a HostPathType value to a pointer to that value,
// which is required by the Kubernetes API for the Type field in HostPathVolumeSource.
func hostPathTypePtr(hostPathType corev1.HostPathType) *corev1.HostPathType {
	return &hostPathType
}

// generatePVName generates a name for a PersistentVolume based on the pod name.
// The suffix "-PV" is added to the pod name.
func generatePVName(podName string) string {
	return podName + "-pv"
}

// Reconcile is the main controller function called each time an observed resource changes
// or after the time specified in RequeueAfter has elapsed.
// The function searches for pods in the Pending state that use PersistentVolumeClaims,
// and creates appropriate PersistentVolumes for them.
func (r *PersistentVolumeProvisionerReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	// Get the list of Pods in the entire cluster
	podList := &corev1.PodList{}
	if err := r.List(ctx, podList); err != nil {
		logger.Error(err, "unable to list pods")
		return ctrl.Result{}, err
	}

	// We're only interested in Pending pods
	for _, pod := range podList.Items {
		if pod.Status.Phase != corev1.PodPending {
			continue
		}

		for _, volume := range pod.Spec.Volumes {
			if volume.PersistentVolumeClaim != nil {
				pvcName := volume.PersistentVolumeClaim.ClaimName
				namespace := pod.Namespace

				pvc := &corev1.PersistentVolumeClaim{}
				if err := r.Get(ctx, types.NamespacedName{
					Name: pvcName, Namespace: namespace,
				}, pvc); err != nil {
					logger.Error(err, "unable to get PVC", "pvcName", pvcName)
					continue
				}

				if pvc.Status.Phase == corev1.ClaimPending {
					logger.Info("Found pending Pod needing PV",
						"pod", pod.Name,
						"pvc", pvcName)

					// Check if PV already exists (e.g., after operator restart)
					pvName := generatePVName(pod.Name)
					existingPV := &corev1.PersistentVolume{}
					err := r.Get(ctx, types.NamespacedName{Name: pvName}, existingPV)
					if err == nil {
						logger.Info("PV already exists", "pvName", pvName)
						continue
					}

					// Create the PV
					if err := r.createPersistentVolume(ctx, pod, *pvc); err != nil {
						logger.Error(err, "failed to create PV", "pod", pod.Name)
						continue
					}

					logger.Info("Successfully created PersistentVolume for PVC",
						"pvName", pvName,
						"pvcName", pvcName)
				}
			}
		}
	}

	// Reconcile every 15 seconds to check more frequently for new PVCs to handle
	return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
}

// createPersistentVolume creates a new PersistentVolume for the given pod and PVC.
// The function generates an appropriate PV name, determines the path in the host filesystem,
// and configures the PV to be bound to both the specific PVC and cluster node.
//
// Parameters:
// - ctx: Context used for communication with the Kubernetes API
// - pod: Pod for which the PV is being created
// - pvc: PersistentVolumeClaim for which the PV is being created
//
// Returns an error if PV creation fails.
func (r *PersistentVolumeProvisionerReconciler) createPersistentVolume(ctx context.Context, pod corev1.Pod, pvc corev1.PersistentVolumeClaim) error {
	logger := log.FromContext(ctx)

	// Generate PV name based on pod name
	pvName := generatePVName(pod.Name)

	// Extract release name from pod labels - default to pod name if not found
	releaseName := "default"
	if val, ok := pod.Labels["app.kubernetes.io/instance"]; ok {
		releaseName = val
	} else if val, ok := pod.Labels["release"]; ok {
		releaseName = val
	} else {
		// Fallback to pod owner reference name if available
		if len(pod.OwnerReferences) > 0 {
			releaseName = pod.OwnerReferences[0].Name
		} else {
			// If no owner or label, use pod name as fallback
			releaseName = pod.Name
		}
	}

	// Create path in format: /hostParentPath/namespace/releaseName/pvName
	var localPath string

	// Check if every element of prefered path is avaiable
	if r.PvPath != "" && pod.Namespace != "" && releaseName != "" && pod.Spec.Hostname != "" {
		// Full path prefered
		localPath = filepath.Join(r.PvPath, pod.Namespace, releaseName+"-"+pod.Spec.Hostname)
		logger.Info("Using full path structure", "localPath", localPath)
	} else {
		// Fallback - dynamic path building with avaiable elements
		if r.PvPath != "" {
			localPath = r.PvPath

			if pod.Namespace != "" {
				localPath = filepath.Join(localPath, pod.Namespace)

				if releaseName != "" && pod.Spec.Hostname != "" {
					localPath = filepath.Join(localPath, releaseName+"-"+pod.Spec.Hostname)
				} else if releaseName != "" {
					localPath = filepath.Join(localPath, releaseName)
				} else if pod.Spec.Hostname != "" {
					localPath = filepath.Join(localPath, pod.Spec.Hostname)
				}
			}
		} else {
			localPath = "/k8s-storage"
		}
		logger.Info("Using partial path structure (fallback)", "localPath", localPath)
	}

	// Get storage class from PVC if specified
	var storageClassName string
	if pvc.Spec.StorageClassName != nil {
		storageClassName = *pvc.Spec.StorageClassName
	}

	// Get requested storage from PVC
	storageSize := pvc.Spec.Resources.Requests[corev1.ResourceStorage]

	var nodeAffinity *corev1.VolumeNodeAffinity

	// Check if every pod has node
	if pod.Spec.NodeName != "" {
		logger.Info("Adding NodeAffinity to PV", "nodeName", pod.Spec.NodeName)
		nodeAffinity = &corev1.VolumeNodeAffinity{
			Required: &corev1.NodeSelector{
				NodeSelectorTerms: []corev1.NodeSelectorTerm{
					{
						MatchExpressions: []corev1.NodeSelectorRequirement{
							{
								Key:      "kubernetes.io/hostname",
								Operator: corev1.NodeSelectorOpIn,
								Values:   []string{pod.Spec.NodeName},
							},
						},
					},
				},
			},
		}
	} else {
		logger.Info("Pod doesn't have assigned node yet, creating PV without NodeAffinity", "pod", pod.Name)
		// nodeAffinity pozostaje nil
	}

	// Create PV object
	pv := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: pvName,
			Labels: map[string]string{
				"created-by":    "pv-provisioner",
				"pod-name":      pod.Name,
				"pod-namespace": pod.Namespace,
				"release-name":  releaseName,
			},
			Annotations: map[string]string{
				"pv-provisioner.io/created-for-pvc": fmt.Sprintf("%s/%s", pvc.Namespace, pvc.Name),
				"pv-provisioner.io/creation-time":   time.Now().Format(time.RFC3339),
			},
		},
		Spec: corev1.PersistentVolumeSpec{

			StorageClassName: storageClassName,
			Capacity: corev1.ResourceList{
				corev1.ResourceStorage: storageSize,
			},
			AccessModes:                   pvc.Spec.AccessModes,
			PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimRetain,
			PersistentVolumeSource: corev1.PersistentVolumeSource{
				HostPath: &corev1.HostPathVolumeSource{
					Path: localPath,
					Type: hostPathTypePtr(corev1.HostPathDirectoryOrCreate),
				},
			},
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

	// Create the PV in Kubernetes
	logger.Info("Creating PersistentVolume", "pvName", pvName, "path", localPath)
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

// SetupWithManager configures the controller with the controller manager,
// specifying which resource to watch (Pod) and setting the default path for PVs
// if one hasn't been provided.
func (r *PersistentVolumeProvisionerReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if r.PvPath == "" {
		r.PvPath = "/k8s-storage"
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&corev1.Pod{}).
		Complete(r)
}
