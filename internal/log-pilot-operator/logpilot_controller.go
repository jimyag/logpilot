/*
Copyright 2026.

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

package operator

import (
	"context"
	"os"

	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	logpilotv1alpha1 "github.com/jimyag/logpilot/api/v1alpha1"
)

const logPilotFinalizer = "logpilot.logpilot.jimyag.com/finalizer"

// LogPilotReconciler reconciles a LogPilot object
type LogPilotReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=logpilot.logpilot.jimyag.com,resources=logpilots,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=logpilot.logpilot.jimyag.com,resources=logpilots/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=logpilot.logpilot.jimyag.com,resources=logpilots/finalizers,verbs=update
// +kubebuilder:rbac:groups=apps,resources=deployments;statefulsets;daemonsets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=pods;nodes;services;serviceaccounts;secrets;configmaps;events,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=rbac.authorization.k8s.io,resources=roles;rolebindings;clusterroles;clusterrolebindings,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=admissionregistration.k8s.io,resources=mutatingwebhookconfigurations,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=coordination.k8s.io,resources=leases,verbs=get;list;watch;create;update;patch;delete

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the LogPilot object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.23.3/pkg/reconcile
func (r *LogPilotReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	var lp logpilotv1alpha1.LogPilot
	if err := r.Get(ctx, req.NamespacedName, &lp); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if !lp.ObjectMeta.DeletionTimestamp.IsZero() {
		if controllerutil.ContainsFinalizer(&lp, logPilotFinalizer) {
			if err := cleanupLogPilot(ctx, r.Client, &lp); err != nil {
				log.Error(err, "Failed to clean up LogPilot resources")
				return ctrl.Result{}, err
			}
			controllerutil.RemoveFinalizer(&lp, logPilotFinalizer)
			return ctrl.Result{}, r.Update(ctx, &lp)
		}
		return ctrl.Result{}, nil
	}

	if !controllerutil.ContainsFinalizer(&lp, logPilotFinalizer) {
		controllerutil.AddFinalizer(&lp, logPilotFinalizer)
		if err := r.Update(ctx, &lp); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}

	apiImage := os.Getenv("LOG_PILOT_API_IMAGE")
	if apiImage == "" {
		apiImage = "ghcr.io/jimyag/logpilot/log-pilot-api:latest"
	}
	agentImage := os.Getenv("LOG_PILOT_AGENT_IMAGE")
	if agentImage == "" {
		agentImage = "ghcr.io/jimyag/logpilot/log-pilot-agent:latest"
	}

	for _, obj := range buildSupportObjects(&lp) {
		if obj.GetNamespace() != "" {
			if err := ctrl.SetControllerReference(&lp, obj, r.Scheme); err != nil {
				return ctrl.Result{}, err
			}
		}
		if err := reconcileObject(ctx, r.Client, obj); err != nil {
			log.Error(err, "Failed to reconcile support object", "kind", obj.GetObjectKind(), "name", obj.GetName())
			return ctrl.Result{}, err
		}
	}

	// Reconcile log-pilot-api Deployment.
	apiDeploy := buildAPIDeployment(&lp, apiImage)
	if err := ctrl.SetControllerReference(&lp, apiDeploy, r.Scheme); err != nil {
		return ctrl.Result{}, err
	}
	if err := reconcileDeployment(ctx, r.Client, apiDeploy); err != nil {
		log.Error(err, "failed to reconcile log-pilot-api deployment")
		return ctrl.Result{}, err
	}

	// Reconcile log-pilot-agent DaemonSet.
	agentDS := buildAgentDaemonSet(&lp, agentImage)
	if err := ctrl.SetControllerReference(&lp, agentDS, r.Scheme); err != nil {
		return ctrl.Result{}, err
	}
	if err := reconcileDaemonSet(ctx, r.Client, agentDS); err != nil {
		log.Error(err, "failed to reconcile log-pilot-agent daemonset")
		return ctrl.Result{}, err
	}

	log.Info("reconciled LogPilot", "name", lp.Name)
	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *LogPilotReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&logpilotv1alpha1.LogPilot{}).
		Named("logpilot").
		Complete(r)
}
