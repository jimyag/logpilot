package operator

import (
	"context"

	admissionv1 "k8s.io/api/admissionregistration/v1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	logpilotv1alpha1 "github.com/jimyag/logpilot/api/v1alpha1"
)

const (
	apiName          = "log-pilot-api"
	agentName        = "log-pilot-agent"
	webhookName      = "log-pilot-api"
	webhookPath      = "/mutate-pod"
	webhookPort      = int32(8443)
	agentKubeletPath = "/var/lib/kubelet"
	agentPodLogsPath = "/var/log/pods"
)

func buildAPIDeployment(lp *logpilotv1alpha1.LogPilot, image string) *appsv1.Deployment {
	replicas := lp.Spec.API.Replicas
	if replicas == 0 {
		replicas = 2
	}
	labels := map[string]string{
		"app.kubernetes.io/name":      "log-pilot-api",
		"app.kubernetes.io/component": "webhook",
	}
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "log-pilot-api",
			Namespace: lp.Namespace,
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{MatchLabels: labels},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec: corev1.PodSpec{
					ServiceAccountName: apiName,
					Containers: []corev1.Container{{
						Name:      apiName,
						Image:     image,
						Command:   []string{"/usr/local/bin/log-pilot-api"},
						Resources: lp.Spec.API.Resources,
						Ports: []corev1.ContainerPort{{
							Name:          "https",
							ContainerPort: webhookPort,
						}},
					}},
				},
			},
		},
	}
}

func buildAgentDaemonSet(lp *logpilotv1alpha1.LogPilot, image string) *appsv1.DaemonSet {
	logDir := lp.Spec.Agent.LogDir
	if logDir == "" {
		logDir = "/var/log/log-pilot"
	}
	metaDir := lp.Spec.Agent.MetaDir
	if metaDir == "" {
		metaDir = "/var/lib/log-pilot-agent"
	}

	labels := map[string]string{
		"app.kubernetes.io/name":      "log-pilot-agent",
		"app.kubernetes.io/component": "agent",
	}
	hostPathType := corev1.HostPathDirectoryOrCreate

	return &appsv1.DaemonSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "log-pilot-agent",
			Namespace: lp.Namespace,
		},
		Spec: appsv1.DaemonSetSpec{
			Selector: &metav1.LabelSelector{MatchLabels: labels},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec: corev1.PodSpec{
					ServiceAccountName: agentName,
					SecurityContext: &corev1.PodSecurityContext{
						RunAsUser:  ptrInt64(0),
						RunAsGroup: ptrInt64(0),
					},
					Containers: []corev1.Container{{
						Name:      agentName,
						Image:     image,
						Command:   []string{"/usr/local/bin/log-pilot-agent"},
						Resources: lp.Spec.Agent.Resources,
						Env: []corev1.EnvVar{
							{
								Name: "NODE_NAME",
								ValueFrom: &corev1.EnvVarSource{
									FieldRef: &corev1.ObjectFieldSelector{FieldPath: "spec.nodeName"},
								},
							},
							{Name: "LOG_DIR", Value: logDir},
							{Name: "META_DIR", Value: metaDir},
						},
						VolumeMounts: []corev1.VolumeMount{
							{Name: "log-dir", MountPath: logDir},
							{Name: "meta-dir", MountPath: metaDir},
							{Name: "pod-logs", MountPath: agentPodLogsPath, ReadOnly: true},
							{Name: "kubelet", MountPath: agentKubeletPath, ReadOnly: true},
						},
					}},
					Volumes: []corev1.Volume{
						{
							Name: "log-dir",
							VolumeSource: corev1.VolumeSource{
								HostPath: &corev1.HostPathVolumeSource{
									Path: logDir,
									Type: &hostPathType,
								},
							},
						},
						{
							Name: "meta-dir",
							VolumeSource: corev1.VolumeSource{
								HostPath: &corev1.HostPathVolumeSource{
									Path: metaDir,
									Type: &hostPathType,
								},
							},
						},
						{
							Name: "pod-logs",
							VolumeSource: corev1.VolumeSource{
								HostPath: &corev1.HostPathVolumeSource{
									Path: agentPodLogsPath,
									Type: &hostPathType,
								},
							},
						},
						{
							Name: "kubelet",
							VolumeSource: corev1.VolumeSource{
								HostPath: &corev1.HostPathVolumeSource{
									Path: agentKubeletPath,
									Type: &hostPathType,
								},
							},
						},
					},
				},
			},
		},
	}
}

func buildSupportObjects(lp *logpilotv1alpha1.LogPilot) []client.Object {
	return []client.Object{
		buildServiceAccount(lp, apiName),
		buildServiceAccount(lp, agentName),
		buildAPIService(lp),
		buildAPIClusterRole(lp),
		buildAPIClusterRoleBinding(lp),
		buildAgentClusterRole(lp),
		buildAgentClusterRoleBinding(lp),
		buildMutatingWebhook(lp),
	}
}

func buildManagedObjects(lp *logpilotv1alpha1.LogPilot, apiImage, agentImage string) []client.Object {
	objects := buildSupportObjects(lp)
	objects = append(objects, buildAPIDeployment(lp, apiImage), buildAgentDaemonSet(lp, agentImage))
	return objects
}

func buildServiceAccount(lp *logpilotv1alpha1.LogPilot, name string) *corev1.ServiceAccount {
	return &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: lp.Namespace}}
}

func buildAPIService(lp *logpilotv1alpha1.LogPilot) *corev1.Service {
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: apiName, Namespace: lp.Namespace},
		Spec: corev1.ServiceSpec{
			Selector: map[string]string{
				"app.kubernetes.io/name":      apiName,
				"app.kubernetes.io/component": "webhook",
			},
			Ports: []corev1.ServicePort{{
				Name:       "https",
				Port:       webhookPort,
				TargetPort: intstrFromInt(int(webhookPort)),
			}},
		},
	}
}

func buildAPIClusterRole(lp *logpilotv1alpha1.LogPilot) *rbacv1.ClusterRole {
	return &rbacv1.ClusterRole{
		ObjectMeta: metav1.ObjectMeta{Name: lp.Namespace + "-" + apiName},
		Rules: []rbacv1.PolicyRule{
			{APIGroups: []string{""}, Resources: []string{"secrets", "configmaps", "events"}, Verbs: []string{"get", "list", "watch", "create", "update", "patch", "delete"}},
			{APIGroups: []string{"coordination.k8s.io"}, Resources: []string{"leases"}, Verbs: []string{"get", "list", "watch", "create", "update", "patch", "delete"}},
			{APIGroups: []string{"logpilot.logpilot.jimyag.com"}, Resources: []string{"logpilotpolicies"}, Verbs: []string{"get", "list", "watch"}},
			{APIGroups: []string{"admissionregistration.k8s.io"}, Resources: []string{"mutatingwebhookconfigurations"}, Verbs: []string{"get", "list", "watch", "update", "patch"}},
		},
	}
}

func buildAPIClusterRoleBinding(lp *logpilotv1alpha1.LogPilot) *rbacv1.ClusterRoleBinding {
	return &rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: lp.Namespace + "-" + apiName},
		RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "ClusterRole", Name: lp.Namespace + "-" + apiName},
		Subjects:   []rbacv1.Subject{{Kind: "ServiceAccount", Name: apiName, Namespace: lp.Namespace}},
	}
}

func buildAgentClusterRole(lp *logpilotv1alpha1.LogPilot) *rbacv1.ClusterRole {
	return &rbacv1.ClusterRole{
		ObjectMeta: metav1.ObjectMeta{Name: lp.Namespace + "-" + agentName},
		Rules: []rbacv1.PolicyRule{
			{APIGroups: []string{""}, Resources: []string{"pods", "events"}, Verbs: []string{"get", "list", "watch"}},
			{APIGroups: []string{"logpilot.logpilot.jimyag.com"}, Resources: []string{"clusterlogpilotpolicies"}, Verbs: []string{"get", "list", "watch"}},
		},
	}
}

func buildAgentClusterRoleBinding(lp *logpilotv1alpha1.LogPilot) *rbacv1.ClusterRoleBinding {
	return &rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: lp.Namespace + "-" + agentName},
		RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "ClusterRole", Name: lp.Namespace + "-" + agentName},
		Subjects:   []rbacv1.Subject{{Kind: "ServiceAccount", Name: agentName, Namespace: lp.Namespace}},
	}
}

func buildMutatingWebhook(lp *logpilotv1alpha1.LogPilot) *admissionv1.MutatingWebhookConfiguration {
	sideEffects := admissionv1.SideEffectClassNone
	failurePolicy := admissionv1.Ignore
	path := webhookPath
	port := webhookPort
	return &admissionv1.MutatingWebhookConfiguration{
		ObjectMeta: metav1.ObjectMeta{Name: webhookName},
		Webhooks: []admissionv1.MutatingWebhook{{
			Name:                    "pods.logpilot.jimyag.com",
			AdmissionReviewVersions: []string{"v1"},
			SideEffects:             &sideEffects,
			FailurePolicy:           &failurePolicy,
			ClientConfig: admissionv1.WebhookClientConfig{
				Service: &admissionv1.ServiceReference{
					Name:      apiName,
					Namespace: lp.Namespace,
					Path:      &path,
					Port:      &port,
				},
			},
			Rules: []admissionv1.RuleWithOperations{{
				Operations: []admissionv1.OperationType{admissionv1.Create},
				Rule: admissionv1.Rule{
					APIGroups:   []string{""},
					APIVersions: []string{"v1"},
					Resources:   []string{"pods"},
				},
			}},
		}},
	}
}

func reconcileDeployment(ctx context.Context, c client.Client, desired *appsv1.Deployment) error {
	existing := &appsv1.Deployment{}
	err := c.Get(ctx, client.ObjectKeyFromObject(desired), existing)
	if err != nil {
		if !apierrors.IsNotFound(err) {
			return err
		}
		return c.Create(ctx, desired)
	}
	existing.Spec = desired.Spec
	return c.Update(ctx, existing)
}

func reconcileObject(ctx context.Context, c client.Client, desired client.Object) error {
	existing := desired.DeepCopyObject().(client.Object)
	err := c.Get(ctx, client.ObjectKeyFromObject(desired), existing)
	if err != nil {
		if !apierrors.IsNotFound(err) {
			return err
		}
		return c.Create(ctx, desired)
	}
	preserveImmutableAndRuntimeFields(existing, desired)
	desired.SetResourceVersion(existing.GetResourceVersion())
	return c.Update(ctx, desired)
}

func preserveImmutableAndRuntimeFields(existing, desired client.Object) {
	existingSvc, ok := existing.(*corev1.Service)
	if ok {
		desiredSvc := desired.(*corev1.Service)
		desiredSvc.Spec.ClusterIP = existingSvc.Spec.ClusterIP
		desiredSvc.Spec.ClusterIPs = existingSvc.Spec.ClusterIPs
		desiredSvc.Spec.IPFamilies = existingSvc.Spec.IPFamilies
		desiredSvc.Spec.IPFamilyPolicy = existingSvc.Spec.IPFamilyPolicy
	}

	existingWebhook, ok := existing.(*admissionv1.MutatingWebhookConfiguration)
	if ok {
		desiredWebhook := desired.(*admissionv1.MutatingWebhookConfiguration)
		for i := range desiredWebhook.Webhooks {
			for _, current := range existingWebhook.Webhooks {
				if desiredWebhook.Webhooks[i].Name == current.Name {
					desiredWebhook.Webhooks[i].ClientConfig.CABundle = current.ClientConfig.CABundle
				}
			}
		}
	}
}

func ptrInt64(v int64) *int64 {
	return &v
}

func intstrFromInt(v int) intstr.IntOrString {
	return intstr.FromInt(v)
}

func reconcileDaemonSet(ctx context.Context, c client.Client, desired *appsv1.DaemonSet) error {
	existing := &appsv1.DaemonSet{}
	err := c.Get(ctx, client.ObjectKeyFromObject(desired), existing)
	if err != nil {
		if !apierrors.IsNotFound(err) {
			return err
		}
		return c.Create(ctx, desired)
	}
	existing.Spec = desired.Spec
	return c.Update(ctx, existing)
}

func cleanupLogPilot(ctx context.Context, c client.Client, lp *logpilotv1alpha1.LogPilot) error {
	for _, obj := range buildManagedObjects(lp, "", "") {
		if err := c.Delete(ctx, obj); err != nil && !apierrors.IsNotFound(err) {
			return err
		}
	}
	return nil
}
