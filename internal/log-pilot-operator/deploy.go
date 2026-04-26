package operator

import (
	"context"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	logpilotv1alpha1 "github.com/jimyag/logpilot/api/v1alpha1"
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
					ServiceAccountName: "log-pilot-api",
					Containers: []corev1.Container{{
						Name:      "log-pilot-api",
						Image:     image,
						Resources: lp.Spec.API.Resources,
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
					ServiceAccountName: "log-pilot-agent",
					Containers: []corev1.Container{{
						Name:      "log-pilot-agent",
						Image:     image,
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
					},
				},
			},
		},
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
