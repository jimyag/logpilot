package main

import (
	"os"

	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/config"

	webhook "github.com/jimyag/auto-cert-webhook"
	logpilotv1alpha1 "github.com/jimyag/logpilot/api/v1alpha1"
	logpilotapi "github.com/jimyag/logpilot/internal/log-pilot-api"
)

var scheme = runtime.NewScheme()

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(logpilotv1alpha1.AddToScheme(scheme))
}

func main() {
	cfg, err := config.GetConfig()
	if err != nil {
		panic(err)
	}
	c, err := client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		panic(err)
	}
	if err := webhook.Run(logpilotapi.New(c, scheme)); err != nil {
		os.Exit(1)
	}
}
