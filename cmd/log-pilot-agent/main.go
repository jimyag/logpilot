package main

import (
	"context"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/config"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"

	logpilotv1alpha1 "github.com/jimyag/logpilot/api/v1alpha1"
	"github.com/jimyag/logpilot/internal/log-pilot-agent/status"
	"github.com/jimyag/logpilot/internal/log-pilot-agent/watcher"
)

var scheme = runtime.NewScheme()

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(logpilotv1alpha1.AddToScheme(scheme))
}

func main() {
	ctrl.SetLogger(zap.New())
	log := ctrl.Log.WithName("log-pilot-agent")

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg, err := config.GetConfig()
	if err != nil {
		log.Error(err, "failed to get k8s config")
		os.Exit(1)
	}
	c, err := client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		log.Error(err, "failed to create k8s client")
		os.Exit(1)
	}

	statusSrv := status.New()

	// Start /status HTTP server.
	statusAddr := envOrDefault("STATUS_ADDR", ":9090")
	go func() {
		if err := statusSrv.ListenAndServe(statusAddr); err != nil && err != http.ErrServerClosed {
			log.Error(err, "status server error")
		}
	}()

	nodeName := os.Getenv("NODE_NAME")
	if nodeName == "" {
		log.Error(nil, "NODE_NAME env var is required")
		os.Exit(1)
	}

	w := watcher.New(watcher.Config{
		NodeName:  nodeName,
		LogDir:    envOrDefault("LOG_DIR", "/var/log/log-pilot"),
		ConfigDir: envOrDefault("CONFIG_DIR", "/var/lib/log-pilot-agent/conf"),
		MetaDir:   envOrDefault("META_DIR", "/var/lib/log-pilot-agent/meta"),
	}, c, statusSrv)

	log.Info("log-pilot-agent started", "node", nodeName, "statusAddr", statusAddr)

	// Start pod watcher — blocks until ctx is cancelled.
	go func() {
		if err := w.Start(ctx); err != nil {
			log.Error(err, "watcher error")
		}
	}()

	<-ctx.Done()
	log.Info("log-pilot-agent shutting down gracefully")
	// Runners drain themselves via Stop() + shutdown() path before process exits.
}

func envOrDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
