package main

import (
        "flag"
        "os"

        // Import all Kubernetes client auth plugins
        _ "k8s.io/client-go/plugin/pkg/client/auth"

        "k8s.io/apimachinery/pkg/runtime"
        utilruntime "k8s.io/apimachinery/pkg/util/runtime"
        clientgoscheme "k8s.io/client-go/kubernetes/scheme"
        ctrl "sigs.k8s.io/controller-runtime"
        "sigs.k8s.io/controller-runtime/pkg/healthz"
        "sigs.k8s.io/controller-runtime/pkg/log/zap"
        "sigs.k8s.io/controller-runtime/pkg/metrics/server"

        "github.com/example/pv-provisioner/internal/controller"
        // +kubebuilder:scaffold:imports
)

var (
        scheme   = runtime.NewScheme()
        setupLog = ctrl.Log.WithName("setup")
)

func init() {
        utilruntime.Must(clientgoscheme.AddToScheme(scheme))
        // +kubebuilder:scaffold:scheme
}

func main() {
        var metricsAddr string
        var enableLeaderElection bool
        var probeAddr string
        var pvPath string

        flag.StringVar(&metricsAddr, "metrics-bind-address", ":8080", "The address the metric endpoint binds to.")
        flag.StringVar(&probeAddr, "health-probe-bind-address", ":8081", "The address the probe endpoint binds to.")
        flag.BoolVar(&enableLeaderElection, "leader-elect", false,
                "Enable leader election for controller manager. Enabling this will ensure there is only one active controller manager.")
        flag.StringVar(&pvPath, "pv-path", "/k8s-storage", "The base path where PersistentVolumes will be created.")

        opts := zap.Options{
                Development: true,
        }
        opts.BindFlags(flag.CommandLine)
        flag.Parse()

        ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))

        mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
                Scheme:                 scheme,
                Metrics:                server.Options{BindAddress: metricsAddr},
                HealthProbeBindAddress: probeAddr,
                LeaderElection:         enableLeaderElection,
                LeaderElectionID:       "c67622b2.example.com",
        })
        if err != nil {
                setupLog.Error(err, "unable to start manager")
                os.Exit(1)
        }

        // Create and register the controller
        if err = (&controller.PersistentVolumeProvisionerReconciler{
                Client: mgr.GetClient(),
                Scheme: mgr.GetScheme(),
                PvPath: pvPath,
        }).SetupWithManager(mgr); err != nil {
                setupLog.Error(err, "unable to create controller", "controller", "PersistentVolumeProvisioner")
                os.Exit(1)
        }
        // +kubebuilder:scaffold:builder

        if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
                setupLog.Error(err, "unable to set up health check")
                os.Exit(1)
        }
        if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
                setupLog.Error(err, "unable to set up ready check")
                os.Exit(1)
        }

        setupLog.Info("starting manager")
        setupLog.Info("PV path configured", "path", pvPath)
        if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
                setupLog.Error(err, "problem running manager")
                os.Exit(1)
        }
}
