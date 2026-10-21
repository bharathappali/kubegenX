package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/bharathappali/kubegenx/internal/app"
	"github.com/bharathappali/kubegenx/internal/store"
	"github.com/bharathappali/kubegenx/internal/verify"
	"google.golang.org/grpc"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

func main() {
	if len(os.Args) > 1 && os.Args[1] == "verify" {
		fs := flag.NewFlagSet("verify", flag.ExitOnError)
		data := fs.String("data", "/data", "data root")
		ds := fs.String("dataset", "", "dataset name")
		_ = fs.Parse(os.Args[2:])
		rep, err := verify.Dataset(*data, *ds)
		fmt.Printf("files=%d expected=%d rows=%d resets=%d\n", rep.Files, rep.Expected, rep.Rows, rep.Resets)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}
	data := env("DATA_DIR", "/data")
	state := env("STATE_DIR", "/data/controller")
	scratch := env("SCRATCH_DIR", "/scratch")
	ns := env("POD_NAMESPACE", "kube-gen-x")
	httpAddr := env("HTTP_ADDR", ":8080")
	grpcAddr := env("GRPC_ADDR", ":9091")
	workerImage := env("KUBEGENX_WORKER_IMAGE", "quay.io/bharathappali/kube-gen-x:worker-v0.1.0")
	thanosImage := env("KUBEGENX_THANOS_IMAGE", "quay.io/thanos/thanos:v0.42.4")

	st, err := store.Open(state)
	if err != nil {
		slog.Error("sqlite", "err", err)
		os.Exit(1)
	}
	defer st.Close()

	cfg, err := rest.InClusterConfig()
	if err != nil {
		cfg, err = clientcmd.BuildConfigFromFlags("", os.Getenv("KUBECONFIG"))
		if err != nil {
			slog.Error("kubeconfig", "err", err)
			os.Exit(1)
		}
	}
	kube, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		slog.Error("client", "err", err)
		os.Exit(1)
	}
	dyn, err := dynamic.NewForConfig(cfg)
	if err != nil {
		slog.Error("dynamic", "err", err)
		os.Exit(1)
	}
	selfTest(data)
	a := &app.App{
		NS: ns, Data: data, Scratch: scratch, Store: st, Kube: kube, Dyn: dyn,
		WorkerImage: workerImage, ThanosImage: thanosImage, Hub: app.NewHub(),
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	go a.Loop(ctx)
	go func() {
		lis, err := net.Listen("tcp", grpcAddr)
		if err != nil {
			slog.Error("grpc listen", "err", err)
			os.Exit(1)
		}
		s := grpc.NewServer()
		app.RegisterGRPC(s, a)
		slog.Info("grpc", "addr", grpcAddr)
		if err := s.Serve(lis); err != nil {
			slog.Error("grpc", "err", err)
		}
	}()
	srv := &http.Server{Addr: httpAddr, Handler: a.Handler(), ReadHeaderTimeout: 10 * time.Second}
	slog.Info("http", "addr", httpAddr)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		slog.Error("http", "err", err)
		os.Exit(1)
	}
}

func selfTest(data string) {
	dir := filepath.Join(data, ".selftest")
	if err := os.MkdirAll(dir, 0o777); err != nil {
		slog.Info("scratchMode", "value", "always", "reason", err.Error())
		return
	}
	f, err := os.OpenFile(filepath.Join(dir, "lock"), os.O_CREATE|os.O_RDWR, 0o666)
	if err != nil {
		slog.Info("scratchMode", "value", "always", "reason", err.Error())
		return
	}
	defer f.Close()
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		slog.Info("scratchMode", "value", "always", "reason", err.Error())
		return
	}
	_ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
	slog.Info("volume self-test", "locks", "ok", "scratchMode", "auto")
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
