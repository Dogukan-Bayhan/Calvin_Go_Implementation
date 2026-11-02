// calvin-lab, Calvin için sürekli çalışan bir test ortamıdır. Bir Calvin
// kümesi kurar, üzerinde ayarlanabilir iş yükü çalıştırır, düzenli olarak
// checkpoint alır, yükü kısa süre durdurup replikaların birebir aynı
// olduğunu ve iş yükü değişmezlerini doğrular. Metrikleri Prometheus
// formatında /metrics'te, logları JSON olarak stdout'a ve isteğe bağlı bir
// dosyaya yazar.
//
// HTTP API:
//
//	GET  /metrics          Prometheus metrikleri
//	GET  /api/settings     aktif yük ayarları
//	POST /api/settings     ayarları kısmen günceller, örn. {"clients":512,"multipartition":0.5}
//	POST /api/verify       hemen doğrulama çalıştırır
//	POST /api/checkpoint   hemen checkpoint alır
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"io"
	"log"
	"log/slog"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"calvin"
	"calvin/internal/lab"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func main() {
	var (
		addr         = flag.String("addr", ":2112", "HTTP dinleme adresi")
		replicas     = flag.Int("replicas", 3, "replika sayısı")
		partitions   = flag.Int("partitions", 4, "replika başına partition sayısı")
		mode         = flag.String("mode", "paxos", "replikasyon: async | paxos")
		epoch        = flag.Duration("epoch", 10*time.Millisecond, "sequencer epoch süresi")
		netDelay     = flag.Duration("net", time.Millisecond, "düğümler arası tek yönlü ağ gecikmesi")
		diskLatency  = flag.Duration("disk-latency", 0, ">0 ise SimDiskEngine kullanılır (soğuk okuma gecikmesi)")
		prefetch     = flag.Duration("prefetch", 0, "soğuk veriye dokunan işlemler için sequencer gecikmesi (0: kapalı)")
		evictPerSec  = flag.Int("evict", 200, "disk modunda saniyede soğuğa taşınan kayıt sayısı")
		cpDir        = flag.String("checkpoint-dir", "", "checkpoint dosyaları için dizin (boşsa bellekte)")
		cpEvery      = flag.Duration("checkpoint-every", 15*time.Second, "checkpoint aralığı (0: kapalı)")
		verifyEvery  = flag.Duration("verify-every", 20*time.Second, "canlı doğrulama aralığı (0: kapalı)")
		summaryEvery = flag.Duration("summary-every", 5*time.Second, "yük özeti log aralığı")
		logFile      = flag.String("log-file", "", "JSON logların ayrıca yazılacağı dosya")
		logLevel     = flag.String("log-level", "info", "debug | info | warn | error (debug her işlemi loglar)")
		records      = flag.Int("records", 10000, "micro: partition başına kayıt")
		accounts     = flag.Int("accounts", 1000, "bank: partition başına hesap")
		customers    = flag.Int("customers", 100, "bank: partition başına müşteri (indeks kaydı)")
		workload     = flag.String("workload", "mixed", "micro | bank | mixed")
		clients      = flag.Int("clients", 128, "eşzamanlı istemci")
		mp           = flag.Float64("mp", 0.1, "çok partition'lı işlem oranı")
		contention   = flag.Float64("ci", 0.01, "micro çekişme indeksi")
		ollp         = flag.Float64("ollp", 0.1, "bank: deposit_by_name (OLLP) oranı")
		reindex      = flag.Float64("reindex", 0.02, "bank: reindex oranı")
		think        = flag.Int("think-ms", 0, "istemci işlemleri arası bekleme")
	)
	flag.Parse()

	logger, closeLog := newLogger(*logFile, *logLevel)
	defer closeLog()

	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	metrics := lab.NewMetrics(reg)

	repl := calvin.AsyncReplication
	if *mode == "paxos" {
		repl = calvin.PaxosReplication
	}
	var disks [][]*calvin.SimDiskEngine
	cfg := calvin.Config{
		Replicas: *replicas, Partitions: *partitions, Replication: repl,
		EpochDuration: *epoch, NetworkDelay: *netDelay, PrefetchDelay: *prefetch,
		Partitioner: lab.Partitioner, CheckpointDir: *cpDir,
		Observer: metrics, Logger: logger,
	}
	if *diskLatency > 0 {
		disks = make([][]*calvin.SimDiskEngine, *replicas)
		for r := range disks {
			disks[r] = make([]*calvin.SimDiskEngine, *partitions)
			for p := range disks[r] {
				disks[r][p] = calvin.NewSimDiskEngine(*diskLatency)
			}
		}
		cfg.NewEngine = func(r, p int) calvin.Engine { return disks[r][p] }
	}

	micro := &lab.Micro{Partitions: *partitions, Records: *records}
	bank := &lab.Bank{Partitions: *partitions, AccountsPerPart: *accounts,
		CustomersPerPart: *customers, InitialBalance: 1000}
	registry := calvin.NewRegistry()
	micro.Register(registry)
	bank.Register(registry)

	c, err := calvin.NewCluster(cfg, registry)
	if err != nil {
		log.Fatal(err)
	}
	micro.Load(c)
	bank.Load(c)
	reg.MustRegister(lab.NewStatsCollector(c, disks))
	c.Start()

	runner := lab.NewRunner(c, logger, metrics, micro, bank, disks, *replicas)
	if err := runner.Apply(lab.Settings{
		Workload: *workload, Clients: *clients, MultiPartition: *mp, Contention: *contention,
		OLLP: *ollp, Reindex: *reindex, ThinkMillis: *think,
	}); err != nil {
		log.Fatal(err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go every(ctx, *summaryEvery, func() { runner.Summary(*summaryEvery) })
	go every(ctx, *cpEvery, func() {
		if _, _, err := runner.Checkpoint(ctx, *partitions); err != nil && ctx.Err() == nil {
			logger.Error("checkpoint failed", "err", err)
		}
	})
	go every(ctx, *verifyEvery, func() { runner.Verify() })
	if disks != nil {
		rng := rand.New(rand.NewSource(1))
		go every(ctx, time.Second, func() { runner.EvictRandom(rng, *evictPerSec) })
	}

	mux := http.NewServeMux()
	mux.Handle("GET /metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { io.WriteString(w, "ok\n") })
	mux.HandleFunc("GET /api/settings", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, runner.Settings())
	})
	mux.HandleFunc("POST /api/settings", func(w http.ResponseWriter, r *http.Request) {
		s := runner.Settings()
		if err := json.NewDecoder(r.Body).Decode(&s); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		if err := runner.Apply(s); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, s)
	})
	mux.HandleFunc("POST /api/verify", func(w http.ResponseWriter, _ *http.Request) {
		res := runner.Verify()
		status := http.StatusOK
		if !res.OK() {
			status = http.StatusConflict
		}
		writeJSON(w, status, res)
	})
	mux.HandleFunc("POST /api/checkpoint", func(w http.ResponseWriter, r *http.Request) {
		seq, consistent, err := runner.Checkpoint(r.Context(), *partitions)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"seq": seq, "replicas_consistent": consistent})
	})
	srv := &http.Server{Addr: *addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		logger.Info("http listening", "addr", *addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("http server failed", "err", err)
			stop()
		}
	}()

	<-ctx.Done()
	logger.Info("shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	srv.Shutdown(shutdownCtx)
	runner.StopWorkers()
	runner.Verify()
	c.Stop()
}

func newLogger(file, level string) (*slog.Logger, func()) {
	var lvl slog.Level
	if err := lvl.UnmarshalText([]byte(level)); err != nil {
		log.Fatalf("invalid -log-level: %v", err)
	}
	var w io.Writer = os.Stdout
	closeFn := func() {}
	if file != "" {
		if err := os.MkdirAll(filepath.Dir(file), 0o755); err != nil {
			log.Fatal(err)
		}
		f, err := os.OpenFile(file, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
		if err != nil {
			log.Fatal(err)
		}
		w = io.MultiWriter(os.Stdout, f)
		closeFn = func() { f.Close() }
	}
	opts := &slog.HandlerOptions{Level: lvl, ReplaceAttr: func(_ []string, a slog.Attr) slog.Attr {
		// Süreler JSON'da nanosaniye yerine "12.5ms" gibi yazılsın.
		if a.Value.Kind() == slog.KindDuration {
			a.Value = slog.StringValue(a.Value.Duration().String())
		}
		return a
	}}
	return slog.New(slog.NewJSONHandler(w, opts)).With("service", "calvin-lab"), closeFn
}

func every(ctx context.Context, d time.Duration, f func()) {
	if d <= 0 {
		return
	}
	t := time.NewTicker(d)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			f()
		}
	}
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	enc.Encode(v)
}
