// calvin-lab, Calvin için tek süreçte çalışan bir test ortamıdır. Tüm
// düğümleri aynı süreçte (simüle ağ gecikmesiyle) çalıştırır, üzerinde
// ayarlanabilir iş yükü üretir, düzenli olarak checkpoint alır, yükü kısa süre
// durdurup replikaların birebir aynı olduğunu ve iş yükü değişmezlerini
// doğrular. Metrikleri /metrics'te, logları JSON olarak stdout'a ve isteğe
// bağlı bir dosyaya yazar. Kontrol API'si için bkz. lab.ControlMux.
//
// Gerçek ağ üzerinde, her düğümü ayrı süreçte çalıştırmak için
// cmd/calvin-node ve cmd/calvin-client kullanılır.
package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	calvin "github.com/Dogukan-Bayhan/Calvin_Go_Implementation"
	"github.com/Dogukan-Bayhan/Calvin_Go_Implementation/internal/lab"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

func main() {
	var (
		addr         = flag.String("addr", ":2112", "HTTP dinleme adresi")
		replicas     = flag.Int("replicas", 3, "replika sayısı")
		partitions   = flag.Int("partitions", 4, "replika başına partition sayısı")
		mode         = flag.String("mode", "paxos", "replikasyon: async | paxos")
		epoch        = flag.Duration("epoch", 10*time.Millisecond, "sequencer epoch süresi")
		netDelay     = flag.Duration("net", time.Millisecond, "düğümler arası simüle tek yönlü ağ gecikmesi")
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

	logger, closeLog := lab.NewLogger(*logFile, *logLevel, "calvin-lab")
	defer closeLog()

	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	nodeMetrics := lab.NewNodeMetrics(reg)
	labMetrics := lab.NewLabMetrics(reg)

	repl := calvin.AsyncReplication
	if *mode == "paxos" {
		repl = calvin.PaxosReplication
	}
	var disks [][]*calvin.SimDiskEngine
	cfg := calvin.Config{
		Replicas: *replicas, Partitions: *partitions, Replication: repl,
		EpochDuration: *epoch, NetworkDelay: *netDelay, PrefetchDelay: *prefetch,
		Partitioner: lab.Partitioner, CheckpointDir: *cpDir,
		Observer: nodeMetrics, Logger: logger,
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

	micro, bank := lab.NewWorkloads(*partitions, *records, *accounts, *customers)
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

	runner := lab.NewRunner(lab.LocalBackend{Cluster: c, Replicas: *replicas, Partitions: *partitions},
		logger, labMetrics, micro, bank, disks, *replicas)
	if err := runner.Apply(lab.Settings{
		Workload: *workload, Clients: *clients, MultiPartition: *mp, Contention: *contention,
		OLLP: *ollp, Reindex: *reindex, ThinkMillis: *think,
	}); err != nil {
		log.Fatal(err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go lab.Every(ctx, *summaryEvery, func() { runner.Summary(*summaryEvery) })
	go lab.Every(ctx, *cpEvery, func() {
		if _, _, err := runner.Checkpoint(ctx); err != nil && ctx.Err() == nil {
			logger.Error("checkpoint failed", "err", err)
		}
	})
	go lab.Every(ctx, *verifyEvery, func() { runner.Verify() })
	if disks != nil {
		rng := rand.New(rand.NewSource(1))
		go lab.Every(ctx, time.Second, func() { runner.EvictRandom(rng, *evictPerSec) })
	}

	srv := &http.Server{Addr: *addr, Handler: lab.ControlMux(reg, runner), ReadHeaderTimeout: 5 * time.Second}
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
