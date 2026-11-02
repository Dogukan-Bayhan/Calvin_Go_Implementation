// calvin-node tek bir Calvin düğümünü (bir replikanın bir partition'ı) ayrı
// bir süreç olarak çalıştırır. Diğer düğümlerle RPC üzerinden konuşur.
//
// Düğüm kimliği bir sıra numarasından türetilir: index = replica*P + partition.
// Kubernetes StatefulSet içinde sıra numarası pod adından ("calvin-node-5")
// otomatik okunur. Diğer düğümlerin adresleri -peer-template ile üretilir.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	calvin "github.com/Dogukan-Bayhan/Calvin_Go_Implementation"
	"github.com/Dogukan-Bayhan/Calvin_Go_Implementation/internal/lab"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

func main() {
	var (
		replicas     = flag.Int("replicas", 3, "replika sayısı")
		partitions   = flag.Int("partitions", 4, "replika başına partition sayısı")
		mode         = flag.String("mode", "paxos", "replikasyon: async | paxos")
		epoch        = flag.Duration("epoch", 10*time.Millisecond, "sequencer epoch süresi")
		index        = flag.Int("index", -1, "düğüm sıra numarası (replica*partitions+partition); -1: pod adından")
		listen       = flag.String("listen", ":7000", "RPC dinleme adresi")
		metricsAddr  = flag.String("metrics", ":2112", "metrik/sağlık HTTP adresi")
		peerTemplate = flag.String("peer-template", "calvin-node-%d.calvin-node:7000", "düğüm adres şablonu (%d = sıra numarası)")
		peerWait     = flag.Duration("peer-wait", 5*time.Minute, "tüm düğümlerin ayağa kalkması için azami bekleme")
		cpDir        = flag.String("checkpoint-dir", "", "checkpoint dosyaları için dizin (boşsa bellekte)")
		logFile      = flag.String("log-file", "", "JSON logların ayrıca yazılacağı dosya")
		logLevel     = flag.String("log-level", "info", "debug | info | warn | error")
		records      = flag.Int("records", 10000, "micro: partition başına kayıt")
		accounts     = flag.Int("accounts", 1000, "bank: partition başına hesap")
		customers    = flag.Int("customers", 100, "bank: partition başına müşteri")
	)
	flag.Parse()

	i := *index
	if i < 0 {
		var err error
		if i, err = ordinalFromHostname(); err != nil {
			log.Fatalf("-index not set and %v", err)
		}
	}
	total := *replicas * *partitions
	if i >= total {
		log.Fatalf("node index %d out of range: cluster has %d x %d = %d nodes", i, *replicas, *partitions, total)
	}
	self := calvin.NodeID{Replica: i / *partitions, Partition: i % *partitions}

	logger, closeLog := lab.NewLogger(*logFile, *logLevel, "calvin-node")
	defer closeLog()
	logger = logger.With("node", self.String())

	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))

	repl := calvin.AsyncReplication
	if *mode == "paxos" {
		repl = calvin.PaxosReplication
	}
	micro, bank := lab.NewWorkloads(*partitions, *records, *accounts, *customers)
	registry := calvin.NewRegistry()
	micro.Register(registry)
	bank.Register(registry)

	c, err := calvin.NewCluster(calvin.Config{
		Replicas: *replicas, Partitions: *partitions, Replication: repl, EpochDuration: *epoch,
		Partitioner: lab.Partitioner, CheckpointDir: *cpDir,
		Observer: lab.NewNodeMetrics(reg), Logger: logger,
		Hosted: []calvin.NodeID{self},
		PeerAddr: func(id calvin.NodeID) string {
			return fmt.Sprintf(*peerTemplate, id.Replica**partitions+id.Partition)
		},
	}, registry)
	if err != nil {
		log.Fatal(err)
	}
	micro.Load(c)
	bank.Load(c)
	reg.MustRegister(lab.NewStatsCollector(c, nil))

	l, err := net.Listen("tcp", *listen)
	if err != nil {
		log.Fatal(err)
	}
	go func() {
		if err := c.Serve(l); err != nil {
			logger.Error("rpc server failed", "err", err)
			os.Exit(1)
		}
	}()

	var ready atomic.Bool
	srv := &http.Server{Addr: *metricsAddr, Handler: lab.MetricsMux(reg, ready.Load), ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("http server failed", "err", err)
		}
	}()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Tüm düğümler birbirine ulaşabilene kadar bekle; böylece liderler
	// aşağı yukarı aynı anda epoch saymaya başlar.
	logger.Info("waiting for peers", "listen", *listen, "nodes", total)
	wctx, cancel := context.WithTimeout(ctx, *peerWait)
	err = c.WaitPeers(wctx)
	cancel()
	if err != nil {
		log.Fatalf("peers not reachable: %v", err)
	}
	// Tüm liderler epoch'ları aynı başlangıç anından sayar: r0/p0 başlar,
	// diğerleri onun genesis'ini alıp katılır.
	if self == (calvin.NodeID{}) {
		c.Start()
	} else {
		gctx, cancel := context.WithTimeout(ctx, *peerWait)
		genesis, err := c.FetchGenesis(gctx, calvin.NodeID{})
		cancel()
		if err != nil {
			log.Fatalf("genesis: %v", err)
		}
		c.StartAt(genesis)
	}
	ready.Store(true)
	logger.Info("node ready", "genesis", c.Genesis().UTC())

	<-ctx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	srv.Shutdown(shutdownCtx)
	c.Stop()
}

// ordinalFromHostname StatefulSet pod adının ("calvin-node-5") sonundaki sıra
// numarasını okur.
func ordinalFromHostname() (int, error) {
	name := os.Getenv("POD_NAME")
	if name == "" {
		var err error
		if name, err = os.Hostname(); err != nil {
			return 0, err
		}
	}
	i := strings.LastIndexByte(name, '-')
	n, err := strconv.Atoi(name[i+1:])
	if i < 0 || err != nil {
		return 0, fmt.Errorf("cannot parse ordinal from pod name %q", name)
	}
	return n, nil
}
