// calvin-client, dağıtık bir Calvin kümesine (calvin-node süreçleri) RPC ile
// istek gönderen istemcidir.
//
//	calvin-client [bayraklar] run         sürekli yük + periyodik checkpoint/doğrulama + HTTP kontrol API'si
//	calvin-client [bayraklar] bench       -duration boyunca yük, ardından doğrulama ve özet (çıkış kodu: 0 = doğru)
//	calvin-client [bayraklar] stats       düğüm durumlarını JSON olarak yazdırır
//	calvin-client [bayraklar] verify      yük yokken replikaları ve değişmezleri doğrular
//	calvin-client [bayraklar] checkpoint  checkpoint alır ve replikalar arasında karşılaştırır
//	calvin-client [bayraklar] exec PROC [ARGS] -keys k1,k2 [-write k1]   tek işlem çalıştırır
//
// Düğüm adresleri -addrs ile verilir ya da -peer-template ve küme boyutundan
// üretilir. Değişmez kontrolleri için iş yükü parametreleri (-partitions,
// -accounts, ...) düğümlerle aynı olmalıdır.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"calvin"
	"calvin/client"
	"calvin/internal/lab"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

func main() {
	var (
		addrsFlag    = flag.String("addrs", "", "virgülle ayrılmış düğüm adresleri (boşsa -peer-template'ten üretilir)")
		peerTemplate = flag.String("peer-template", "calvin-node-%d.calvin-node:7000", "düğüm adres şablonu")
		replicas     = flag.Int("replicas", 3, "replika sayısı")
		partitions   = flag.Int("partitions", 4, "replika başına partition sayısı")
		addr         = flag.String("addr", ":2113", "run: metrik ve kontrol API'si HTTP adresi")
		readyWait    = flag.Duration("ready-wait", 5*time.Minute, "düğümlerin hazır olması için azami bekleme")
		duration     = flag.Duration("duration", 30*time.Second, "bench: yük süresi")
		cpEvery      = flag.Duration("checkpoint-every", 15*time.Second, "run: checkpoint aralığı (0: kapalı)")
		verifyEvery  = flag.Duration("verify-every", 20*time.Second, "run: doğrulama aralığı (0: kapalı)")
		summaryEvery = flag.Duration("summary-every", 5*time.Second, "yük özeti log aralığı")
		logFile      = flag.String("log-file", "", "JSON logların ayrıca yazılacağı dosya")
		logLevel     = flag.String("log-level", "info", "debug | info | warn | error")
		records      = flag.Int("records", 10000, "micro: partition başına kayıt")
		accounts     = flag.Int("accounts", 1000, "bank: partition başına hesap")
		customers    = flag.Int("customers", 100, "bank: partition başına müşteri")
		workload     = flag.String("workload", "mixed", "micro | bank | mixed")
		clients      = flag.Int("clients", 64, "eşzamanlı istemci")
		mp           = flag.Float64("mp", 0.1, "çok partition'lı işlem oranı")
		contention   = flag.Float64("ci", 0.01, "micro çekişme indeksi")
		ollp         = flag.Float64("ollp", 0.1, "bank: deposit_by_name (OLLP) oranı")
		reindex      = flag.Float64("reindex", 0.02, "bank: reindex oranı")
		think        = flag.Int("think-ms", 0, "istemci işlemleri arası bekleme")
		keys         = flag.String("keys", "", "exec: okuma kümesi (virgülle)")
		writes       = flag.String("write", "", "exec: yazma kümesi (virgülle; boşsa -keys)")
	)
	flag.Parse()
	cmd := flag.Arg(0)
	if cmd == "" {
		cmd = "run"
	}

	var addrs []string
	if *addrsFlag != "" {
		addrs = strings.Split(*addrsFlag, ",")
	} else {
		for i := 0; i < *replicas**partitions; i++ {
			addrs = append(addrs, fmt.Sprintf(*peerTemplate, i))
		}
	}

	logger, closeLog := lab.NewLogger(*logFile, *logLevel, "calvin-client")
	defer closeLog()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cl := client.New(addrs)
	defer cl.Close()
	rctx, cancel := context.WithTimeout(ctx, *readyWait)
	logger.Info("waiting for nodes", "nodes", len(addrs))
	if err := cl.WaitReady(rctx); err != nil {
		log.Fatal(err)
	}
	cancel()

	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	micro, bank := lab.NewWorkloads(*partitions, *records, *accounts, *customers)
	runner := lab.NewRunner(lab.RemoteBackend{Client: cl, Replicas: *replicas},
		logger, lab.NewLabMetrics(reg), micro, bank, nil, *replicas)
	settings := lab.Settings{
		Workload: *workload, Clients: *clients, MultiPartition: *mp, Contention: *contention,
		OLLP: *ollp, Reindex: *reindex, ThinkMillis: *think,
	}
	if cmd == "run" || cmd == "bench" || cmd == "verify" {
		// Küme verisi istemciden uzun yaşar (örn. istemci pod'u yeniden
		// başladığında); değişmezler mevcut durumu başlangıç kabul eder.
		bctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
		if err := runner.Baseline(bctx); err != nil {
			log.Fatalf("baseline: %v", err)
		}
		cancel()
	}

	switch cmd {
	case "run":
		if err := runner.Apply(settings); err != nil {
			log.Fatal(err)
		}
		go lab.Every(ctx, *summaryEvery, func() { runner.Summary(*summaryEvery) })
		go lab.Every(ctx, *cpEvery, func() {
			if _, _, err := runner.Checkpoint(ctx); err != nil && ctx.Err() == nil {
				logger.Error("checkpoint failed", "err", err)
			}
		})
		go lab.Every(ctx, *verifyEvery, func() { runner.Verify() })
		srv := &http.Server{Addr: *addr, Handler: lab.ControlMux(reg, runner), ReadHeaderTimeout: 5 * time.Second}
		go func() {
			logger.Info("http listening", "addr", *addr)
			if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				logger.Error("http server failed", "err", err)
				stop()
			}
		}()
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		srv.Shutdown(shutdownCtx)
		runner.StopWorkers()

	case "bench":
		if err := runner.Apply(settings); err != nil {
			log.Fatal(err)
		}
		start := time.Now()
		go lab.Every(ctx, *summaryEvery, func() { runner.Summary(*summaryEvery) })
		select {
		case <-ctx.Done():
		case <-time.After(*duration):
		}
		runner.StopWorkers()
		elapsed := time.Since(start)
		committed, aborted, restarts, errs := runner.Totals()
		_, cpOK, cpErr := runner.Checkpoint(context.Background())
		res := runner.Verify()
		printJSON(map[string]any{
			"nodes": len(addrs), "replicas": *replicas, "partitions": *partitions,
			"duration": elapsed.String(), "settings": settings,
			"committed": committed, "aborted": aborted, "restarts": restarts, "errors": errs,
			"committed_per_sec":   int(float64(committed) / elapsed.Seconds()),
			"checkpoint_ok":       cpErr == nil && cpOK,
			"verification":        res,
			"verification_passed": res.OK(),
		})
		if !res.OK() || cpErr != nil || !cpOK {
			os.Exit(1)
		}

	case "stats":
		stats, err := cl.Stats(ctx)
		if err != nil {
			log.Fatal(err)
		}
		printJSON(stats)

	case "verify":
		res := runner.Verify()
		printJSON(res)
		if !res.OK() {
			os.Exit(1)
		}

	case "checkpoint":
		seq, ok, err := runner.Checkpoint(ctx)
		if err != nil {
			log.Fatal(err)
		}
		printJSON(map[string]any{"seq": seq, "replicas_consistent": ok})

	case "exec":
		proc := flag.Arg(1)
		if proc == "" || *keys == "" {
			log.Fatal("usage: calvin-client exec PROC [ARGS] -keys k1,k2 [-write k1]")
		}
		req := calvin.Request{Proc: proc, Args: []byte(flag.Arg(2)), ReadSet: strings.Split(*keys, ",")}
		req.WriteSet = req.ReadSet
		if *writes != "" {
			req.WriteSet = strings.Split(*writes, ",")
		}
		res, err := cl.Exec(ctx, req)
		if err != nil {
			log.Fatal(err)
		}
		out := map[string]any{"seq": res.Seq, "committed": res.Committed, "output": string(res.Output)}
		if res.Err != nil {
			out["error"] = res.Err.Error()
		}
		printJSON(out)

	default:
		log.Fatalf("unknown command %q (run | bench | stats | verify | checkpoint | exec)", cmd)
	}
}

func printJSON(v any) {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	enc.Encode(v)
}
