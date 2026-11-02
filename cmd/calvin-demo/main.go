// calvin-demo, makalenin §6.2'deki microbenchmark'ını çalıştırır: her işlem
// 10 kayıt okur, bir kısıt kontrolü yapar ve kontrol geçerse her kayıttaki
// sayacı artırır. Kayıtlardan biri küçük bir "hot" kümeden seçilir; çekişme
// indeksi (contention index) = 1 / hot kayıt sayısı. Çok partition'lı
// işlemler iki makineye yayılır ve her birinde bir hot kayda dokunur.
package main

import (
	"context"
	"encoding/binary"
	"flag"
	"fmt"
	"log"
	"maps"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"calvin"
)

func key(part int, kind string, i int) calvin.Key { return fmt.Sprintf("%d:%s-%d", part, kind, i) }

// partitioner "p:..." biçimindeki anahtarları p. partition'a koyar, böylece
// benchmark hangi kaydın hangi makinede olduğunu kontrol edebilir.
func partitioner(k calvin.Key, n int) int {
	p, _ := strconv.Atoi(k[:strings.IndexByte(k, ':')])
	return p % n
}

func main() {
	replicas := flag.Int("replicas", 2, "replika sayısı")
	partitions := flag.Int("partitions", 4, "replika başına partition (makine) sayısı")
	mode := flag.String("mode", "async", "replikasyon modu: async | paxos")
	duration := flag.Duration("duration", 3*time.Second, "benchmark süresi")
	clients := flag.Int("clients", 256, "eşzamanlı istemci sayısı")
	mp := flag.Float64("mp", 0.1, "çok partition'lı işlem oranı")
	ci := flag.Float64("ci", 0.01, "çekişme indeksi (1/hot kayıt sayısı)")
	coldRecords := flag.Int("records", 10000, "partition başına normal kayıt sayısı")
	netDelay := flag.Duration("net", 0, "düğümler arası tek yönlü ağ gecikmesi")
	cpDir := flag.String("checkpoint-dir", "", "checkpoint dosyalarının yazılacağı dizin (boşsa bellekte)")
	flag.Parse()

	repl := calvin.AsyncReplication
	if *mode == "paxos" {
		repl = calvin.PaxosReplication
	}
	hot := int(1 / *ci)
	if hot < 1 {
		hot = 1
	}

	reg := calvin.NewRegistry()
	reg.Register("micro", func(tx *calvin.Tx) error {
		vals := make([]uint64, len(tx.WriteSet()))
		for i, k := range tx.WriteSet() {
			v, _ := tx.Read(k)
			if len(v) == 8 {
				vals[i] = binary.BigEndian.Uint64(v)
			}
		}
		// Kısıt kontrolü: sayaçlar taşmamalı. Geçmezse deterministik abort.
		for _, v := range vals {
			if v == ^uint64(0) {
				return fmt.Errorf("counter overflow")
			}
		}
		for i, k := range tx.WriteSet() {
			tx.Write(k, binary.BigEndian.AppendUint64(nil, vals[i]+1))
		}
		return nil
	})

	c, err := calvin.NewCluster(calvin.Config{
		Replicas:      *replicas,
		Partitions:    *partitions,
		Replication:   repl,
		NetworkDelay:  *netDelay,
		Partitioner:   partitioner,
		CheckpointDir: *cpDir,
	}, reg)
	if err != nil {
		log.Fatal(err)
	}
	c.Start()
	defer c.Stop()

	var committed, aborted atomic.Int64
	var latencySum atomic.Int64
	deadline := time.Now().Add(*duration)
	ctx := context.Background()
	var wg sync.WaitGroup
	for w := 0; w < *clients; w++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(seed))
			for time.Now().Before(deadline) {
				keys := microKeys(rng, *partitions, hot, *coldRecords, *mp)
				start := time.Now()
				r, err := c.Exec(ctx, calvin.Request{Proc: "micro", ReadSet: keys, WriteSet: keys})
				if err != nil {
					return
				}
				latencySum.Add(int64(time.Since(start)))
				if r.Committed {
					committed.Add(1)
				} else {
					aborted.Add(1)
				}
			}
		}(int64(w))
	}

	// Benchmark ortasında yürütmeyi durdurmadan bir checkpoint al.
	time.Sleep(*duration / 2)
	cpStart := time.Now()
	seq, err := c.Checkpoint(ctx)
	if err != nil {
		log.Fatal(err)
	}
	cpTime := time.Since(cpStart)

	wg.Wait()
	c.Drain()

	n := committed.Load() + aborted.Load()
	fmt.Printf("mode=%s replicas=%d partitions=%d contention=%.4f multipartition=%.0f%%\n",
		*mode, *replicas, *partitions, *ci, *mp*100)
	fmt.Printf("committed=%d aborted=%d throughput=%.0f txn/s (%.0f txn/s/partition)\n",
		committed.Load(), aborted.Load(),
		float64(committed.Load())/duration.Seconds(),
		float64(committed.Load())/duration.Seconds()/float64(*partitions))
	if n > 0 {
		fmt.Printf("avg latency=%v\n", time.Duration(latencySum.Load()/n))
	}
	fmt.Printf("checkpoint at seq=%d took %v (no execution stall)\n", seq, cpTime)

	// Her işlem 10 sayacı artırır; replikalar yalnızca girdiyi paylaştığı
	// halde aynı duruma ulaşmış olmalıdır.
	ref := c.Snapshot(0)
	var sum uint64
	for _, v := range ref {
		sum += binary.BigEndian.Uint64(v)
	}
	fmt.Printf("replica 0: sum of counters=%d (expected %d)\n", sum, committed.Load()*10)
	for r := 1; r < *replicas; r++ {
		fmt.Printf("replica %d identical to replica 0: %v\n", r, maps.EqualFunc(ref, c.Snapshot(r), bytesEqual))
	}
}

// microKeys bir işlem için 10 farklı anahtar seçer.
func microKeys(rng *rand.Rand, partitions, hot, records int, mp float64) []calvin.Key {
	p := rng.Intn(partitions)
	parts := []int{p}
	if partitions > 1 && rng.Float64() < mp {
		q := rng.Intn(partitions - 1)
		if q >= p {
			q++
		}
		parts = append(parts, q)
	}
	seen := map[calvin.Key]bool{}
	var keys []calvin.Key
	add := func(k calvin.Key) bool {
		if seen[k] {
			return false
		}
		seen[k] = true
		keys = append(keys, k)
		return true
	}
	for _, q := range parts {
		add(key(q, "hot", rng.Intn(hot)))
	}
	for len(keys) < 10 {
		add(key(parts[rng.Intn(len(parts))], "rec", rng.Intn(records)))
	}
	return keys
}

func bytesEqual(a, b []byte) bool { return string(a) == string(b) }
