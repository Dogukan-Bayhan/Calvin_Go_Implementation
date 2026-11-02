package lab

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"math/rand"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	calvin "github.com/Dogukan-Bayhan/Calvin_Go_Implementation"
)

// Settings çalışma anında HTTP API ile değiştirilebilen yük ayarlarıdır.
type Settings struct {
	Workload       string  `json:"workload"` // micro | bank | mixed
	Clients        int     `json:"clients"`
	MultiPartition float64 `json:"multipartition"`
	Contention     float64 `json:"contention"`
	OLLP           float64 `json:"ollp"`
	Reindex        float64 `json:"reindex"`
	ThinkMillis    int     `json:"think_ms"`
}

func (s Settings) Validate() error {
	switch {
	case s.Workload != "micro" && s.Workload != "bank" && s.Workload != "mixed":
		return fmt.Errorf("workload must be micro, bank or mixed")
	case s.Clients < 0 || s.Clients > 10000:
		return fmt.Errorf("clients must be in [0, 10000]")
	case s.MultiPartition < 0 || s.MultiPartition > 1:
		return fmt.Errorf("multipartition must be in [0, 1]")
	case s.Contention <= 0 || s.Contention > 1:
		return fmt.Errorf("contention must be in (0, 1]")
	case s.OLLP < 0 || s.Reindex < 0 || s.OLLP+s.Reindex > 1:
		return fmt.Errorf("ollp + reindex must be in [0, 1]")
	case s.ThinkMillis < 0:
		return fmt.Errorf("think_ms must be >= 0")
	}
	return nil
}

// Runner yük üreticisi ve canlı doğrulayıcıdır.
type Runner struct {
	be    Backend
	log   *slog.Logger
	m     *LabMetrics
	micro *Micro
	bank  *Bank
	disks [][]*calvin.SimDiskEngine
	reps  int

	settings atomic.Pointer[Settings]
	// gate: işçiler her işlem için RLock alır; doğrulayıcı Lock alarak yükü
	// durdurur, tüm replikaları boşaltır ve tutarlı bir anda denetim yapar.
	gate sync.RWMutex

	mu      sync.Mutex
	workers []chan struct{}
	wg      sync.WaitGroup

	committed, aborted, restarts, errs atomic.Int64
	latencyNanos                       atomic.Int64
	// totals: başlangıçtan beri commit, abort, restart, hata (Summary sıfırlamaz).
	totals [4]atomic.Int64
}

// NewRunner bir yük üreticisi oluşturur. disks yalnızca tek süreçli
// SimDiskEngine kullanımında doludur (EvictRandom için).
func NewRunner(be Backend, log *slog.Logger, m *LabMetrics, micro *Micro, bank *Bank,
	disks [][]*calvin.SimDiskEngine, replicas int) *Runner {
	return &Runner{be: be, log: log, m: m, micro: micro, bank: bank, disks: disks, reps: replicas}
}

func (r *Runner) Settings() Settings { return *r.settings.Load() }

// Apply yeni ayarları uygular ve istemci sayısını ölçekler.
func (r *Runner) Apply(s Settings) error {
	if err := s.Validate(); err != nil {
		return err
	}
	r.settings.Store(&s)
	r.mu.Lock()
	defer r.mu.Unlock()
	for len(r.workers) < s.Clients {
		stop := make(chan struct{})
		r.workers = append(r.workers, stop)
		r.wg.Add(1)
		go r.worker(int64(len(r.workers)), stop)
	}
	for len(r.workers) > s.Clients {
		last := len(r.workers) - 1
		close(r.workers[last])
		r.workers = r.workers[:last]
	}
	r.m.Clients.Set(float64(s.Clients))
	r.m.SettingsInfo.Reset()
	r.m.SettingsInfo.WithLabelValues(s.Workload, strconv.FormatFloat(s.MultiPartition, 'f', -1, 64),
		strconv.FormatFloat(s.Contention, 'f', -1, 64)).Set(1)
	r.log.Info("workload settings applied", "workload", s.Workload, "clients", s.Clients,
		"multipartition", s.MultiPartition, "contention", s.Contention, "ollp", s.OLLP,
		"reindex", s.Reindex, "think_ms", s.ThinkMillis)
	return nil
}

func (r *Runner) worker(seed int64, stop chan struct{}) {
	defer r.wg.Done()
	rng := rand.New(rand.NewSource(seed*7919 + time.Now().UnixNano()))
	ctx := context.Background()
	for {
		select {
		case <-stop:
			return
		default:
		}
		s := r.Settings()
		var w Workload = r.micro
		if s.Workload == "bank" || (s.Workload == "mixed" && rng.Intn(2) == 0) {
			w = r.bank
		}
		start := time.Now()
		r.gate.RLock()
		res, err := w.Step(ctx, r.be, rng, s)
		r.gate.RUnlock()
		lat := time.Since(start)
		r.latencyNanos.Add(int64(lat))
		r.m.ClientLatency.WithLabelValues(w.Name()).Observe(lat.Seconds())
		switch {
		case errors.Is(err, calvin.ErrStopped):
			return
		case err != nil:
			r.errs.Add(1)
			r.totals[3].Add(1)
			r.log.Warn("operation failed", "workload", w.Name(), "err", err)
		case res.Committed:
			r.committed.Add(1)
			r.totals[0].Add(1)
		case errors.Is(res.Err, calvin.ErrRestart):
			r.restarts.Add(1)
			r.totals[2].Add(1)
		default:
			r.aborted.Add(1)
			r.totals[1].Add(1)
		}
		if s.ThinkMillis > 0 {
			select {
			case <-stop:
				return
			case <-time.After(time.Duration(s.ThinkMillis) * time.Millisecond):
			}
		}
	}
}

// StopWorkers tüm istemcileri durdurur.
func (r *Runner) StopWorkers() {
	r.mu.Lock()
	for _, s := range r.workers {
		close(s)
	}
	r.workers = nil
	r.mu.Unlock()
	r.wg.Wait()
}

// VerifyResult bir doğrulama turunun sonucudur.
type VerifyResult struct {
	At                 time.Time `json:"at"`
	Pause              string    `json:"pause"`
	ReplicasConsistent bool      `json:"replicas_consistent"`
	InvariantErrors    []string  `json:"invariant_errors,omitempty"`
	DivergentKeys      []string  `json:"divergent_keys,omitempty"`
	Records            int       `json:"records"`
}

func (v VerifyResult) OK() bool { return v.ReplicasConsistent && len(v.InvariantErrors) == 0 }

// Verify yükü durdurur, tüm replikaların tüm işlemleri bitirmesini bekler,
// replikaları birebir karşılaştırır ve iş yükü değişmezlerini denetler.
func (r *Runner) Verify() VerifyResult {
	start := time.Now()
	r.gate.Lock()
	defer r.gate.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	res := VerifyResult{At: start, ReplicasConsistent: true}
	states, err := r.quiesceAndRead(ctx)
	if err != nil {
		res.ReplicasConsistent = false
		res.InvariantErrors = append(res.InvariantErrors, "verification could not run: "+err.Error())
	} else {
		ref := states[0]
		res.Records = len(ref)
		for rep := 1; rep < len(states); rep++ {
			if !maps.EqualFunc(ref, states[rep], bytesEqual) {
				res.ReplicasConsistent = false
				res.DivergentKeys = append(res.DivergentKeys, diffKeys(ref, states[rep], rep, 5)...)
			}
		}
		for _, w := range []Workload{r.micro, r.bank} {
			if err := w.Check(ref); err != nil {
				res.InvariantErrors = append(res.InvariantErrors, err.Error())
			}
		}
	}
	pause := time.Since(start)
	res.Pause = pause.String()

	r.m.VerifyPause.Observe(pause.Seconds())
	r.m.ReplicasConsistent.Set(b2f(res.ReplicasConsistent))
	r.m.InvariantOK.Set(b2f(len(res.InvariantErrors) == 0))
	r.m.LastVerify.Set(float64(time.Now().Unix()))
	if res.OK() {
		r.m.VerifyRuns.WithLabelValues("ok").Inc()
		r.log.Info("verification passed", "records", res.Records, "pause", pause)
	} else {
		r.m.VerifyRuns.WithLabelValues("fail").Inc()
		r.log.Error("verification FAILED", "replicas_consistent", res.ReplicasConsistent,
			"invariant_errors", res.InvariantErrors, "divergent_keys", res.DivergentKeys)
	}
	return res
}

// Baseline, yük başlamadan önce kümenin mevcut durumunu iş yükü
// değişmezlerinin başlangıç noktası olarak kaydeder. Kümede başka bir yük
// üreticisi çalışmıyor olmalıdır.
func (r *Runner) Baseline(ctx context.Context) error {
	r.gate.Lock()
	defer r.gate.Unlock()
	states, err := r.quiesceAndRead(ctx)
	if err != nil {
		return err
	}
	r.micro.Baseline(states[0])
	r.bank.Baseline(states[0])
	r.log.Info("invariant baseline recorded", "records", len(states[0]))
	return nil
}

func (r *Runner) quiesceAndRead(ctx context.Context) ([]map[calvin.Key][]byte, error) {
	if err := r.be.Quiesce(ctx); err != nil {
		return nil, err
	}
	return r.be.ReplicaStates(ctx)
}

// Checkpoint yürütmeyi durdurmadan bir checkpoint alır ve tüm replikaların
// aynı sanal tutarlılık noktasında aynı görüntüyü kaydettiğini doğrular.
func (r *Runner) Checkpoint(ctx context.Context) (uint64, bool, error) {
	// Doğrulayıcı Quiesce içindeyken yeni işlem eklenmemesi için kapıdan geç.
	r.gate.RLock()
	defer r.gate.RUnlock()
	seq, err := r.be.Checkpoint(ctx)
	if err != nil {
		return seq, false, err
	}
	snaps, err := r.be.CheckpointStates(ctx)
	if err != nil {
		return seq, false, err
	}
	ok := true
	for rep := 1; rep < len(snaps); rep++ {
		ok = ok && maps.EqualFunc(snaps[0], snaps[rep], bytesEqual)
	}
	r.m.CheckpointConsistent.Set(b2f(ok))
	if !ok {
		r.log.Error("checkpoint replicas differ", "seq", seq)
	}
	return seq, ok, nil
}

// EvictRandom, disk modunda rastgele kayıtları soğuğa taşıyarak prefetch
// mekanizmasını sürekli çalıştırır.
func (r *Runner) EvictRandom(rng *rand.Rand, n int) {
	if r.disks == nil {
		return
	}
	for i := 0; i < n; i++ {
		var k calvin.Key
		if rng.Intn(2) == 0 {
			k = r.micro.SampleKey(rng)
		} else {
			k = r.bank.SampleKey(rng)
		}
		p := Partitioner(k, len(r.disks[0]))
		for rep := range r.disks {
			r.disks[rep][p].Evict(k)
		}
	}
}

// Summary son çağrıdan bu yana geçen süredeki yük özetini loglar.
func (r *Runner) Summary(interval time.Duration) {
	c, a, rs, e := r.committed.Swap(0), r.aborted.Swap(0), r.restarts.Swap(0), r.errs.Swap(0)
	lat := r.latencyNanos.Swap(0)
	n := c + a + rs + e
	avg := time.Duration(0)
	if n > 0 {
		avg = time.Duration(lat / n)
	}
	sec := interval.Seconds()
	r.log.Info("load summary",
		"committed_per_s", int(float64(c)/sec), "aborted_per_s", int(float64(a)/sec),
		"restarts_per_s", int(float64(rs)/sec), "errors", e, "avg_latency", avg,
		"clients", r.Settings().Clients)
}

func bytesEqual(a, b []byte) bool { return string(a) == string(b) }

func b2f(b bool) float64 {
	if b {
		return 1
	}
	return 0
}

func diffKeys(a, b map[calvin.Key][]byte, rep, limit int) []string {
	var out []string
	for k, v := range a {
		if w, ok := b[k]; !ok || string(v) != string(w) {
			out = append(out, fmt.Sprintf("replica %d: %s", rep, k))
			if len(out) >= limit {
				return out
			}
		}
	}
	for k := range b {
		if _, ok := a[k]; !ok && len(out) < limit {
			out = append(out, fmt.Sprintf("replica %d: extra %s", rep, k))
		}
	}
	return out
}

// Totals başlangıçtan beri commit, abort, restart ve hata sayılarını döner.
func (r *Runner) Totals() (committed, aborted, restarts, errs int64) {
	return r.totals[0].Load(), r.totals[1].Load(), r.totals[2].Load(), r.totals[3].Load()
}
