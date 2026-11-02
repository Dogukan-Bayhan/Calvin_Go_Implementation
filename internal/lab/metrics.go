// Package lab, Calvin için uzun süre çalışan bir test ortamıdır: iş yükü
// üreticisi, canlı doğruluk denetleyicisi ve Prometheus metrikleri.
package lab

import (
	"errors"
	"strconv"
	"time"

	"calvin"

	"github.com/prometheus/client_golang/prometheus"
)

// latencyBuckets 0.25 ms ile ~4 s arasını kapsar.
var latencyBuckets = prometheus.ExponentialBuckets(0.00025, 2, 15)

// Metrics, calvin.Observer'ı Prometheus'a bağlar.
type Metrics struct {
	txnTotal        *prometheus.CounterVec
	txnLatency      *prometheus.HistogramVec
	lockWait        *prometheus.HistogramVec
	remoteReadWait  prometheus.Histogram
	footprint       *prometheus.HistogramVec
	batchTxns       prometheus.Histogram
	paxosRound      prometheus.Histogram
	paxosRounds     *prometheus.CounterVec
	checkpointDur   prometheus.Histogram
	checkpointRecs  *prometheus.GaugeVec
	checkpointTotal *prometheus.CounterVec

	VerifyRuns           *prometheus.CounterVec
	VerifyPause          prometheus.Histogram
	ReplicasConsistent   prometheus.Gauge
	InvariantOK          prometheus.Gauge
	CheckpointConsistent prometheus.Gauge
	LastVerify           prometheus.Gauge
	Clients              prometheus.Gauge
	SettingsInfo         *prometheus.GaugeVec
}

func NewMetrics(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		txnTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "calvin_txn_total", Help: "Tamamlanan işlemler, sonuca göre.",
		}, []string{"proc", "result", "multipartition"}),
		txnLatency: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "calvin_txn_latency_seconds", Help: "Gönderimden ilk replikada tamamlanmaya kadar geçen süre.",
			Buckets: latencyBuckets,
		}, []string{"proc", "multipartition"}),
		lockWait: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "calvin_lock_wait_seconds", Help: "Kilit isteğinden tüm kilitlerin alınmasına kadar geçen süre.",
			Buckets: latencyBuckets,
		}, []string{"role"}),
		remoteReadWait: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name: "calvin_remote_read_wait_seconds", Help: "Aktif katılımcının uzak okuma sonuçlarını bekleme süresi (aşama 4).",
			Buckets: latencyBuckets,
		}),
		footprint: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "calvin_contention_footprint_seconds", Help: "Kilitlerin tutulduğu süre (contention footprint).",
			Buckets: latencyBuckets,
		}, []string{"role"}),
		batchTxns: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name: "calvin_batch_txns", Help: "Sequencer epoch batch boyutu.",
			Buckets: []float64{0, 1, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000},
		}),
		paxosRound: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name: "calvin_paxos_round_seconds", Help: "Bir epoch için Paxos faz 2 süresi.",
			Buckets: latencyBuckets,
		}),
		paxosRounds: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "calvin_paxos_rounds_total", Help: "Paxos turları, sonuca göre.",
		}, []string{"result"}),
		checkpointDur: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name: "calvin_checkpoint_duration_seconds", Help: "Düğüm başına checkpoint yazma süresi.",
			Buckets: prometheus.ExponentialBuckets(0.001, 2, 14),
		}),
		checkpointRecs: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "calvin_checkpoint_records", Help: "Son checkpoint'teki kayıt sayısı.",
		}, []string{"replica", "partition"}),
		checkpointTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "calvin_checkpoints_total", Help: "Düğüm checkpoint'leri, sonuca göre.",
		}, []string{"result"}),

		VerifyRuns: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "calvin_lab_verify_runs_total", Help: "Canlı doğrulama çalıştırmaları.",
		}, []string{"result"}),
		VerifyPause: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name: "calvin_lab_verify_pause_seconds", Help: "Doğrulama için yükün durdurulduğu süre.",
			Buckets: prometheus.ExponentialBuckets(0.001, 2, 14),
		}),
		ReplicasConsistent: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "calvin_lab_replicas_consistent", Help: "1: son doğrulamada tüm replikalar birebir aynıydı.",
		}),
		InvariantOK: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "calvin_lab_invariant_ok", Help: "1: son doğrulamada iş yükü değişmezleri sağlandı.",
		}),
		CheckpointConsistent: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "calvin_lab_checkpoint_consistent", Help: "1: son checkpoint tüm replikalarda aynıydı.",
		}),
		LastVerify: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "calvin_lab_last_verify_timestamp_seconds", Help: "Son doğrulamanın zamanı.",
		}),
		Clients: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "calvin_lab_clients", Help: "Eşzamanlı istemci sayısı.",
		}),
		SettingsInfo: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "calvin_lab_settings_info", Help: "Aktif iş yükü ayarları.",
		}, []string{"workload", "multipartition", "contention"}),
	}
	reg.MustRegister(m.txnTotal, m.txnLatency, m.lockWait, m.remoteReadWait, m.footprint,
		m.batchTxns, m.paxosRound, m.paxosRounds, m.checkpointDur, m.checkpointRecs,
		m.checkpointTotal, m.VerifyRuns, m.VerifyPause, m.ReplicasConsistent, m.InvariantOK,
		m.CheckpointConsistent, m.LastVerify, m.Clients, m.SettingsInfo)
	return m
}

func role(active bool) string {
	if active {
		return "active"
	}
	return "passive"
}

func (m *Metrics) BatchSequenced(_ int, _ uint64, txns int) { m.batchTxns.Observe(float64(txns)) }

func (m *Metrics) PaxosRound(_ int, d time.Duration, ok bool) {
	m.paxosRound.Observe(d.Seconds())
	if ok {
		m.paxosRounds.WithLabelValues("chosen").Inc()
	} else {
		m.paxosRounds.WithLabelValues("rejected").Inc()
	}
}

func (m *Metrics) TxnReleased(ev calvin.NodeTxnEvent) {
	r := role(ev.Active)
	m.lockWait.WithLabelValues(r).Observe(ev.LockWait.Seconds())
	m.footprint.WithLabelValues(r).Observe(ev.Footprint.Seconds())
	if ev.Active {
		m.remoteReadWait.Observe(ev.RemoteReadWait.Seconds())
	}
}

func (m *Metrics) TxnCompleted(ev calvin.TxnEvent) {
	mp := strconv.FormatBool(ev.Partitions > 1)
	result := "committed"
	switch {
	case ev.Committed:
	case errors.Is(ev.Err, calvin.ErrRestart):
		result = "restart"
	case errors.Is(ev.Err, calvin.ErrAborted):
		result = "aborted"
	default:
		result = "error"
	}
	m.txnTotal.WithLabelValues(ev.Proc, result, mp).Inc()
	m.txnLatency.WithLabelValues(ev.Proc, mp).Observe(ev.Latency.Seconds())
}

func (m *Metrics) CheckpointDone(ev calvin.CheckpointEvent) {
	m.checkpointDur.Observe(ev.Duration.Seconds())
	m.checkpointRecs.WithLabelValues(strconv.Itoa(ev.Replica), strconv.Itoa(ev.Partition)).Set(float64(ev.Records))
	if ev.Err != nil {
		m.checkpointTotal.WithLabelValues("error").Inc()
	} else {
		m.checkpointTotal.WithLabelValues("ok").Inc()
	}
}

// StatsCollector, Cluster.Stats() ve disk motoru sayaçlarını her scrape'te
// okuyup yayınlar.
type StatsCollector struct {
	c     *calvin.Cluster
	disks [][]*calvin.SimDiskEngine

	seqEpoch, schedEpoch, lag, inflight, locked, logBatches, stalls *prometheus.Desc
}

func NewStatsCollector(c *calvin.Cluster, disks [][]*calvin.SimDiskEngine) *StatsCollector {
	node := []string{"replica", "partition"}
	return &StatsCollector{
		c: c, disks: disks,
		seqEpoch:   prometheus.NewDesc("calvin_sequencer_epoch", "Lider sequencer'ın kapattığı son epoch.", []string{"partition"}, nil),
		schedEpoch: prometheus.NewDesc("calvin_scheduler_epoch", "Scheduler'ın kilitlerini istediği son epoch.", node, nil),
		lag:        prometheus.NewDesc("calvin_replica_lag_epochs", "Sequencer epoch'u ile scheduler epoch'u arasındaki fark.", node, nil),
		inflight:   prometheus.NewDesc("calvin_scheduler_inflight", "Kilit kuyruğunda olup bitmemiş işlemler.", node, nil),
		locked:     prometheus.NewDesc("calvin_locked_keys", "Kilit tablosundaki anahtar sayısı.", node, nil),
		logBatches: prometheus.NewDesc("calvin_input_log_batches", "Girdi logunda tutulan batch sayısı.", node, nil),
		stalls:     prometheus.NewDesc("calvin_disk_stalls_total", "Yürütme sırasında soğuk veriye takılan okumalar.", node, nil),
	}
}

func (s *StatsCollector) Describe(ch chan<- *prometheus.Desc) { prometheus.DescribeByCollect(s, ch) }

func (s *StatsCollector) Collect(ch chan<- prometheus.Metric) {
	gauge := func(d *prometheus.Desc, v float64, labels ...string) {
		ch <- prometheus.MustNewConstMetric(d, prometheus.GaugeValue, v, labels...)
	}
	for _, st := range s.c.Stats() {
		r, p := strconv.Itoa(st.Replica), strconv.Itoa(st.Partition)
		if st.Replica == 0 {
			gauge(s.seqEpoch, float64(st.SequencedEpoch), p)
		}
		lag := float64(st.SequencedEpoch) - float64(st.ScheduledEpoch)
		gauge(s.schedEpoch, float64(st.ScheduledEpoch), r, p)
		gauge(s.lag, max(lag, 0), r, p)
		gauge(s.inflight, float64(st.Inflight), r, p)
		gauge(s.locked, float64(st.LockedKeys), r, p)
		gauge(s.logBatches, float64(st.LoggedBatches), r, p)
		if s.disks != nil {
			ch <- prometheus.MustNewConstMetric(s.stalls, prometheus.CounterValue,
				float64(s.disks[st.Replica][st.Partition].Stalls()), r, p)
		}
	}
}
