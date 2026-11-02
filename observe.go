package calvin

import "time"

// Observer, çekirdek kütüphaneyi bir metrik sistemine bağlamadan ölçüm
// almak için kullanılan kancalardır. Metotlar sıcak yoldan (kilit yöneticisi
// goroutine'i dahil) çağrılır; hızlı olmalı ve bloklamamalıdır.
type Observer interface {
	// BatchSequenced lider sequencer bir epoch'u kapattığında çağrılır.
	BatchSequenced(partition int, epoch uint64, txns int)
	// PaxosRound bir epoch'un Paxos faz 2 turu bittiğinde çağrılır.
	PaxosRound(partition int, d time.Duration, ok bool)
	// TxnReleased, bir işlem bir düğümde kilitlerini bıraktığında çağrılır.
	TxnReleased(ev NodeTxnEvent)
	// TxnCompleted, işlemin sonucu istemciye ilk kez verildiğinde çağrılır.
	TxnCompleted(ev TxnEvent)
	// CheckpointDone bir düğüm checkpoint'ini yazdığında çağrılır.
	CheckpointDone(ev CheckpointEvent)
}

// NodeTxnEvent bir işlemin tek bir düğümdeki yaşam döngüsüdür.
type NodeTxnEvent struct {
	Replica, Partition int
	// Active, düğümün işlem mantığını yürüttüğünü (yazma kümesinde yerel
	// anahtar olduğunu) belirtir; pasif katılımcılar yalnızca okuma servis eder.
	Active bool
	// LockWait: kilit isteğinden tüm kilitlerin alınmasına kadar geçen süre.
	LockWait time.Duration
	// RemoteReadWait: 4. aşamada uzak okuma sonuçlarını bekleme süresi.
	RemoteReadWait time.Duration
	// Footprint: kilitlerin tutulduğu süre, yani makaledeki contention
	// footprint. Calvin'in amacı bunu 2PC'siz kısa tutmaktır.
	Footprint time.Duration
}

// TxnEvent bir işlemin istemci açısından sonucudur.
type TxnEvent struct {
	Seq        uint64
	Proc       string
	Partitions int
	Dependent  bool
	Committed  bool
	Err        error
	// Latency: gönderimden ilk replikada tamamlanmaya kadar geçen süre.
	Latency time.Duration
}

// CheckpointEvent bir düğümün checkpoint sonucudur.
type CheckpointEvent struct {
	Replica, Partition int
	Seq                uint64
	Records            int
	Duration           time.Duration
	Err                error
}

// NopObserver hiçbir şey yapmaz; kısmi Observer implementasyonlarına
// gömülmek için kullanılabilir.
type NopObserver struct{}

func (NopObserver) BatchSequenced(int, uint64, int)     {}
func (NopObserver) PaxosRound(int, time.Duration, bool) {}
func (NopObserver) TxnReleased(NodeTxnEvent)            {}
func (NopObserver) TxnCompleted(TxnEvent)               {}
func (NopObserver) CheckpointDone(CheckpointEvent)      {}

// NodeStats bir düğümün anlık durumudur.
type NodeStats struct {
	Replica, Partition int
	// SequencedEpoch, bu partition'ın lider sequencer'ının kapattığı son epoch.
	SequencedEpoch uint64
	// ScheduledEpoch, bu düğümün scheduler'ının kilit istemeyi bitirdiği son
	// epoch. SequencedEpoch - ScheduledEpoch replikanın gecikmesini gösterir.
	ScheduledEpoch uint64
	// Inflight: kilit kuyruğuna alınmış ama henüz bitmemiş işlem sayısı.
	Inflight int64
	// LockedKeys: kilit tablosundaki anahtar sayısı.
	LockedKeys int64
	// LoggedBatches: girdi logunda tutulan batch sayısı (checkpoint'te kırpılır).
	LoggedBatches int
	// SequencerQueue: (yalnızca lider düğümlerde) henüz batch'e girmemiş işlemler.
	SequencerQueue int64
	// ScheduleDelay: son epoch'un liderde kesilmesinden bu düğümde kilit
	// istenmesine kadar geçen süre (replikasyon + ağ + kuyruk gecikmesi).
	// Farklı makinelerin saatlerinin senkron olduğunu varsayar.
	ScheduleDelay time.Duration
}

// Stats bu süreçteki düğümlerin anlık durumunu döner. SequencedEpoch yalnızca
// grup lideri aynı süreçteyse doludur.
func (c *Cluster) Stats() []NodeStats {
	var out []NodeStats
	for _, n := range c.hosted {
		n.seq.logMu.Lock()
		logged := len(n.seq.log)
		n.seq.logMu.Unlock()
		st := NodeStats{
			Replica:        n.replica,
			Partition:      n.partition,
			ScheduledEpoch: n.sched.scheduled.Load(),
			Inflight:       n.sched.inflightN.Load(),
			LockedKeys:     n.sched.lockedKeys.Load(),
			LoggedBatches:  logged,
			ScheduleDelay:  time.Duration(n.sched.scheduleDelay.Load()),
		}
		if l := c.local(NodeID{0, n.partition}); l != nil {
			st.SequencedEpoch = l.seq.sequenced.Load()
		}
		if n.replica == 0 {
			st.SequencerQueue = int64(len(n.seq.in)) + n.seq.queued.Load()
		}
		out = append(out, st)
	}
	return out
}
