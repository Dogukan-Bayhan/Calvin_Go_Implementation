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
}

// Stats tüm düğümlerin anlık durumunu döner.
func (c *Cluster) Stats() []NodeStats {
	var out []NodeStats
	for _, row := range c.nodes {
		for _, n := range row {
			n.seq.logMu.Lock()
			logged := len(n.seq.log)
			n.seq.logMu.Unlock()
			out = append(out, NodeStats{
				Replica:        n.replica,
				Partition:      n.partition,
				SequencedEpoch: n.seq.leader().sequenced.Load(),
				ScheduledEpoch: n.sched.scheduled.Load(),
				Inflight:       n.sched.inflightN.Load(),
				LockedKeys:     n.sched.lockedKeys.Load(),
				LoggedBatches:  logged,
			})
		}
	}
	return out
}
