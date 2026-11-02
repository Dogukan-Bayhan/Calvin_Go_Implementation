package calvin

import (
	"sort"
	"sync/atomic"
	"time"
)

type lockReq struct {
	ts      *txnState
	write   bool
	granted bool
}

type txnState struct {
	txn     *Txn
	waiting int
	keys    []Key

	admitted, granted time.Time
	// worker'ın doldurduğu alanlar; releaseCh üzerinden scheduler'a geçer.
	active     bool
	remoteWait time.Duration
}

type cpWait struct {
	marker    *Txn
	remaining int
}

// scheduler, §3.2'deki deterministik kilit yöneticisidir. Tüm kilit istekleri
// ve bırakmaları tek bir goroutine'de işlenir ve iki değişmez korunur:
//  1. Global sırada A, B'den önceyse ve ikisi de yerel R kaydını istiyorsa,
//     A kilidi B'den önce ister (işlemler sırayla, tüm kilitleriyle birlikte
//     kabul edilir).
//  2. Bir anahtarın kilidi istek sırasına göre (FIFO) verilir.
//
// Bu iki kural, eşzamanlı yürütmeye rağmen her replikanın aynı seri sıraya
// denk davranmasını garanti eder ve dağıtık deadlock'u imkânsız kılar.
type scheduler struct {
	n         *node
	batchIn   chan *batch
	releaseCh chan *txnState

	pending   map[uint64][]*batch
	nextEpoch uint64
	locks     map[Key][]*lockReq
	inflight  int
	cp        *cpWait

	// Stats() için başka goroutine'lerden okunan kopyalar.
	scheduled  atomic.Uint64
	inflightN  atomic.Int64
	lockedKeys atomic.Int64
}

func newScheduler(n *node) *scheduler {
	return &scheduler{
		n:         n,
		batchIn:   make(chan *batch, 1024),
		releaseCh: make(chan *txnState, 1024),
		pending:   make(map[uint64][]*batch),
		nextEpoch: 1,
		locks:     make(map[Key][]*lockReq),
	}
}

func (s *scheduler) run() {
	c := s.n.c
	defer c.wg.Done()
	for {
		select {
		case <-c.stop:
			return
		case b := <-s.batchIn:
			s.pending[b.epoch] = append(s.pending[b.epoch], b)
			s.drain()
		case ts := <-s.releaseCh:
			s.release(ts)
		}
	}
}

// drain, tüm sequencer'lardan batch'i gelmiş epoch'ları sırayla işler.
func (s *scheduler) drain() {
	for {
		bs := s.pending[s.nextEpoch]
		if len(bs) < s.n.c.cfg.Partitions {
			return
		}
		delete(s.pending, s.nextEpoch)
		sort.Slice(bs, func(i, j int) bool { return bs[i].from < bs[j].from })
		for _, b := range bs {
			for _, t := range b.txns {
				s.admit(t)
			}
		}
		s.scheduled.Store(s.nextEpoch)
		s.nextEpoch++
		s.inflightN.Store(int64(s.inflight))
		s.lockedKeys.Store(int64(len(s.locks)))
	}
}

func (s *scheduler) admit(t *Txn) {
	if t.kind == kindCheckpoint {
		s.beginCheckpoint(t)
		return
	}
	c := s.n.c
	modes := make(map[Key]bool)
	for _, k := range t.ReadSet {
		if c.partitionOf(k) == s.n.partition {
			if _, ok := modes[k]; !ok {
				modes[k] = false
			}
		}
	}
	for _, k := range t.WriteSet {
		if c.partitionOf(k) == s.n.partition {
			modes[k] = true
		}
	}
	ts := &txnState{txn: t, admitted: time.Now()}
	s.inflight++
	for k, w := range modes {
		q := s.locks[k]
		req := &lockReq{ts: ts, write: w}
		if canGrant(q, w) {
			req.granted = true
		} else {
			ts.waiting++
		}
		s.locks[k] = append(q, req)
		ts.keys = append(ts.keys, k)
	}
	if ts.waiting == 0 {
		ts.granted = ts.admitted
		go s.n.execute(ts)
	}
}

// canGrant: kuyruk boşsa her istek, değilse yalnızca önündekilerin hepsi
// okuma olan bir okuma isteği hemen verilebilir.
func canGrant(q []*lockReq, write bool) bool {
	if len(q) == 0 {
		return true
	}
	if write {
		return false
	}
	for _, r := range q {
		if r.write {
			return false
		}
	}
	return true
}

func (s *scheduler) release(ts *txnState) {
	now := time.Now()
	s.n.c.cfg.Observer.TxnReleased(NodeTxnEvent{
		Replica:        s.n.replica,
		Partition:      s.n.partition,
		Active:         ts.active,
		LockWait:       ts.granted.Sub(ts.admitted),
		RemoteReadWait: ts.remoteWait,
		Footprint:      now.Sub(ts.granted),
	})
	s.inflight--
	defer func() {
		s.inflightN.Store(int64(s.inflight))
		s.lockedKeys.Store(int64(len(s.locks)))
	}()
	for _, k := range ts.keys {
		q := s.locks[k]
		for i, r := range q {
			if r.ts == ts {
				q = append(q[:i], q[i+1:]...)
				break
			}
		}
		if len(q) == 0 {
			delete(s.locks, k)
			continue
		}
		s.locks[k] = q
		for i, r := range q {
			if r.write {
				if i == 0 && !r.granted {
					s.grant(r)
				}
				break
			}
			if !r.granted {
				s.grant(r)
			}
		}
	}
	if s.cp != nil && ts.txn.Seq < s.cp.marker.Seq {
		s.cp.remaining--
		if s.cp.remaining == 0 {
			s.fireCheckpoint()
		}
	}
}

func (s *scheduler) grant(r *lockReq) {
	r.granted = true
	r.ts.waiting--
	if r.ts.waiting == 0 {
		r.ts.granted = time.Now()
		go s.n.execute(r.ts)
	}
}

// beginCheckpoint, global sıradaki checkpoint işaretçisine (sanal tutarlılık
// noktası) ulaşıldığında çağrılır. Bu anda kabul edilmiş tüm işlemler
// işaretçiden öncedir; sonrakiler henüz kilit bile istememiştir.
func (s *scheduler) beginCheckpoint(marker *Txn) {
	s.n.store.beginCheckpoint(marker.Seq)
	s.cp = &cpWait{marker: marker, remaining: s.inflight}
	if s.inflight == 0 {
		s.fireCheckpoint()
	}
}

// fireCheckpoint, işaretçiden önceki tüm işlemler bittiğinde "before"
// sürümlerini asenkron olarak diske yazar; işlem yürütmesi durmaz.
func (s *scheduler) fireCheckpoint() {
	marker := s.cp.marker
	s.cp = nil
	n := s.n
	go func() {
		start := time.Now()
		snap := n.store.snapshotAndMerge()
		err := n.c.writeCheckpoint(n.replica, n.partition, marker.Seq, snap)
		d := time.Since(start)
		n.c.cfg.Observer.CheckpointDone(CheckpointEvent{
			Replica: n.replica, Partition: n.partition, Seq: marker.Seq,
			Records: len(snap), Duration: d, Err: err,
		})
		n.c.log.Debug("node checkpoint written", "replica", n.replica, "partition", n.partition,
			"seq", marker.Seq, "records", len(snap), "duration", d, "err", err)
		n.c.complete(marker, Result{Seq: marker.Seq, Committed: err == nil, Err: err})
	}()
}
