package calvin

import (
	"sync"
	"time"
)

// Bu dosya §3.1.1'deki Paxos tabanlı senkron replikasyonu uygular. Makaledeki
// implementasyon ZooKeeper kullanır; burada her replikasyon grubu (aynı
// partition'ın tüm replikaları) epoch başına bir slot olacak şekilde
// Multi-Paxos çalıştırır. Lider replica 0'daki sequencer'dır; faz 1 bir kez
// yapılır, sonraki her epoch yalnızca faz 2 gerektirir. Konsensüs kilitler
// alınmadan önce yapıldığı için yalnızca gecikmeyi artırır, throughput'u
// etkilemez.

type ballot struct {
	N  uint64
	ID int
}

func (b ballot) less(o ballot) bool {
	return b.N < o.N || (b.N == o.N && b.ID < o.ID)
}

type acceptedVal struct {
	b ballot
	v *batch
}

type acceptor struct {
	mu       sync.Mutex
	promised ballot
	accepted map[uint64]acceptedVal
}

func newAcceptor() *acceptor { return &acceptor{accepted: make(map[uint64]acceptedVal)} }

func (a *acceptor) prepare(b ballot) (bool, map[uint64]acceptedVal) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if b.less(a.promised) {
		return false, nil
	}
	a.promised = b
	acc := make(map[uint64]acceptedVal, len(a.accepted))
	for k, v := range a.accepted {
		acc[k] = v
	}
	return true, acc
}

func (a *acceptor) accept(b ballot, slot uint64, v *batch) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	if b.less(a.promised) {
		return false
	}
	a.promised = b
	a.accepted[slot] = acceptedVal{b: b, v: v}
	return true
}

// learned, seçilmiş ve tüm gruba dağıtılmış bir slotun durumunu temizler.
func (a *acceptor) learned(slot uint64) {
	a.mu.Lock()
	defer a.mu.Unlock()
	delete(a.accepted, slot)
}

type paxosLeader struct {
	s         *sequencer
	mu        sync.Mutex
	b         ballot
	prepared  bool
	recovered map[uint64]*batch
}

func (l *paxosLeader) propose(b *batch) { go l.run(b) }

func (l *paxosLeader) run(v *batch) {
	c := l.s.n.c
	for {
		select {
		case <-c.stop:
			return
		default:
		}
		bal, ok := l.ensurePrepared()
		if !ok {
			continue
		}
		l.mu.Lock()
		if rv, found := l.recovered[v.epoch]; found {
			// Paxos güvenliği: bu slot için daha önce kabul edilmiş bir değer
			// varsa onu önermek zorundayız.
			v = rv
			delete(l.recovered, v.epoch)
		}
		l.mu.Unlock()
		start := time.Now()
		ok = l.quorum(func(a *acceptor) bool { return a.accept(bal, v.epoch, v) })
		c.cfg.Observer.PaxosRound(l.s.n.partition, time.Since(start), ok)
		if ok {
			l.learn(v)
			return
		}
		c.log.Warn("paxos accept rejected, retrying with higher ballot",
			"partition", l.s.n.partition, "epoch", v.epoch, "ballot", bal.N)
		l.mu.Lock()
		if l.b == bal {
			l.prepared = false
			l.b.N++
		}
		l.mu.Unlock()
	}
}

func (l *paxosLeader) ensurePrepared() (ballot, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.prepared {
		return l.b, true
	}
	bal := l.b
	var accMu sync.Mutex
	best := make(map[uint64]acceptedVal)
	ok := l.quorum(func(a *acceptor) bool {
		promised, acc := a.prepare(bal)
		if promised {
			accMu.Lock()
			for slot, av := range acc {
				if cur, ok := best[slot]; !ok || cur.b.less(av.b) {
					best[slot] = av
				}
			}
			accMu.Unlock()
		}
		return promised
	})
	if !ok {
		l.b.N++
		return ballot{}, false
	}
	accMu.Lock()
	l.recovered = make(map[uint64]*batch, len(best))
	for slot, av := range best {
		l.recovered[slot] = av.v
	}
	accMu.Unlock()
	l.prepared = true
	l.s.n.c.log.Info("paxos leader prepared", "partition", l.s.n.partition,
		"ballot", bal.N, "recovered_slots", len(best))
	return bal, true
}

// quorum call'u gruptaki her acceptor'a (simüle edilmiş ağ üzerinden) uygular
// ve çoğunluk olumlu yanıt verirse true döner.
func (l *paxosLeader) quorum(call func(a *acceptor) bool) bool {
	c := l.s.n.c
	p := l.s.n.partition
	n := c.cfg.Replicas
	need := n/2 + 1
	ch := make(chan bool, n)
	for r := 0; r < n; r++ {
		a := c.nodes[r][p].seq.acceptor
		remote := r != l.s.n.replica
		go func() {
			c.delay(remote)
			ok := call(a)
			c.delay(remote)
			ch <- ok
		}()
	}
	yes, no := 0, 0
	for i := 0; i < n; i++ {
		select {
		case ok := <-ch:
			if ok {
				yes++
				if yes >= need {
					return true
				}
			} else {
				no++
				if no > n-need {
					return false
				}
			}
		case <-c.stop:
			return false
		}
	}
	return false
}

func (l *paxosLeader) learn(v *batch) {
	c := l.s.n.c
	for r := 0; r < c.cfg.Replicas; r++ {
		dst := c.nodes[r][l.s.n.partition].seq
		c.send(r != l.s.n.replica, func() {
			dst.acceptor.learned(v.epoch)
			dst.deliver(v)
		})
	}
}
