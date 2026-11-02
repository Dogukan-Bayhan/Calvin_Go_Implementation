package calvin

import (
	"sync"
	"sync/atomic"
	"time"
)

type delayedTxn struct {
	t       *Txn
	readyAt time.Time
}

// sequencer, §3.1'deki sıralama katmanının bir partition'ıdır. Her
// replikasyon grubunda replica 0'daki sequencer lider/master'dır: istemci
// isteklerini toplar, epoch sonunda batch'e çevirir ve replike eder. Diğer
// replikalardaki sequencer'lar istekleri lidere iletir ve replike edilmiş
// batch'leri kendi replikalarının scheduler'larına dağıtır.
type sequencer struct {
	n  *node
	in chan *Txn

	// yalnızca lider goroutine'i kullanır
	epoch   uint64
	waiting []delayedTxn

	acceptor *acceptor
	paxos    *paxosLeader

	sequenced atomic.Uint64 // lider tarafından kapatılan son epoch

	logMu sync.Mutex
	log   []*batch // replike edilmiş girdi logu (fiziksel REDO logu yok)
}

func newSequencer(n *node) *sequencer {
	s := &sequencer{n: n, in: make(chan *Txn, 4096), acceptor: newAcceptor()}
	if n.replica == 0 && n.c.cfg.Replication == PaxosReplication {
		s.paxos = &paxosLeader{s: s, b: ballot{N: 1, ID: n.replica}}
	}
	return s
}

func (s *sequencer) leader() *sequencer { return s.n.c.nodes[0][s.n.partition].seq }

// submit bir istemci isteğini kabul eder. Hem asenkron hem Paxos modunda
// istekler grubun liderine (replica 0) iletilir.
func (s *sequencer) submit(t *Txn) {
	c := s.n.c
	l := s.leader()
	c.send(s.n.replica != 0, func() {
		select {
		case l.in <- t:
		case <-c.stop:
		}
	})
}

func (s *sequencer) run() {
	c := s.n.c
	defer c.wg.Done()
	ticker := time.NewTicker(c.cfg.EpochDuration)
	defer ticker.Stop()
	for {
		select {
		case <-c.stop:
			return
		case t := <-s.in:
			s.accept(t)
		case now := <-ticker.C:
			s.cutBatch(now)
		}
	}
}

// accept, §4'teki prefetch mekanizmasını uygular: işlem soğuk bir kayda
// dokunuyorsa ilgili depolama bileşenlerine ısıtma isteği gönderilir ve işlem
// tahmini disk gecikmesi kadar bekletilir. Böylece disk gecikmesi kilitler
// alınmadan önce, contention footprint dışında yaşanır.
//
// Not: Makaledeki deneylerde olduğu gibi her sequencer tüm sistemdeki sıcak
// kayıtları bilir; burada bu, motorlara doğrudan sorarak simüle edilir.
func (s *sequencer) accept(t *Txn) {
	c := s.n.c
	readyAt := time.Time{}
	if c.cfg.PrefetchDelay > 0 && t.kind == kindNormal {
		cold := false
		for _, k := range append(t.ReadSet[:len(t.ReadSet):len(t.ReadSet)], t.WriteSet...) {
			q := c.partitionOf(k)
			for r := 0; r < c.cfg.Replicas; r++ {
				if pf, ok := c.nodes[r][q].store.eng.(Prefetcher); ok && !pf.IsHot(k) {
					cold = true
					pf.Prefetch(k)
				}
			}
		}
		if cold {
			readyAt = time.Now().Add(c.cfg.PrefetchDelay)
		}
	}
	s.waiting = append(s.waiting, delayedTxn{t: t, readyAt: readyAt})
}

// cutBatch epoch'u kapatır. Boş olsa bile her epoch için bir batch üretilir;
// scheduler'lar bir epoch'un tamamlandığını tüm sequencer'lardan batch
// gelmesiyle anlar.
func (s *sequencer) cutBatch(now time.Time) {
	s.epoch++
	b := &batch{epoch: s.epoch, from: s.n.partition}
	rest := s.waiting[:0]
	for _, d := range s.waiting {
		if len(b.txns) < maxBatch && !now.Before(d.readyAt) {
			d.t.Seq = makeSeq(b.epoch, b.from, len(b.txns))
			b.txns = append(b.txns, d.t)
		} else {
			rest = append(rest, d)
		}
	}
	s.waiting = rest
	s.sequenced.Store(b.epoch)
	s.n.c.cfg.Observer.BatchSequenced(b.from, b.epoch, len(b.txns))

	if s.paxos != nil {
		s.paxos.propose(b)
		return
	}
	// Asenkron replikasyon: master hemen kendi replikasına dağıtır, batch'i
	// gruptaki slave sequencer'lara arka planda gönderir.
	c := s.n.c
	s.deliver(b)
	for r := 1; r < c.cfg.Replicas; r++ {
		dst := c.nodes[r][s.n.partition].seq
		c.send(true, func() { dst.deliver(b) })
	}
}

// deliver replike edilmiş bir batch'i loglar ve kendi replikasındaki her
// scheduler'a yalnızca onun katılacağı işlemleri gönderir.
func (s *sequencer) deliver(b *batch) {
	c := s.n.c
	s.logMu.Lock()
	s.log = append(s.log, b)
	s.logMu.Unlock()

	subs := make([][]*Txn, c.cfg.Partitions)
	for _, t := range b.txns {
		for _, q := range t.parts {
			subs[q] = append(subs[q], t)
		}
	}
	for q := 0; q < c.cfg.Partitions; q++ {
		sched := c.nodes[s.n.replica][q].sched
		sub := &batch{epoch: b.epoch, from: b.from, txns: subs[q]}
		c.send(q != s.n.partition, func() {
			select {
			case sched.batchIn <- sub:
			case <-c.stop:
			}
		})
	}
}

// truncateLog, tamamlanmış bir checkpoint'ten önceki epoch'ları girdi
// logundan atar: kurtarma o checkpoint'ten başlayıp sonrasını yeniden
// oynatacağı için daha eskisine gerek yoktur.
func (s *sequencer) truncateLog(beforeEpoch uint64) {
	s.logMu.Lock()
	defer s.logMu.Unlock()
	i := 0
	for i < len(s.log) && s.log[i].epoch < beforeEpoch {
		i++
	}
	s.log = append([]*batch(nil), s.log[i:]...)
}
