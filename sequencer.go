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

type submitKey struct {
	origin   NodeID
	clientID uint64
}

// submitDedupWindow, liderin tekrar gelen istemci isteklerini tanımak için
// hatırladığı son istek sayısıdır. Ağ üzerinden en az bir kez teslimat,
// aynı isteğin iki kez sıraya girmesine yol açmamalıdır.
const submitDedupWindow = 1 << 20

// sequencer, §3.1'deki sıralama katmanının bir partition'ıdır. Her
// replikasyon grubunda replica 0'daki sequencer lider/master'dır: istemci
// isteklerini toplar, epoch sonunda batch'e çevirir ve replike eder. Diğer
// replikalardaki sequencer'lar istekleri lidere iletir ve replike edilmiş
// batch'leri kendi replikalarının scheduler'larına dağıtır.
type sequencer struct {
	n  *node
	in chan *Txn

	// yalnızca lider goroutine'i kullanır
	epoch      uint64
	waiting    []delayedTxn
	seen       map[submitKey]struct{}
	seenOrder  []submitKey
	acceptor   *acceptor
	paxos      *paxosLeader
	sequenced  atomic.Uint64 // lider tarafından kapatılan son epoch
	queued     atomic.Int64  // liderde batch'lenmeyi bekleyen işlem sayısı
	logMu      sync.Mutex
	log        []*batch // replike edilmiş girdi logu (fiziksel REDO logu yok)
	delivered  map[uint64]struct{}
	deliveredW uint64 // bu epoch'a kadar (dahil) tüm epoch'lar teslim edildi
}

func newSequencer(n *node) *sequencer {
	s := &sequencer{
		n: n, in: make(chan *Txn, 4096), acceptor: newAcceptor(),
		seen: make(map[submitKey]struct{}), delivered: make(map[uint64]struct{}),
	}
	if n.replica == 0 && n.c.cfg.Replication == PaxosReplication {
		s.paxos = &paxosLeader{s: s, b: ballot{N: 1, ID: n.replica}}
	}
	return s
}

// submit bir istemci isteğini kabul eder. Hem asenkron hem Paxos modunda
// istekler grubun liderine (replica 0) iletilir.
func (s *sequencer) submit(t *Txn) {
	s.n.c.peer(s.n, NodeID{0, s.n.partition}).submit(t)
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
			// Epoch numarası tüm liderler için ortak başlangıç anından (genesis)
			// itibaren duvar saatiyle belirlenir (§3.1: epoch tüm sistemde
			// senkron artar). Geç başlayan veya duraklayan bir lider kaçırdığı
			// epoch'lar için boş batch keserek yetişir. Aksi halde liderlerin
			// epoch'ları kayar ve scheduler'lar en geride kalan lideri bekler.
			target := uint64(now.Sub(c.genesis()) / c.cfg.EpochDuration)
			for s.epoch < target {
				s.cutBatch(now)
			}
		}
	}
}

// accept, §4'teki prefetch mekanizmasını uygular: işlem soğuk bir kayda
// dokunuyorsa ilgili depolama bileşenlerine ısıtma isteği gönderilir ve işlem
// tahmini disk gecikmesi kadar bekletilir. Böylece disk gecikmesi kilitler
// alınmadan önce, contention footprint dışında yaşanır.
//
// Not: Makaledeki deneylerde olduğu gibi sequencer sıcak kayıtları bilir;
// burada bu, aynı süreçteki motorlara doğrudan sorarak simüle edilir. Başka
// süreçlerdeki düğümler için prefetch yapılmaz.
func (s *sequencer) accept(t *Txn) {
	key := submitKey{t.origin, t.ClientID}
	if _, dup := s.seen[key]; dup {
		return
	}
	s.seen[key] = struct{}{}
	s.seenOrder = append(s.seenOrder, key)
	if len(s.seenOrder) > submitDedupWindow {
		delete(s.seen, s.seenOrder[0])
		s.seenOrder = s.seenOrder[1:]
	}

	c := s.n.c
	readyAt := time.Time{}
	if c.cfg.PrefetchDelay > 0 && t.kind == kindNormal {
		cold := false
		for _, k := range append(t.ReadSet[:len(t.ReadSet):len(t.ReadSet)], t.WriteSet...) {
			q := c.partitionOf(k)
			for r := 0; r < c.cfg.Replicas; r++ {
				n := c.local(NodeID{r, q})
				if n == nil {
					continue
				}
				if pf, ok := n.store.eng.(Prefetcher); ok && !pf.IsHot(k) {
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
	s.queued.Store(int64(len(s.waiting)))
}

// cutBatch epoch'u kapatır. Boş olsa bile her epoch için bir batch üretilir;
// scheduler'lar bir epoch'un tamamlandığını tüm sequencer'lardan batch
// gelmesiyle anlar.
func (s *sequencer) cutBatch(now time.Time) {
	s.epoch++
	b := &batch{epoch: s.epoch, from: s.n.partition, cutAt: now.UnixNano()}
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
	s.queued.Store(int64(len(s.waiting)))
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
		c.peer(s.n, NodeID{r, s.n.partition}).replicate(b)
	}
}

// deliver replike edilmiş bir batch'i loglar ve kendi replikasındaki her
// scheduler'a yalnızca onun katılacağı işlemleri gönderir. Aynı epoch'un
// tekrar teslimi yok sayılır.
func (s *sequencer) deliver(b *batch) {
	c := s.n.c
	s.logMu.Lock()
	if _, dup := s.delivered[b.epoch]; dup || b.epoch <= s.deliveredW {
		s.logMu.Unlock()
		return
	}
	s.delivered[b.epoch] = struct{}{}
	for {
		if _, ok := s.delivered[s.deliveredW+1]; !ok {
			break
		}
		s.deliveredW++
		delete(s.delivered, s.deliveredW)
	}
	s.log = append(s.log, b)
	s.logMu.Unlock()

	subs := make([][]*Txn, c.cfg.Partitions)
	for _, t := range b.txns {
		for _, q := range t.parts {
			subs[q] = append(subs[q], t)
		}
	}
	for q := 0; q < c.cfg.Partitions; q++ {
		sub := &batch{epoch: b.epoch, from: b.from, cutAt: b.cutAt, txns: subs[q]}
		c.peer(s.n, NodeID{s.n.replica, q}).batch(sub)
	}
}

// truncateLog, tamamlanmış bir checkpoint'ten önceki epoch'ları girdi
// logundan atar: kurtarma o checkpoint'ten başlayıp sonrasını yeniden
// oynatacağı için daha eskisine gerek yoktur.
func (s *sequencer) truncateLog(beforeEpoch uint64) {
	s.logMu.Lock()
	defer s.logMu.Unlock()
	kept := s.log[:0]
	for _, b := range s.log {
		if b.epoch >= beforeEpoch {
			kept = append(kept, b)
		}
	}
	s.log = append([]*batch(nil), kept...)
}
