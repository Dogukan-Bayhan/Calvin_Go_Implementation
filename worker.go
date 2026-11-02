package calvin

import (
	"fmt"
	"maps"
	"slices"
	"sync"
	"time"
)

type remoteRead struct {
	from   int
	values map[Key]readVal
}

// mailbox, uzak okuma sonuçlarını işlem sırası numarasına göre tamponlar.
// Karşı düğüm işlemi henüz başlatmamış olsa bile mesajlar burada bekler.
// Aynı göndericiden gelen tekrarlar ve işi bitmiş işlemlere geç gelen
// mesajlar yok sayılır.
type mailbox struct {
	mu      sync.Mutex
	size    int
	boxes   map[uint64]*inbox
	dropped map[uint64]struct{}
	order   []uint64
}

type inbox struct {
	ch   chan remoteRead
	from map[int]bool
}

const mailboxDroppedWindow = 1 << 16

func newMailbox(size int) mailbox {
	return mailbox{size: size, boxes: make(map[uint64]*inbox), dropped: make(map[uint64]struct{})}
}

func (m *mailbox) boxLocked(seq uint64) *inbox {
	ib, ok := m.boxes[seq]
	if !ok {
		ib = &inbox{ch: make(chan remoteRead, m.size), from: make(map[int]bool)}
		m.boxes[seq] = ib
	}
	return ib
}

func (m *mailbox) box(seq uint64) chan remoteRead {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.boxLocked(seq).ch
}

func (m *mailbox) put(seq uint64, msg remoteRead) {
	m.mu.Lock()
	if _, done := m.dropped[seq]; done {
		m.mu.Unlock()
		return
	}
	ib := m.boxLocked(seq)
	if ib.from[msg.from] {
		m.mu.Unlock()
		return
	}
	ib.from[msg.from] = true
	m.mu.Unlock()
	ib.ch <- msg // farklı gönderici sayısı <= partition sayısı = kapasite
}

func (m *mailbox) drop(seq uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.boxes, seq)
	m.dropped[seq] = struct{}{}
	m.order = append(m.order, seq)
	if len(m.order) > mailboxDroppedWindow {
		delete(m.dropped, m.order[0])
		m.order = m.order[1:]
	}
}

// execute, tüm kilitleri alınmış bir işlemi §3.2'deki beş aşamada yürütür.
// Her işlem kendi goroutine'inde çalışır: uzak okuma bekleyen bir işlem,
// sınırlı bir worker havuzunu tıkayıp ilerlemeyi engellemesin diye.
func (n *node) execute(ts *txnState) {
	c := n.c
	t := ts.txn

	// 1. Okuma/yazma kümesi analizi.
	readParts := make(map[int]bool)
	for _, k := range t.ReadSet {
		readParts[c.partitionOf(k)] = true
	}
	var active []int
	for _, k := range t.WriteSet {
		if q := c.partitionOf(k); !slices.Contains(active, q) {
			active = append(active, q)
		}
	}
	if len(active) == 0 {
		// Salt okunur işlem: mantığı en küçük okuma partition'ı yürütür.
		minP := -1
		for q := range readParts {
			if minP == -1 || q < minP {
				minP = q
			}
		}
		active = []int{minP}
	}
	slices.Sort(active)
	isActive := slices.Contains(active, n.partition)
	ts.active = isActive

	// 2. Yerel okumalar.
	local := make(map[Key]readVal)
	for _, k := range t.ReadSet {
		if c.partitionOf(k) == n.partition {
			v, ok := n.store.get(k, t.Seq)
			local[k] = readVal{val: v, found: ok}
		}
	}

	// 3. Uzak okumaları servis et: yerel okumalar tüm aktif katılımcılara gider.
	if len(local) > 0 {
		for _, q := range active {
			if q == n.partition {
				continue
			}
			c.peer(n, NodeID{n.replica, q}).remoteRead(t.Seq, remoteRead{from: n.partition, values: local})
		}
	}
	if !isActive {
		// Pasif katılımcı veri değiştirmez; burada işi biter.
		n.releaseLocks(ts)
		c.workDone()
		return
	}

	// 4. Uzak okuma sonuçlarını topla.
	// local, uzak okuma mesajlarıyla paylaşıldığı için kopyalanır.
	reads := maps.Clone(local)
	expected := len(readParts)
	if readParts[n.partition] {
		expected--
	}
	if expected > 0 {
		waitStart := time.Now()
		box := n.mbox.box(t.Seq)
		for i := 0; i < expected; i++ {
			select {
			case m := <-box:
				for k, v := range m.values {
					reads[k] = v
				}
			case <-c.stop:
				return
			}
		}
		n.mbox.drop(t.Seq)
		ts.remoteWait = time.Since(waitStart)
	}

	// 5. Mantığı yürüt ve yerel yazmaları uygula. Yerel olmayan yazmalar,
	// karşı düğümdeki aynı işlem tarafından yerel yazma olarak uygulanır.
	res, tx := n.runLogic(t, reads)
	if res.Committed {
		for k, w := range tx.writes {
			if c.partitionOf(k) == n.partition {
				n.store.put(k, w, t.Seq)
			}
		}
	}
	n.releaseLocks(ts)
	if n.partition == active[0] {
		c.peer(n, t.origin).complete(t.ClientID, n.id(), res)
	}
	c.workDone()
}

func (n *node) releaseLocks(ts *txnState) {
	select {
	case n.sched.releaseCh <- ts:
	case <-n.c.stop:
	}
}

func (n *node) runLogic(t *Txn, reads map[Key]readVal) (res Result, tx *Tx) {
	res.Seq = t.Seq
	tx = newTx(t, reads)
	entry, ok := n.c.reg.lookup(t.Proc)
	if !ok {
		res.Err = fmt.Errorf("%w: unknown procedure %q", ErrAborted, t.Proc)
		return res, tx
	}

	// OLLP: keşif sorgusunu gerçek okumalar üzerinde tekrar çalıştır. Küme
	// değiştiyse tüm aktif katılımcılar aynı sonuca varır ve işlem
	// deterministik olarak yeniden başlatılır.
	if t.Dependent && entry.recon != nil {
		rs, ws, err := entry.recon(tx, t.Args)
		if err != nil || tx.violation != nil || !sameKeys(rs, t.ReadSet) || !sameKeys(ws, t.WriteSet) {
			res.Err = ErrRestart
			return res, tx
		}
	}

	err := func() (err error) {
		defer func() {
			if r := recover(); r != nil {
				err = fmt.Errorf("panic: %v", r)
			}
		}()
		return entry.fn(tx)
	}()
	if err == nil {
		err = tx.violation
	}
	if err != nil {
		res.Err = fmt.Errorf("%w: %w", ErrAborted, err)
		return res, tx
	}
	res.Committed = true
	res.Output = tx.output
	return res, tx
}

func sameKeys(a, b []Key) bool {
	as, bs := slices.Clone(a), slices.Clone(b)
	slices.Sort(as)
	slices.Sort(bs)
	return slices.Equal(slices.Compact(as), slices.Compact(bs))
}
