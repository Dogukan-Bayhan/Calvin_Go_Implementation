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
type mailbox struct {
	mu    sync.Mutex
	size  int
	boxes map[uint64]chan remoteRead
}

func (m *mailbox) box(seq uint64) chan remoteRead {
	m.mu.Lock()
	defer m.mu.Unlock()
	ch, ok := m.boxes[seq]
	if !ok {
		ch = make(chan remoteRead, m.size)
		m.boxes[seq] = ch
	}
	return ch
}

func (m *mailbox) drop(seq uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.boxes, seq)
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
			box := c.nodes[n.replica][q].mbox.box(t.Seq)
			msg := remoteRead{from: n.partition, values: local}
			c.send(true, func() { box <- msg })
		}
	}
	if !isActive {
		// Pasif katılımcı veri değiştirmez; burada işi biter.
		n.releaseLocks(ts)
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
		c.complete(t, res)
	}
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
