package calvin

import "fmt"

// NodeID bir düğümün kimliğidir: replikası ve partition'ı.
type NodeID struct{ Replica, Partition int }

func (id NodeID) String() string { return fmt.Sprintf("r%d/p%d", id.Replica, id.Partition) }

// peer, bir düğüme ulaşmanın yoludur. Aynı süreçteki düğümler doğrudan
// (isteğe bağlı simüle gecikmeyle) çağrılır; başka süreçlerdeki düğümler RPC
// ile (rpc.go). Ağ üzerinden bir mesaj birden fazla kez teslim edilebileceği
// için tüm alıcı taraflar tekrarları yok sayar.
type peer interface {
	// submit bir istemci işlemini düğümün sequencer'ına (grup lideri) iletir.
	submit(t *Txn)
	// batch bir epoch batch'inin bu düğüme düşen kısmını scheduler'a iletir.
	batch(b *batch)
	// replicate asenkron replikasyonda batch'i slave sequencer'a iletir.
	replicate(b *batch)
	prepare(b ballot) (bool, map[uint64]acceptedVal)
	accept(b ballot, slot uint64, v *batch) bool
	learn(v *batch)
	// remoteRead 3. aşamadaki okuma sonuçlarını aktif katılımcıya iletir.
	remoteRead(seq uint64, m remoteRead)
	// complete bir işlemin sonucunu, isteği alan (origin) düğüme bildirir.
	complete(clientID uint64, from NodeID, res Result)
	// read kilitsiz okur (OLLP keşif sorgusu için).
	read(k Key) ([]byte, bool)
}

// localPeer aynı süreçteki bir düğümdür.
type localPeer struct {
	to      *node
	delayed bool // gönderen farklı bir düğümse simüle ağ gecikmesi uygulanır
}

func (p localPeer) send(fn func()) { p.to.c.send(p.delayed, fn) }

func (p localPeer) submit(t *Txn) {
	c, in := p.to.c, p.to.seq.in
	p.send(func() {
		select {
		case in <- t:
		case <-c.stop:
		}
	})
}

func (p localPeer) batch(b *batch) {
	c, in := p.to.c, p.to.sched.batchIn
	p.send(func() {
		select {
		case in <- b:
		case <-c.stop:
		}
	})
}

func (p localPeer) replicate(b *batch) {
	s := p.to.seq
	p.send(func() { s.deliver(b) })
}

func (p localPeer) prepare(b ballot) (bool, map[uint64]acceptedVal) {
	p.to.c.delay(p.delayed)
	ok, acc := p.to.seq.acceptor.prepare(b)
	p.to.c.delay(p.delayed)
	return ok, acc
}

func (p localPeer) accept(b ballot, slot uint64, v *batch) bool {
	p.to.c.delay(p.delayed)
	ok := p.to.seq.acceptor.accept(b, slot, v)
	p.to.c.delay(p.delayed)
	return ok
}

func (p localPeer) learn(v *batch) {
	s := p.to.seq
	p.send(func() {
		s.acceptor.learned(v.epoch)
		s.deliver(v)
	})
}

func (p localPeer) remoteRead(seq uint64, m remoteRead) {
	mb := &p.to.mbox
	p.send(func() { mb.put(seq, m) })
}

func (p localPeer) complete(clientID uint64, from NodeID, res Result) {
	c := p.to.c
	p.send(func() { c.complete(clientID, from, res) })
}

func (p localPeer) read(k Key) ([]byte, bool) { return p.to.store.get(k, seqMaxRead) }
