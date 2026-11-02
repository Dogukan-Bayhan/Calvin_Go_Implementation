package calvin

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"net/rpc"
	"sync"
	"time"
)

// Dağıtık modda her süreç bir veya daha fazla düğüm barındırır ve diğer
// düğümlerle net/rpc (gob) üzerinden konuşur. İki servis yayınlanır:
//   - "Node":   düğümler arası protokol mesajları.
//   - "Client": istemcilerin işlem gönderdiği ve yönetim çağrıları yaptığı uç.

// Serve, l üzerinden gelen bağlantıları kabul eder. Küme durdurulunca döner.
func (c *Cluster) Serve(l net.Listener) error {
	srv := rpc.NewServer()
	if err := srv.RegisterName("Node", &NodeRPC{c}); err != nil {
		return err
	}
	if err := srv.RegisterName("Client", &ClientRPC{c}); err != nil {
		return err
	}
	go func() {
		<-c.stop
		l.Close()
	}()
	for {
		conn, err := l.Accept()
		if err != nil {
			select {
			case <-c.stop:
				return nil
			default:
				return err
			}
		}
		if tc, ok := conn.(*net.TCPConn); ok {
			tc.SetNoDelay(true)
		}
		go srv.ServeConn(conn)
	}
}

// WaitPeers tüm uzak düğümlere ulaşılabilene kadar bekler. Tüm süreçler
// bunu Start'tan önce çağırınca liderler aşağı yukarı aynı anda başlar.
func (c *Cluster) WaitPeers(ctx context.Context) error {
	for _, row := range c.peers {
		for _, p := range row {
			rp, ok := p.(*rpcPeer)
			if !ok {
				continue
			}
			for {
				err := rp.call("Node.Ping", &Empty{}, &Empty{}, time.Second)
				if err == nil {
					break
				}
				c.log.Info("waiting for peer", "peer", rp.id, "addr", rp.addr, "err", err)
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(time.Second):
				}
			}
		}
	}
	return nil
}

// FetchGenesis, başlamış olan bir düğümden epoch başlangıcını alır; düğüm
// henüz başlamadıysa bekler. Dağıtık modda bir düğüm (örn. r0/p0) Start ile
// başlar, diğerleri FetchGenesis + StartAt ile aynı epoch saatine katılır.
func (c *Cluster) FetchGenesis(ctx context.Context, from NodeID) (time.Time, error) {
	rp, ok := c.peers[from.Replica][from.Partition].(*rpcPeer)
	if !ok {
		return time.Time{}, fmt.Errorf("calvin: %v is not a remote node", from)
	}
	for {
		var n int64
		if err := rp.call("Node.Genesis", &Empty{}, &n, time.Second); err == nil {
			return time.Unix(0, n), nil
		}
		select {
		case <-ctx.Done():
			return time.Time{}, ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func (c *Cluster) mustLocal(id NodeID) (*node, error) {
	if n := c.local(id); n != nil {
		return n, nil
	}
	return nil, fmt.Errorf("calvin: node %v is not hosted here", id)
}

// NodeRPC düğümler arası protokol uçlarıdır.
type NodeRPC struct{ c *Cluster }

func (s *NodeRPC) Ping(_ *Empty, _ *Empty) error { return nil }

func (s *NodeRPC) Genesis(_ *Empty, r *int64) error {
	if *r = s.c.genesisNanos.Load(); *r == 0 {
		return errors.New("calvin: not started yet")
	}
	return nil
}

func (s *NodeRPC) Submit(a *SubmitArgs, _ *Empty) error {
	n, err := s.c.mustLocal(a.To)
	if err != nil {
		return err
	}
	localPeer{to: n}.submit(fromWireTxn(a.Txn))
	return nil
}

func (s *NodeRPC) Batch(a *WireBatch, _ *Empty) error {
	n, err := s.c.mustLocal(a.To)
	if err != nil {
		return err
	}
	localPeer{to: n}.batch(fromWireBatch(a))
	return nil
}

func (s *NodeRPC) Replicate(a *WireBatch, _ *Empty) error {
	n, err := s.c.mustLocal(a.To)
	if err != nil {
		return err
	}
	n.seq.deliver(fromWireBatch(a))
	return nil
}

func (s *NodeRPC) Prepare(a *PrepareArgs, r *PrepareReply) error {
	n, err := s.c.mustLocal(a.To)
	if err != nil {
		return err
	}
	ok, acc := n.seq.acceptor.prepare(ballot{N: a.N, ID: a.ID})
	r.OK = ok
	r.Accepted = make(map[uint64]WireAccepted, len(acc))
	for slot, av := range acc {
		r.Accepted[slot] = WireAccepted{N: av.b.N, ID: av.b.ID, Batch: *toWireBatch(a.To, av.v)}
	}
	return nil
}

func (s *NodeRPC) Accept(a *AcceptArgs, r *bool) error {
	n, err := s.c.mustLocal(a.To)
	if err != nil {
		return err
	}
	*r = n.seq.acceptor.accept(ballot{N: a.N, ID: a.ID}, a.Slot, fromWireBatch(&a.Batch))
	return nil
}

func (s *NodeRPC) Learn(a *WireBatch, _ *Empty) error {
	n, err := s.c.mustLocal(a.To)
	if err != nil {
		return err
	}
	n.seq.acceptor.learned(a.Epoch)
	n.seq.deliver(fromWireBatch(a))
	return nil
}

func (s *NodeRPC) RemoteRead(a *RemoteReadArgs, _ *Empty) error {
	n, err := s.c.mustLocal(a.To)
	if err != nil {
		return err
	}
	n.mbox.put(a.Seq, remoteRead{from: a.From, values: fromWireVals(a.Values)})
	return nil
}

func (s *NodeRPC) Complete(a *CompleteArgs, _ *Empty) error {
	s.c.complete(a.ClientID, a.From, a.Result.Result())
	return nil
}

func (s *NodeRPC) Read(a *ReadArgs, r *ReadReply) error {
	n, err := s.c.mustLocal(a.To)
	if err != nil {
		return err
	}
	r.Val, r.Found = n.store.get(a.Key, seqMaxRead)
	return nil
}

// ClientRPC istemcilerin kullandığı uçlardır (bkz. calvin/client paketi).
type ClientRPC struct{ c *Cluster }

func (s *ClientRPC) Ping(_ *Empty, _ *Empty) error { return nil }

func (s *ClientRPC) Exec(a *ExecArgs, r *WireResult) error {
	res, err := s.c.Exec(context.Background(), a.Req)
	if err != nil {
		return err
	}
	*r = ToWireResult(res)
	return nil
}

func (s *ClientRPC) ExecDependent(a *ExecDependentArgs, r *WireResult) error {
	res, err := s.c.ExecDependent(context.Background(), a.Proc, a.Args, a.MaxAttempts)
	if err != nil {
		return err
	}
	*r = ToWireResult(res)
	return nil
}

func (s *ClientRPC) Checkpoint(_ *Empty, seq *uint64) error {
	var err error
	*seq, err = s.c.Checkpoint(context.Background())
	return err
}

func (s *ClientRPC) Stats(_ *Empty, r *[]NodeStats) error {
	*r = s.c.Stats()
	return nil
}

func (s *ClientRPC) Snapshot(_ *Empty, r *[]NodeSnapshot) error {
	*r = s.c.hostedSnapshots(func(n *node) map[Key][]byte {
		out := make(map[Key][]byte)
		n.store.scanAll(func(k Key, v []byte) { out[k] = v })
		return out
	})
	return nil
}

func (s *ClientRPC) CheckpointData(_ *Empty, r *[]NodeSnapshot) error {
	*r = s.c.hostedSnapshots(func(n *node) map[Key][]byte { return n.store.lastCheckpoint() })
	return nil
}

// rpcPeer başka bir süreçteki düğümdür. Tek yönlü mesajlar en az bir kez
// teslim edilir: bağlantı hatasında yeniden bağlanılıp tekrar gönderilir.
// Uygulama düzeyindeki hatalar (rpc.ServerError) tekrar denenmez.
type rpcPeer struct {
	c    *Cluster
	id   NodeID
	addr string

	mu   sync.Mutex
	cl   *rpc.Client
	done chan *rpc.Call
}

func newRPCPeer(c *Cluster, id NodeID, addr string) *rpcPeer {
	p := &rpcPeer{c: c, id: id, addr: addr, done: make(chan *rpc.Call, 1<<16)}
	go p.watch()
	return p
}

func (p *rpcPeer) client() (*rpc.Client, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.cl != nil {
		return p.cl, nil
	}
	conn, err := net.DialTimeout("tcp", p.addr, 2*time.Second)
	if err != nil {
		return nil, err
	}
	if tc, ok := conn.(*net.TCPConn); ok {
		tc.SetNoDelay(true)
	}
	p.cl = rpc.NewClient(conn)
	return p.cl, nil
}

// dropClient bağlantı hatasından sonra mevcut istemciyi kapatır; bir sonraki
// gönderim yeniden bağlanır.
func (p *rpcPeer) dropClient() {
	p.mu.Lock()
	cl := p.cl
	p.cl = nil
	p.mu.Unlock()
	if cl != nil {
		cl.Close()
	}
}

func isServerError(err error) bool {
	var se rpc.ServerError
	return errors.As(err, &se)
}

func (p *rpcPeer) oneway(method string, args any) {
	cl, err := p.client()
	if err != nil {
		p.retryLater(method, args, err)
		return
	}
	cl.Go(method, args, &Empty{}, p.done)
}

// watch tek yönlü çağrıların sonuçlarını izler ve başarısızları yeniden dener.
func (p *rpcPeer) watch() {
	for {
		select {
		case <-p.c.stop:
			return
		case call := <-p.done:
			switch {
			case call.Error == nil:
			case isServerError(call.Error):
				p.c.log.Warn("rpc rejected by peer", "peer", p.id, "method", call.ServiceMethod, "err", call.Error)
			default:
				p.dropClient()
				p.retryLater(call.ServiceMethod, call.Args, call.Error)
			}
		}
	}
}

func (p *rpcPeer) retryLater(method string, args any, cause error) {
	p.c.log.Debug("rpc retry", "peer", p.id, "method", method, "err", cause)
	go func() {
		select {
		case <-p.c.stop:
		case <-time.After(time.Duration(50+rand.Intn(150)) * time.Millisecond):
			p.oneway(method, args)
		}
	}()
}

// call senkron bir çağrı yapar.
func (p *rpcPeer) call(method string, args, reply any, timeout time.Duration) error {
	cl, err := p.client()
	if err != nil {
		return err
	}
	call := cl.Go(method, args, reply, make(chan *rpc.Call, 1))
	t := time.NewTimer(timeout)
	defer t.Stop()
	select {
	case <-call.Done:
		if call.Error != nil && !isServerError(call.Error) {
			p.dropClient()
		}
		return call.Error
	case <-t.C:
		return fmt.Errorf("calvin: rpc %s to %v timed out", method, p.id)
	case <-p.c.stop:
		return ErrStopped
	}
}

const paxosRPCTimeout = 2 * time.Second

func (p *rpcPeer) submit(t *Txn) {
	p.oneway("Node.Submit", &SubmitArgs{To: p.id, Txn: toWireTxn(t)})
}

func (p *rpcPeer) batch(b *batch) { p.oneway("Node.Batch", toWireBatch(p.id, b)) }

func (p *rpcPeer) replicate(b *batch) { p.oneway("Node.Replicate", toWireBatch(p.id, b)) }

func (p *rpcPeer) prepare(b ballot) (bool, map[uint64]acceptedVal) {
	var r PrepareReply
	if err := p.call("Node.Prepare", &PrepareArgs{To: p.id, N: b.N, ID: b.ID}, &r, paxosRPCTimeout); err != nil {
		return false, nil
	}
	acc := make(map[uint64]acceptedVal, len(r.Accepted))
	for slot, a := range r.Accepted {
		acc[slot] = acceptedVal{b: ballot{N: a.N, ID: a.ID}, v: fromWireBatch(&a.Batch)}
	}
	return r.OK, acc
}

func (p *rpcPeer) accept(b ballot, slot uint64, v *batch) bool {
	var ok bool
	args := &AcceptArgs{To: p.id, N: b.N, ID: b.ID, Slot: slot, Batch: *toWireBatch(p.id, v)}
	if err := p.call("Node.Accept", args, &ok, paxosRPCTimeout); err != nil {
		return false
	}
	return ok
}

func (p *rpcPeer) learn(v *batch) { p.oneway("Node.Learn", toWireBatch(p.id, v)) }

func (p *rpcPeer) remoteRead(seq uint64, m remoteRead) {
	p.oneway("Node.RemoteRead", &RemoteReadArgs{To: p.id, Seq: seq, From: m.from, Values: toWireVals(m.values)})
}

func (p *rpcPeer) complete(clientID uint64, from NodeID, res Result) {
	p.oneway("Node.Complete", &CompleteArgs{To: p.id, ClientID: clientID, From: from, Result: ToWireResult(res)})
}

func (p *rpcPeer) read(k Key) ([]byte, bool) {
	var r ReadReply
	if err := p.call("Node.Read", &ReadArgs{To: p.id, Key: k}, &r, 5*time.Second); err != nil {
		return nil, false
	}
	return r.Val, r.Found
}
