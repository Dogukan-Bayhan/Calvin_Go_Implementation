package calvin

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"time"
)

// ReplicationMode, işlem girdilerinin replike edilme biçimidir (§3.1.1).
type ReplicationMode int

const (
	// AsyncReplication: master replika (0) batch'i hemen yürütür ve
	// slave'lere arka planda gönderir. En düşük gecikme.
	AsyncReplication ReplicationMode = iota
	// PaxosReplication: her replikasyon grubu, her epoch'un batch'i üzerinde
	// Paxos ile anlaşır; batch ancak çoğunluk kabul edince yürütülür.
	PaxosReplication
)

func (m ReplicationMode) String() string {
	if m == PaxosReplication {
		return "paxos"
	}
	return "async"
}

type Config struct {
	Replicas   int
	Partitions int
	// EpochDuration sequencer batch aralığıdır (makalede 10 ms).
	EpochDuration time.Duration
	Replication   ReplicationMode
	// NetworkDelay, aynı süreçteki düğümler arası her mesaja eklenen tek
	// yönlü simüle gecikmedir. Gerçek ağ üzerindeki düğümlere uygulanmaz.
	NetworkDelay time.Duration
	// PrefetchDelay, soğuk veriye erişen işlemlerin sequencer'da bekletileceği
	// tahmini disk gecikmesidir (§4.1). 0 ise prefetch kapalıdır.
	PrefetchDelay time.Duration
	// NewEngine her (replica, partition) için depolama motoru üretir.
	// Varsayılan MemEngine'dir.
	NewEngine func(replica, partition int) Engine
	// Partitioner bir anahtarın partition'ını belirler. Varsayılan FNV hash.
	// Dağıtık modda tüm süreçlerde aynı olmalıdır.
	Partitioner func(k Key, partitions int) int
	// CheckpointDir boş değilse checkpoint'ler buraya JSON olarak yazılır.
	CheckpointDir string
	// CheckpointKeep, düğüm başına diskte tutulacak checkpoint dosyası
	// sayısıdır; eskileri silinir. Varsayılan 2.
	CheckpointKeep int
	// Observer metrik kancalarıdır. Varsayılan NopObserver.
	Observer Observer
	// Logger yapılandırılmış log çıktısıdır. Varsayılan: log yok.
	Logger *slog.Logger

	// Hosted bu süreçte çalışacak düğümlerdir. Boşsa tüm düğümler bu süreçte
	// çalışır (tek süreçli simülasyon). Doluysa küme dağıtık moddadır: diğer
	// düğümlere PeerAddr ile bulunan adresler üzerinden RPC ile ulaşılır ve
	// süreç Serve ile dinlemelidir.
	Hosted []NodeID
	// PeerAddr dağıtık modda bir düğümün "host:port" adresini döner.
	PeerAddr func(NodeID) string
}

func (c *Config) setDefaults() error {
	if c.Replicas <= 0 {
		c.Replicas = 1
	}
	if c.Partitions <= 0 {
		c.Partitions = 1
	}
	if c.Partitions > maxParts {
		return fmt.Errorf("calvin: at most %d partitions supported", maxParts)
	}
	if c.EpochDuration <= 0 {
		c.EpochDuration = 10 * time.Millisecond
	}
	if c.NewEngine == nil {
		c.NewEngine = func(int, int) Engine { return NewMemEngine() }
	}
	if c.CheckpointKeep <= 0 {
		c.CheckpointKeep = 2
	}
	if c.Observer == nil {
		c.Observer = NopObserver{}
	}
	if c.Logger == nil {
		c.Logger = slog.New(slog.DiscardHandler)
	}
	if c.Partitioner == nil {
		c.Partitioner = func(k Key, n int) int {
			h := fnv.New32a()
			h.Write([]byte(k))
			return int(h.Sum32() % uint32(n))
		}
	}
	for _, id := range c.Hosted {
		if id.Replica < 0 || id.Replica >= c.Replicas || id.Partition < 0 || id.Partition >= c.Partitions {
			return fmt.Errorf("calvin: hosted node %v out of range", id)
		}
	}
	if len(c.Hosted) > 0 && c.PeerAddr == nil {
		return errors.New("calvin: PeerAddr is required when Hosted is set")
	}
	return nil
}

type node struct {
	c         *Cluster
	replica   int
	partition int
	store     *store
	seq       *sequencer
	sched     *scheduler
	mbox      mailbox
}

func (n *node) id() NodeID { return NodeID{n.replica, n.partition} }

type pending struct {
	t        *Txn
	ch       chan Result
	expected int
	all      bool // true: sonuç son tamamlanmada verilir (checkpoint)
	once     sync.Once
	mu       sync.Mutex
	seen     map[NodeID]bool // aynı düğümden gelen tekrar bildirimler yok sayılır
	err      error
}

// Cluster, replica x partition düğümlerinden oluşan bir Calvin dağıtımıdır.
// Tek süreçli modda tüm düğümleri, dağıtık modda yalnızca Config.Hosted
// düğümlerini barındırır.
type Cluster struct {
	cfg         Config
	log         *slog.Logger
	reg         *Registry
	nodes       [][]*node // bu süreçte olmayan düğümler için nil
	peers       [][]peer  // uzak düğümler için rpcPeer, yereller için nil
	hosted      []*node
	distributed bool

	nextClient atomic.Uint64
	rr         atomic.Uint64
	pending    sync.Map // clientID -> *pending
	inflight   sync.WaitGroup
	cpMu       sync.Mutex

	genesisNanos atomic.Int64 // epoch 0'ın başladığı an (unix ns); 0: başlamadı

	stop     chan struct{}
	stopOnce sync.Once
	wg       sync.WaitGroup
}

func NewCluster(cfg Config, reg *Registry) (*Cluster, error) {
	if err := cfg.setDefaults(); err != nil {
		return nil, err
	}
	c := &Cluster{cfg: cfg, log: cfg.Logger, reg: reg, stop: make(chan struct{}),
		distributed: len(cfg.Hosted) > 0}
	hosted := make(map[NodeID]bool)
	for _, id := range cfg.Hosted {
		hosted[id] = true
	}
	c.nodes = make([][]*node, cfg.Replicas)
	c.peers = make([][]peer, cfg.Replicas)
	for r := range c.nodes {
		c.nodes[r] = make([]*node, cfg.Partitions)
		c.peers[r] = make([]peer, cfg.Partitions)
		for p := range c.nodes[r] {
			id := NodeID{r, p}
			if c.distributed && !hosted[id] {
				c.peers[r][p] = newRPCPeer(c, id, cfg.PeerAddr(id))
				continue
			}
			n := &node{c: c, replica: r, partition: p, store: newStore(cfg.NewEngine(r, p)),
				mbox: newMailbox(cfg.Partitions)}
			c.nodes[r][p] = n
			c.hosted = append(c.hosted, n)
		}
	}
	for _, n := range c.hosted {
		n.seq = newSequencer(n)
		n.sched = newScheduler(n)
	}
	return c, nil
}

func (c *Cluster) local(id NodeID) *node { return c.nodes[id.Replica][id.Partition] }

// peer, from düğümünden to düğümüne ulaşmanın yolunu döner.
func (c *Cluster) peer(from *node, to NodeID) peer {
	if n := c.local(to); n != nil {
		return localPeer{to: n, delayed: n != from}
	}
	return c.peers[to.Replica][to.Partition]
}

// Load başlangıç verisini, bu süreçteki ilgili düğümlere doğrudan yükler.
// Start'tan önce çağrılmalıdır. Dağıtık modda her süreç aynı veriyle
// çağırabilir; yalnızca kendi düğümlerine düşen anahtarlar yüklenir.
func (c *Cluster) Load(k Key, v []byte) {
	p := c.partitionOf(k)
	for r := range c.nodes {
		if n := c.local(NodeID{r, p}); n != nil {
			n.store.eng.Put(k, v)
		}
	}
}

// Start kümeyi şimdiki anı epoch başlangıcı (genesis) kabul ederek başlatır.
func (c *Cluster) Start() { c.StartAt(time.Now()) }

// StartAt kümeyi verilen epoch başlangıcıyla başlatır. Dağıtık modda tüm
// süreçler aynı genesis'i kullanmalıdır (bkz. FetchGenesis); epoch k, genesis +
// k*EpochDuration anında kesilir. Makineler arası saat farkı kadar gecikme
// eklenir; doğruluğu etkilemez.
func (c *Cluster) StartAt(genesis time.Time) {
	c.genesisNanos.Store(genesis.UnixNano())
	c.log.Info("cluster started", "replicas", c.cfg.Replicas, "partitions", c.cfg.Partitions,
		"replication", c.cfg.Replication.String(), "epoch", c.cfg.EpochDuration,
		"network_delay", c.cfg.NetworkDelay, "prefetch_delay", c.cfg.PrefetchDelay,
		"hosted", len(c.hosted), "distributed", c.distributed, "genesis", genesis.UTC())
	for _, n := range c.hosted {
		c.wg.Add(1)
		go n.sched.run()
		if n.replica == 0 {
			c.wg.Add(1)
			go n.seq.run()
		}
	}
}

func (c *Cluster) genesis() time.Time { return time.Unix(0, c.genesisNanos.Load()) }

// Genesis kümenin epoch başlangıcıdır; küme başlamadıysa sıfır değer.
func (c *Cluster) Genesis() time.Time {
	if n := c.genesisNanos.Load(); n != 0 {
		return time.Unix(0, n)
	}
	return time.Time{}
}

func (c *Cluster) Stop() {
	c.stopOnce.Do(func() {
		close(c.stop)
		c.log.Info("cluster stopped")
	})
	c.wg.Wait()
}

// Hosted bu süreçte çalışan düğümlerin kimlikleridir.
func (c *Cluster) Hosted() []NodeID {
	ids := make([]NodeID, len(c.hosted))
	for i, n := range c.hosted {
		ids[i] = n.id()
	}
	return ids
}

// Engine bir düğümün depolama motorunu döner (bu süreçte değilse nil).
func (c *Cluster) Engine(replica, partition int) Engine {
	if n := c.local(NodeID{replica, partition}); n != nil {
		return n.store.eng
	}
	return nil
}

func (c *Cluster) partitionOf(k Key) int { return c.cfg.Partitioner(k, c.cfg.Partitions) }

// send simüle edilmiş ağ üzerinden fn'i çalıştırır.
func (c *Cluster) send(remote bool, fn func()) {
	if remote && c.cfg.NetworkDelay > 0 {
		go func() {
			c.delay(true)
			fn()
		}()
		return
	}
	fn()
}

func (c *Cluster) delay(remote bool) {
	if !remote || c.cfg.NetworkDelay <= 0 {
		return
	}
	t := time.NewTimer(c.cfg.NetworkDelay)
	defer t.Stop()
	select {
	case <-t.C:
	case <-c.stop:
	}
}

// Submit bir işlemi kümeye gönderir ve sonucu taşıyacak kanalı döner.
// Sonuç, herhangi bir replikada işlem tamamlandığında gelir.
func (c *Cluster) Submit(req Request) (<-chan Result, error) {
	return c.submit(req, false)
}

func (c *Cluster) submit(req Request, dependent bool) (<-chan Result, error) {
	if _, ok := c.reg.lookup(req.Proc); !ok {
		return nil, fmt.Errorf("calvin: unknown procedure %q", req.Proc)
	}
	if len(req.ReadSet)+len(req.WriteSet) == 0 {
		return nil, errors.New("calvin: transaction must declare at least one key")
	}
	t := &Txn{
		Proc:      req.Proc,
		Args:      slices.Clone(req.Args),
		ReadSet:   slices.Clone(req.ReadSet),
		WriteSet:  slices.Clone(req.WriteSet),
		Dependent: dependent,
	}
	for _, k := range append(slices.Clone(t.ReadSet), t.WriteSet...) {
		if q := c.partitionOf(k); !slices.Contains(t.parts, q) {
			t.parts = append(t.parts, q)
		}
	}
	slices.Sort(t.parts)
	return c.enqueue(t, c.cfg.Replicas, false), nil
}

func (c *Cluster) enqueue(t *Txn, completions int, all bool) <-chan Result {
	n := c.hosted[c.rr.Add(1)%uint64(len(c.hosted))]
	t.ClientID = c.nextClient.Add(1)
	t.submitted = time.Now()
	t.origin = n.id()
	p := &pending{t: t, ch: make(chan Result, 1), expected: completions, all: all,
		seen: make(map[NodeID]bool)}
	c.pending.Store(t.ClientID, p)
	if !c.distributed {
		// Drain için iş birimi: işleme katılan her düğüm (replika x partition)
		// işini bitirince bir workDone çağırır. Normal işlemlerde bu, worker
		// kilitleri bıraktığında; checkpoint işaretçisinde düğüm
		// checkpoint'i yazdığında olur.
		c.inflight.Add(c.cfg.Replicas * len(t.parts))
	}
	n.seq.submit(t)
	return p.ch
}

func (c *Cluster) workDone() {
	if !c.distributed {
		c.inflight.Done()
	}
}

// complete, işlemi alan (origin) süreçte çağrılır: her replikanın ilk aktif
// katılımcısından bir kez (checkpoint için her düğümden bir kez).
func (c *Cluster) complete(clientID uint64, from NodeID, res Result) {
	v, ok := c.pending.Load(clientID)
	if !ok {
		return
	}
	p := v.(*pending)
	p.mu.Lock()
	if p.seen[from] {
		p.mu.Unlock()
		return
	}
	p.seen[from] = true
	if p.all && res.Err != nil {
		p.err = errors.Join(p.err, res.Err)
	}
	last := len(p.seen) == p.expected
	p.mu.Unlock()

	if !p.all {
		p.once.Do(func() {
			t := p.t
			lat := time.Since(t.submitted)
			c.cfg.Observer.TxnCompleted(TxnEvent{
				Seq: res.Seq, Proc: t.Proc, Partitions: len(t.parts), Dependent: t.Dependent,
				Committed: res.Committed, Err: res.Err, Latency: lat,
			})
			c.log.Debug("txn completed", "seq", res.Seq, "proc", t.Proc, "partitions", len(t.parts),
				"committed", res.Committed, "err", res.Err, "latency", lat)
			p.ch <- res
		})
	}
	if last {
		if p.all {
			p.mu.Lock()
			res.Err, res.Committed = p.err, p.err == nil
			p.mu.Unlock()
			p.ch <- res
		}
		c.pending.Delete(clientID)
	}
	if p.all {
		c.workDone()
	}
}

// Exec bir işlemi gönderir ve sonucunu bekler.
func (c *Cluster) Exec(ctx context.Context, req Request) (Result, error) {
	ch, err := c.Submit(req)
	if err != nil {
		return Result{}, err
	}
	return c.wait(ctx, ch)
}

func (c *Cluster) wait(ctx context.Context, ch <-chan Result) (Result, error) {
	select {
	case r := <-ch:
		return r, nil
	case <-ctx.Done():
		return Result{}, ctx.Err()
	case <-c.stop:
		return Result{}, ErrStopped
	}
}

// ExecDependent, okuma/yazma kümesi önceden bilinmeyen bir işlemi OLLP ile
// çalıştırır (§3.2.1): önce ucuz, düşük izolasyonlu, replike edilmeyen bir
// keşif sorgusu kümeleri bulur; işlem bu kümelerle sıraya girer. Yürütme
// anında kümeler değişmişse işlem deterministik olarak yeniden başlatılır.
func (c *Cluster) ExecDependent(ctx context.Context, proc string, args []byte, maxAttempts int) (Result, error) {
	entry, ok := c.reg.lookup(proc)
	if !ok || entry.recon == nil {
		return Result{}, fmt.Errorf("calvin: %q is not a dependent procedure", proc)
	}
	if maxAttempts <= 0 {
		maxAttempts = 10
	}
	var last Result
	for i := 0; i < maxAttempts; i++ {
		rs, ws, err := entry.recon(reconReader{c, c.hosted[0].replica}, args)
		if err != nil {
			return Result{}, err
		}
		ch, err := c.submit(Request{Proc: proc, Args: args, ReadSet: rs, WriteSet: ws}, true)
		if err != nil {
			return Result{}, err
		}
		last, err = c.wait(ctx, ch)
		if err != nil || !errors.Is(last.Err, ErrRestart) {
			return last, err
		}
	}
	return last, nil
}

// reconReader bir replikadan kilitsiz okur (düşük izolasyon).
type reconReader struct {
	c       *Cluster
	replica int
}

func (r reconReader) Read(k Key) ([]byte, bool) {
	return r.c.peer(nil, NodeID{r.replica, r.c.partitionOf(k)}).read(k)
}

// Checkpoint global sıraya bir sanal tutarlılık noktası işaretçisi ekler ve
// tüm düğümler bu noktadaki durumu kaydedene kadar bekler. İşlem yürütmesi
// bu sırada durmaz (§5, Zig-Zag varyantı). Dönen değer işaretçinin sırasıdır.
func (c *Cluster) Checkpoint(ctx context.Context) (uint64, error) {
	c.cpMu.Lock()
	defer c.cpMu.Unlock()
	t := &Txn{kind: kindCheckpoint}
	for q := 0; q < c.cfg.Partitions; q++ {
		t.parts = append(t.parts, q)
	}
	start := time.Now()
	res, err := c.wait(ctx, c.enqueue(t, c.cfg.Replicas*c.cfg.Partitions, true))
	if err != nil {
		return 0, err
	}
	if res.Err != nil {
		c.log.Error("checkpoint failed", "seq", res.Seq, "err", res.Err)
		return res.Seq, res.Err
	}
	c.log.Info("checkpoint completed", "seq", res.Seq,
		"epoch", res.Seq>>(partBits+indexBits), "duration", time.Since(start))
	return res.Seq, nil
}

// CheckpointData bir düğümün son checkpoint verisini döner.
func (c *Cluster) CheckpointData(replica, partition int) map[Key][]byte {
	if n := c.local(NodeID{replica, partition}); n != nil {
		return n.store.lastCheckpoint()
	}
	return nil
}

func (c *Cluster) writeCheckpoint(replica, partition int, seq uint64, snap map[Key][]byte) error {
	if c.cfg.CheckpointDir == "" {
		return nil
	}
	if err := os.MkdirAll(c.cfg.CheckpointDir, 0o755); err != nil {
		return err
	}
	data, err := json.Marshal(snap)
	if err != nil {
		return err
	}
	prefix := fmt.Sprintf("calvin-r%d-p%d-", replica, partition)
	name := fmt.Sprintf("%s%020d.json", prefix, seq)
	if err := os.WriteFile(filepath.Join(c.cfg.CheckpointDir, name), data, 0o644); err != nil {
		return err
	}
	// Sıra numarası sıfır dolgulu olduğundan Glob'un sözlük sırası
	// kronolojiktir; en yeni CheckpointKeep dosya dışındakiler silinir.
	old, err := filepath.Glob(filepath.Join(c.cfg.CheckpointDir, prefix+"*.json"))
	if err != nil {
		return err
	}
	for _, f := range old[:max(len(old)-c.cfg.CheckpointKeep, 0)] {
		if err := os.Remove(f); err != nil {
			return err
		}
	}
	return nil
}

// Drain, gönderilmiş tüm işlemler tüm replikalardaki tüm katılımcı
// düğümlerde tamamlanana (yazmalar uygulanıp kilitler bırakılana) kadar bekler.
// İstemciye sonuç ilk aktif katılımcı bitince döner; diğer partition'lar o an
// hâlâ yazıyor olabilir, bu yüzden tutarlı bir Snapshot için Drain gerekir.
//
// Yalnızca tek süreçli modda çalışır; dağıtık modda süreç diğer düğümlerin
// işini göremez (bkz. client.Client.WaitQuiescent).
func (c *Cluster) Drain() { c.inflight.Wait() }

// Snapshot bir replikanın bu süreçteki partition'larındaki güncel durumu döner.
// Tutarlı bir görüntü için Drain'den sonra çağrılmalıdır.
func (c *Cluster) Snapshot(replica int) map[Key][]byte {
	out := make(map[Key][]byte)
	for _, n := range c.nodes[replica] {
		if n != nil {
			n.store.scanAll(func(k Key, v []byte) { out[k] = v })
		}
	}
	return out
}

func (c *Cluster) hostedSnapshots(f func(*node) map[Key][]byte) []NodeSnapshot {
	out := make([]NodeSnapshot, 0, len(c.hosted))
	for _, n := range c.hosted {
		out = append(out, NodeSnapshot{ID: n.id(), Data: f(n)})
	}
	return out
}

// InputLog bir sequencer'ın replike edilmiş girdi logundaki batch ve işlem
// sayısını döner. Deterministik sistemde kurtarma için yalnızca bu log ve son
// checkpoint gerekir.
func (c *Cluster) InputLog(replica, partition int) (batches, txns int) {
	n := c.local(NodeID{replica, partition})
	if n == nil {
		return 0, 0
	}
	s := n.seq
	s.logMu.Lock()
	defer s.logMu.Unlock()
	for _, b := range s.log {
		txns += len(b.txns)
	}
	return len(s.log), txns
}
