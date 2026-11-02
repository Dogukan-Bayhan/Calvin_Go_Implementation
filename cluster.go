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

type Config struct {
	Replicas   int
	Partitions int
	// EpochDuration sequencer batch aralığıdır (makalede 10 ms).
	EpochDuration time.Duration
	Replication   ReplicationMode
	// NetworkDelay, düğümler arası her mesaja eklenen tek yönlü gecikmedir.
	NetworkDelay time.Duration
	// PrefetchDelay, soğuk veriye erişen işlemlerin sequencer'da bekletileceği
	// tahmini disk gecikmesidir (§4.1). 0 ise prefetch kapalıdır.
	PrefetchDelay time.Duration
	// NewEngine her (replica, partition) için depolama motoru üretir.
	// Varsayılan MemEngine'dir.
	NewEngine func(replica, partition int) Engine
	// Partitioner bir anahtarın partition'ını belirler. Varsayılan FNV hash.
	Partitioner func(k Key, partitions int) int
	// CheckpointDir boş değilse checkpoint'ler buraya JSON olarak yazılır.
	CheckpointDir string
	// Observer metrik kancalarıdır. Varsayılan NopObserver.
	Observer Observer
	// Logger yapılandırılmış log çıktısıdır. Varsayılan: log yok.
	Logger *slog.Logger
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

type pending struct {
	ch        chan Result
	remaining atomic.Int32
	all       bool // true: sonuç son tamamlanmada verilir (checkpoint)
	once      sync.Once
	mu        sync.Mutex
	err       error
}

// Cluster, replica x partition düğümlerinden oluşan bir Calvin dağıtımıdır.
type Cluster struct {
	cfg   Config
	log   *slog.Logger
	reg   *Registry
	nodes [][]*node

	nextClient atomic.Uint64
	rr         atomic.Uint64
	pending    sync.Map // clientID -> *pending
	inflight   sync.WaitGroup
	cpMu       sync.Mutex

	stop     chan struct{}
	stopOnce sync.Once
	wg       sync.WaitGroup
}

func NewCluster(cfg Config, reg *Registry) (*Cluster, error) {
	if err := cfg.setDefaults(); err != nil {
		return nil, err
	}
	c := &Cluster{cfg: cfg, log: cfg.Logger, reg: reg, stop: make(chan struct{})}
	c.nodes = make([][]*node, cfg.Replicas)
	for r := range c.nodes {
		c.nodes[r] = make([]*node, cfg.Partitions)
		for p := range c.nodes[r] {
			n := &node{c: c, replica: r, partition: p, store: newStore(cfg.NewEngine(r, p))}
			n.mbox = mailbox{size: cfg.Partitions, boxes: make(map[uint64]chan remoteRead)}
			c.nodes[r][p] = n
		}
	}
	for _, row := range c.nodes {
		for _, n := range row {
			n.seq = newSequencer(n)
			n.sched = newScheduler(n)
		}
	}
	return c, nil
}

// Load başlangıç verisini tüm replikalara doğrudan yükler. Start'tan önce
// çağrılmalıdır.
func (c *Cluster) Load(k Key, v []byte) {
	p := c.partitionOf(k)
	for r := range c.nodes {
		c.nodes[r][p].store.eng.Put(k, v)
	}
}

func (c *Cluster) Start() {
	c.log.Info("cluster started", "replicas", c.cfg.Replicas, "partitions", c.cfg.Partitions,
		"replication", c.cfg.Replication.String(), "epoch", c.cfg.EpochDuration,
		"network_delay", c.cfg.NetworkDelay, "prefetch_delay", c.cfg.PrefetchDelay)
	for _, row := range c.nodes {
		for _, n := range row {
			c.wg.Add(1)
			go n.sched.run()
			if n.replica == 0 {
				c.wg.Add(1)
				go n.seq.run()
			}
		}
	}
}

func (c *Cluster) Stop() {
	c.stopOnce.Do(func() {
		close(c.stop)
		c.log.Info("cluster stopped")
	})
	c.wg.Wait()
}

func (m ReplicationMode) String() string {
	if m == PaxosReplication {
		return "paxos"
	}
	return "async"
}

// Engine bir düğümün depolama motorunu döner (istatistik ve test için).
func (c *Cluster) Engine(replica, partition int) Engine {
	return c.nodes[replica][partition].store.eng
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
	t.ClientID = c.nextClient.Add(1)
	t.submitted = time.Now()
	p := &pending{ch: make(chan Result, 1), all: all}
	p.remaining.Store(int32(completions))
	c.pending.Store(t.ClientID, p)
	c.inflight.Add(completions)

	total := uint64(c.cfg.Replicas * c.cfg.Partitions)
	i := c.rr.Add(1) % total
	n := c.nodes[i/uint64(c.cfg.Partitions)][i%uint64(c.cfg.Partitions)]
	n.seq.submit(t)
	return p.ch
}

// complete her replikadan bir kez (checkpoint için her düğümden bir kez)
// çağrılır.
func (c *Cluster) complete(t *Txn, res Result) {
	v, ok := c.pending.Load(t.ClientID)
	if !ok {
		return
	}
	p := v.(*pending)
	if p.all {
		if res.Err != nil {
			p.mu.Lock()
			p.err = errors.Join(p.err, res.Err)
			p.mu.Unlock()
		}
	} else {
		p.once.Do(func() {
			lat := time.Since(t.submitted)
			c.cfg.Observer.TxnCompleted(TxnEvent{
				Seq: t.Seq, Proc: t.Proc, Partitions: len(t.parts), Dependent: t.Dependent,
				Committed: res.Committed, Err: res.Err, Latency: lat,
			})
			c.log.Debug("txn completed", "seq", t.Seq, "proc", t.Proc, "partitions", len(t.parts),
				"committed", res.Committed, "err", res.Err, "latency", lat)
			p.ch <- res
		})
	}
	if p.remaining.Add(-1) == 0 {
		if p.all {
			p.mu.Lock()
			res.Err, res.Committed = p.err, p.err == nil
			p.mu.Unlock()
			p.ch <- res
		}
		c.pending.Delete(t.ClientID)
	}
	c.inflight.Done()
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
		rs, ws, err := entry.recon(reconReader{c}, args)
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

// reconReader replica 0'dan kilitsiz okur (düşük izolasyon).
type reconReader struct{ c *Cluster }

func (r reconReader) Read(k Key) ([]byte, bool) {
	return r.c.nodes[0][r.c.partitionOf(k)].store.get(k, seqMaxRead)
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
	epoch := res.Seq >> (partBits + indexBits)
	for _, row := range c.nodes {
		for _, n := range row {
			n.seq.truncateLog(epoch)
		}
	}
	c.log.Info("checkpoint completed", "seq", res.Seq, "epoch", epoch, "duration", time.Since(start))
	return res.Seq, nil
}

// CheckpointData bir düğümün son checkpoint verisini döner.
func (c *Cluster) CheckpointData(replica, partition int) map[Key][]byte {
	return c.nodes[replica][partition].store.lastCheckpoint()
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
	name := fmt.Sprintf("calvin-r%d-p%d-%020d.json", replica, partition, seq)
	return os.WriteFile(filepath.Join(c.cfg.CheckpointDir, name), data, 0o644)
}

// Drain, gönderilmiş tüm işlemler tüm replikalarda tamamlanana kadar bekler.
func (c *Cluster) Drain() { c.inflight.Wait() }

// Snapshot bir replikanın tüm partition'larındaki güncel durumu döner.
// Tutarlı bir görüntü için Drain'den sonra çağrılmalıdır.
func (c *Cluster) Snapshot(replica int) map[Key][]byte {
	out := make(map[Key][]byte)
	for _, n := range c.nodes[replica] {
		n.store.scanAll(func(k Key, v []byte) { out[k] = v })
	}
	return out
}

// InputLog bir sequencer'ın replike edilmiş girdi logundaki işlem sayısını
// döner. Deterministik sistemde kurtarma için yalnızca bu log ve son
// checkpoint gerekir.
func (c *Cluster) InputLog(replica, partition int) (batches, txns int) {
	s := c.nodes[replica][partition].seq
	s.logMu.Lock()
	defer s.logMu.Unlock()
	for _, b := range s.log {
		txns += len(b.txns)
	}
	return len(s.log), txns
}
