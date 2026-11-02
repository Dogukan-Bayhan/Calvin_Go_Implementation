package calvin

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"math/rand"
	"sync"
	"testing"
	"time"
)

func i64(b []byte) int64 {
	if len(b) != 8 {
		return 0
	}
	return int64(binary.BigEndian.Uint64(b))
}

func b64(v int64) []byte { return binary.BigEndian.AppendUint64(nil, uint64(v)) }

type transferArgs struct {
	From, To Key
	Amount   int64
}

func bankRegistry() *Registry {
	reg := NewRegistry()
	reg.Register("transfer", func(tx *Tx) error {
		var a transferArgs
		if err := json.Unmarshal(tx.Args(), &a); err != nil {
			return err
		}
		from, _ := tx.Read(a.From)
		to, _ := tx.Read(a.To)
		if i64(from) < a.Amount {
			return errors.New("insufficient funds")
		}
		tx.Write(a.From, b64(i64(from)-a.Amount))
		tx.Write(a.To, b64(i64(to)+a.Amount))
		return nil
	})
	reg.Register("balance", func(tx *Tx) error {
		v, _ := tx.Read(tx.ReadSet()[0])
		tx.SetOutput(v)
		return nil
	})
	return reg
}

func acct(i int) Key { return fmt.Sprintf("acct-%d", i) }

func newBank(t *testing.T, cfg Config, accounts int, balance int64) *Cluster {
	t.Helper()
	c, err := NewCluster(cfg, bankRegistry())
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < accounts; i++ {
		c.Load(acct(i), b64(balance))
	}
	c.Start()
	t.Cleanup(c.Stop)
	return c
}

func runTransfers(t *testing.T, c *Cluster, accounts, n int, seed int64) (committed, aborted int) {
	t.Helper()
	rng := rand.New(rand.NewSource(seed))
	var chans []<-chan Result
	for i := 0; i < n; i++ {
		from, to := rng.Intn(accounts), rng.Intn(accounts)
		for to == from {
			to = rng.Intn(accounts)
		}
		args, _ := json.Marshal(transferArgs{acct(from), acct(to), int64(rng.Intn(80))})
		keys := []Key{acct(from), acct(to)}
		ch, err := c.Submit(Request{Proc: "transfer", Args: args, ReadSet: keys, WriteSet: keys})
		if err != nil {
			t.Fatal(err)
		}
		chans = append(chans, ch)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for _, ch := range chans {
		r, err := c.wait(ctx, ch)
		if err != nil {
			t.Fatal(err)
		}
		switch {
		case r.Committed:
			committed++
		case errors.Is(r.Err, ErrAborted):
			aborted++
		default:
			t.Fatalf("unexpected result: %+v", r)
		}
	}
	c.Drain()
	return committed, aborted
}

func total(m map[Key][]byte) (sum int64) {
	for _, v := range m {
		sum += i64(v)
	}
	return sum
}

// Replikalar yalnızca girdiyi paylaşır; deterministik yürütme sayesinde aynı
// duruma ulaşmalıdırlar. Transferler para yaratmamalı/yok etmemelidir.
func TestReplicasConvergeAndAtomicity(t *testing.T) {
	for _, mode := range []ReplicationMode{AsyncReplication, PaxosReplication} {
		t.Run(fmt.Sprint("mode=", mode), func(t *testing.T) {
			const accounts, balance = 40, 100
			c := newBank(t, Config{
				Replicas: 3, Partitions: 4, Replication: mode,
				EpochDuration: 5 * time.Millisecond, NetworkDelay: time.Millisecond,
			}, accounts, balance)
			committed, aborted := runTransfers(t, c, accounts, 2000, 1)
			if committed == 0 || aborted == 0 {
				t.Fatalf("want both commits and deterministic aborts, got %d/%d", committed, aborted)
			}
			s0 := c.Snapshot(0)
			if got := total(s0); got != accounts*balance {
				t.Fatalf("total = %d, want %d", got, accounts*balance)
			}
			for r := 1; r < 3; r++ {
				if !maps.EqualFunc(s0, c.Snapshot(r), func(a, b []byte) bool { return i64(a) == i64(b) }) {
					t.Fatalf("replica %d diverged from replica 0", r)
				}
			}
			for p := 0; p < 4; p++ {
				b0, t0 := c.InputLog(0, p)
				_, t1 := c.InputLog(1, p)
				if b0 == 0 || t0 != t1 {
					t.Fatalf("input log mismatch on partition %d: %d vs %d", p, t0, t1)
				}
			}
		})
	}
}

func TestReadOnlyAndUndeclaredKey(t *testing.T) {
	c := newBank(t, Config{Replicas: 2, Partitions: 3, EpochDuration: 2 * time.Millisecond}, 4, 7)
	ctx := context.Background()
	r, err := c.Exec(ctx, Request{Proc: "balance", ReadSet: []Key{acct(1)}})
	if err != nil || !r.Committed || i64(r.Output) != 7 {
		t.Fatalf("balance: %+v %v", r, err)
	}
	// acct-2 yazma kümesinde bildirilmemiş.
	args, _ := json.Marshal(transferArgs{acct(1), acct(2), 1})
	r, err = c.Exec(ctx, Request{Proc: "transfer", Args: args, ReadSet: []Key{acct(1), acct(2)}, WriteSet: []Key{acct(1)}})
	if err != nil || !errors.Is(r.Err, ErrUndeclaredKey) {
		t.Fatalf("want ErrUndeclaredKey, got %+v %v", r, err)
	}
	c.Drain()
	if total(c.Snapshot(1)) != 28 {
		t.Fatal("aborted transaction leaked writes")
	}
}

// Checkpoint, global sıradaki işaretçideki tutarlı durumu yakalamalıdır:
// yük altında alınsa bile para toplamı korunmalı ve replikalar aynı
// görüntüyü kaydetmelidir.
func TestCheckpointUnderLoad(t *testing.T) {
	const accounts, balance = 30, 100
	c := newBank(t, Config{
		Replicas: 2, Partitions: 3, EpochDuration: 2 * time.Millisecond,
		CheckpointDir: t.TempDir(),
	}, accounts, balance)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		runTransfers(t, c, accounts, 3000, 2)
	}()
	time.Sleep(20 * time.Millisecond)
	seq, err := c.Checkpoint(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	wg.Wait()

	snaps := make([]map[Key][]byte, 2)
	for r := range snaps {
		snaps[r] = map[Key][]byte{}
		for p := 0; p < 3; p++ {
			maps.Copy(snaps[r], c.CheckpointData(r, p))
		}
	}
	if total(snaps[0]) != accounts*balance {
		t.Fatalf("checkpoint @%d not consistent: total %d", seq, total(snaps[0]))
	}
	if !maps.EqualFunc(snaps[0], snaps[1], func(a, b []byte) bool { return i64(a) == i64(b) }) {
		t.Fatal("replica checkpoints differ")
	}
	if total(c.Snapshot(0)) != accounts*balance {
		t.Fatal("live state inconsistent after checkpoint merge")
	}
}

// OLLP: ikincil indeks (isim -> hesap anahtarı) üzerinden yapılan bağımlı
// işlem. İndeks keşif ile yürütme arasında değişirse işlem yeniden başlar.
func TestOLLP(t *testing.T) {
	reg := bankRegistry()
	reg.RegisterDependent("deposit_by_name",
		func(r Reader, args []byte) ([]Key, []Key, error) {
			idx := "idx:" + string(args)
			target, _ := r.Read(idx)
			return []Key{idx, string(target)}, []Key{string(target)}, nil
		},
		func(tx *Tx) error {
			target, _ := tx.Read("idx:" + string(tx.Args()))
			v, _ := tx.Read(string(target))
			tx.Write(string(target), b64(i64(v)+1))
			return nil
		})
	reg.Register("set_index", func(tx *Tx) error {
		tx.Write(tx.WriteSet()[0], tx.Args())
		return nil
	})
	c, err := NewCluster(Config{Replicas: 2, Partitions: 4, EpochDuration: 2 * time.Millisecond}, reg)
	if err != nil {
		t.Fatal(err)
	}
	c.Load("idx:alice", []byte(acct(1)))
	c.Load(acct(1), b64(0))
	c.Load(acct(2), b64(0))
	c.Start()
	defer c.Stop()
	ctx := context.Background()

	r, err := c.ExecDependent(ctx, "deposit_by_name", []byte("alice"), 5)
	if err != nil || !r.Committed {
		t.Fatalf("dependent txn: %+v %v", r, err)
	}

	// Eski (keşfedilmiş) kümeyle gönderilen işlem, indeks değiştiği için
	// deterministik olarak yeniden başlatılmalı.
	if _, err := c.Exec(ctx, Request{Proc: "set_index", Args: []byte(acct(2)), WriteSet: []Key{"idx:alice"}}); err != nil {
		t.Fatal(err)
	}
	stale := []Key{"idx:alice", acct(1)}
	ch, _ := c.submit(Request{Proc: "deposit_by_name", Args: []byte("alice"), ReadSet: stale, WriteSet: []Key{acct(1)}}, true)
	if r, _ = c.wait(ctx, ch); !errors.Is(r.Err, ErrRestart) {
		t.Fatalf("want ErrRestart, got %+v", r)
	}

	r, err = c.ExecDependent(ctx, "deposit_by_name", []byte("alice"), 5)
	if err != nil || !r.Committed {
		t.Fatalf("dependent txn after index change: %+v %v", r, err)
	}
	c.Drain()
	s := c.Snapshot(1)
	if i64(s[acct(1)]) != 1 || i64(s[acct(2)]) != 1 {
		t.Fatalf("unexpected balances: %d %d", i64(s[acct(1)]), i64(s[acct(2)]))
	}
}

// Prefetch açıkken soğuk kayıtlar kilitler alınmadan önce ısıtılır; bu
// yüzden yürütme sırasında diske takılan okuma olmamalıdır.
func TestPrefetchAvoidsStalls(t *testing.T) {
	stalls := func(prefetch time.Duration) int64 {
		const accounts = 20
		var engines []*SimDiskEngine
		c := newBank(t, Config{
			Replicas: 2, Partitions: 2, EpochDuration: 2 * time.Millisecond,
			PrefetchDelay: prefetch,
			NewEngine: func(int, int) Engine {
				e := NewSimDiskEngine(15 * time.Millisecond)
				engines = append(engines, e)
				return e
			},
		}, accounts, 100)
		for _, e := range engines {
			for i := 0; i < accounts; i++ {
				e.Evict(acct(i))
			}
		}
		runTransfers(t, c, accounts, 40, 3)
		var n int64
		for _, e := range engines {
			n += e.Stalls()
		}
		return n
	}
	without := stalls(0)
	with := stalls(40 * time.Millisecond)
	t.Logf("stalls without prefetch=%d, with prefetch=%d", without, with)
	if without == 0 || with*4 > without {
		t.Fatalf("prefetch did not reduce stalls enough: %d vs %d", with, without)
	}
}
