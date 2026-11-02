package calvin_test

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"math/rand"
	"net"
	"sync"
	"testing"
	"time"

	calvin "github.com/Dogukan-Bayhan/Calvin_Go_Implementation"
	"github.com/Dogukan-Bayhan/Calvin_Go_Implementation/client"
)

func u64(b []byte) int64 {
	if len(b) != 8 {
		return 0
	}
	return int64(binary.BigEndian.Uint64(b))
}

func enc(v int64) []byte { return binary.BigEndian.AppendUint64(nil, uint64(v)) }

func bank() *calvin.Registry {
	reg := calvin.NewRegistry()
	type args struct {
		From, To calvin.Key
		Amount   int64
	}
	reg.Register("transfer", func(tx *calvin.Tx) error {
		var a args
		if err := json.Unmarshal(tx.Args(), &a); err != nil {
			return err
		}
		from, _ := tx.Read(a.From)
		to, _ := tx.Read(a.To)
		if u64(from) < a.Amount {
			return errors.New("insufficient funds")
		}
		tx.Write(a.From, enc(u64(from)-a.Amount))
		tx.Write(a.To, enc(u64(to)+a.Amount))
		return nil
	})
	reg.RegisterDependent("deposit_by_name",
		func(r calvin.Reader, a []byte) ([]calvin.Key, []calvin.Key, error) {
			target, _ := r.Read(string(a))
			return []calvin.Key{string(a), string(target)}, []calvin.Key{string(target)}, nil
		},
		func(tx *calvin.Tx) error {
			target, _ := tx.Read(string(tx.Args()))
			v, _ := tx.Read(string(target))
			tx.Write(string(target), enc(u64(v)+1))
			return nil
		})
	return reg
}

// startDistributed her düğümü ayrı bir Cluster nesnesinde, kendi TCP portunda
// çalıştırır; düğümler birbirleriyle yalnızca RPC üzerinden konuşur.
func startDistributed(t *testing.T, replicas, partitions int, mode calvin.ReplicationMode,
	load func(*calvin.Cluster)) []string {
	t.Helper()
	listeners := map[calvin.NodeID]net.Listener{}
	addrs := map[calvin.NodeID]string{}
	var all []string
	for r := 0; r < replicas; r++ {
		for p := 0; p < partitions; p++ {
			l, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			id := calvin.NodeID{Replica: r, Partition: p}
			listeners[id] = l
			addrs[id] = l.Addr().String()
			all = append(all, addrs[id])
		}
	}
	var wg sync.WaitGroup
	// calvin-node ile aynı başlatma: r0/p0 Start, diğerleri FetchGenesis + StartAt.
	// Düğümleri kasıtlı olarak farklı anlarda başlat; epoch'lar yine de
	// genesis'e göre hizalanmalı.
	for id, l := range listeners {
		c, err := calvin.NewCluster(calvin.Config{
			Replicas: replicas, Partitions: partitions, Replication: mode,
			EpochDuration: 5 * time.Millisecond,
			Hosted:        []calvin.NodeID{id},
			PeerAddr:      func(n calvin.NodeID) string { return addrs[n] },
		}, bank())
		if err != nil {
			t.Fatal(err)
		}
		load(c)
		go c.Serve(l)
		t.Cleanup(c.Stop)
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if err := c.WaitPeers(ctx); err != nil {
				t.Error(err)
				return
			}
			if id == (calvin.NodeID{}) {
				c.Start()
				return
			}
			time.Sleep(time.Duration(id.Partition) * 150 * time.Millisecond)
			g, err := c.FetchGenesis(ctx, calvin.NodeID{})
			if err != nil {
				t.Error(err)
				return
			}
			c.StartAt(g)
		}()
	}
	wg.Wait()
	return all
}

func TestDistributedOverRPC(t *testing.T) {
	for _, mode := range []calvin.ReplicationMode{calvin.AsyncReplication, calvin.PaxosReplication} {
		t.Run(mode.String(), func(t *testing.T) {
			const replicas, partitions, accounts, balance = 3, 3, 30, 100
			acct := func(i int) calvin.Key { return fmt.Sprintf("acct-%d", i) }
			addrs := startDistributed(t, replicas, partitions, mode, func(c *calvin.Cluster) {
				for i := 0; i < accounts; i++ {
					c.Load(acct(i), enc(balance))
				}
				c.Load("idx:alice", []byte(acct(7)))
			})
			cl := client.New(addrs)
			defer cl.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()
			if err := cl.WaitReady(ctx); err != nil {
				t.Fatal(err)
			}

			// Eşzamanlı transferler; bir kısmı bakiye yetmediği için abort eder.
			var mu sync.Mutex
			committed, aborted, slow := 0, 0, 0
			var wg sync.WaitGroup
			for w := 0; w < 16; w++ {
				wg.Add(1)
				go func(seed int64) {
					defer wg.Done()
					rng := rand.New(rand.NewSource(seed))
					for i := 0; i < 40; i++ {
						from, to := rng.Intn(accounts), rng.Intn(accounts)
						if from == to {
							continue
						}
						a, _ := json.Marshal(map[string]any{"From": acct(from), "To": acct(to), "Amount": rng.Intn(150)})
						keys := []calvin.Key{acct(from), acct(to)}
						start := time.Now()
						res, err := cl.Exec(ctx, calvin.Request{Proc: "transfer", Args: a, ReadSet: keys, WriteSet: keys})
						lat := time.Since(start)
						if err != nil {
							t.Error(err)
							return
						}
						mu.Lock()
						if lat > 200*time.Millisecond {
							slow++
						}
						if res.Committed {
							committed++
						} else if errors.Is(res.Err, calvin.ErrAborted) {
							aborted++
						} else {
							t.Errorf("unexpected result %+v", res)
						}
						mu.Unlock()
					}
				}(int64(w))
			}
			wg.Wait()
			res, err := cl.ExecDependent(ctx, "deposit_by_name", []byte("idx:alice"), 5)
			if err != nil || !res.Committed {
				t.Fatalf("OLLP over RPC: %+v %v", res, err)
			}
			// Liderlerin epoch'ları hizalı değilse (düğümler kademeli başladı),
			// erken başlayan liderin işlemleri diğerlerini bekler ve yavaşlar.
			if total := committed + aborted; slow*20 > total {
				t.Fatalf("%d of %d transactions took >200ms; leader epochs are likely skewed", slow, total)
			}
			if committed == 0 || aborted == 0 {
				t.Fatalf("want commits and aborts, got %d/%d", committed, aborted)
			}

			if _, err := cl.Checkpoint(ctx); err != nil {
				t.Fatal(err)
			}
			// Liderler her epoch'ta boş batch açmaya devam ederken sakinlik
			// tespiti tekrar tekrar ve hızlıca sonuçlanmalı (hedef epoch'u her
			// turda yeniden okuyan eski sürüm burada takılıyordu).
			for i := 0; i < 10; i++ {
				qctx, qcancel := context.WithTimeout(ctx, 3*time.Second)
				err := cl.WaitQuiescent(qctx)
				qcancel()
				if err != nil {
					t.Fatalf("WaitQuiescent #%d: %v", i, err)
				}
			}
			snaps, err := cl.Snapshots(ctx)
			if err != nil {
				t.Fatal(err)
			}
			states := client.ReplicaStates(snaps, replicas)
			var sum int64
			for k, v := range states[0] {
				if k != "idx:alice" {
					sum += u64(v)
				}
			}
			if want := int64(accounts*balance + 1); sum != want {
				t.Fatalf("total %d, want %d", sum, want)
			}
			for r := 1; r < replicas; r++ {
				if !maps.EqualFunc(states[0], states[r], func(a, b []byte) bool { return string(a) == string(b) }) {
					t.Fatalf("replica %d diverged", r)
				}
			}
			cps, err := cl.CheckpointSnapshots(ctx)
			if err != nil {
				t.Fatal(err)
			}
			cpStates := client.ReplicaStates(cps, replicas)
			for r := 1; r < replicas; r++ {
				if !maps.EqualFunc(cpStates[0], cpStates[r], func(a, b []byte) bool { return string(a) == string(b) }) {
					t.Fatalf("checkpoint of replica %d differs", r)
				}
			}
			t.Logf("committed=%d aborted=%d over %d RPC nodes", committed, aborted, len(addrs))
		})
	}
}
