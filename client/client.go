// Package client, dağıtık bir Calvin kümesine (her düğümü ayrı bir süreç
// veya pod) bağlanan Go istemcisidir. İstekler düğümler arasında round-robin
// dağıtılır; isteği alan düğüm işlemi kendi sequencer'ı üzerinden sıraya
// sokar ve sonucu döner.
package client

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/rpc"
	"sync"
	"sync/atomic"
	"time"

	"calvin"
)

// Client eşzamanlı kullanıma güvenlidir.
type Client struct {
	conns []*conn
	rr    atomic.Uint64
}

type conn struct {
	addr string
	mu   sync.Mutex
	cl   *rpc.Client
}

// New, verilen düğüm adreslerine ("host:port") bağlanacak bir istemci
// oluşturur. Bağlantılar ilk kullanımda kurulur.
func New(addrs []string) *Client {
	c := &Client{}
	for _, a := range addrs {
		c.conns = append(c.conns, &conn{addr: a})
	}
	return c
}

func (c *conn) client() (*rpc.Client, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.cl != nil {
		return c.cl, nil
	}
	nc, err := net.DialTimeout("tcp", c.addr, 3*time.Second)
	if err != nil {
		return nil, err
	}
	if tc, ok := nc.(*net.TCPConn); ok {
		tc.SetNoDelay(true)
	}
	c.cl = rpc.NewClient(nc)
	return c.cl, nil
}

func (c *conn) drop(cl *rpc.Client) {
	c.mu.Lock()
	if c.cl == cl {
		c.cl = nil
	}
	c.mu.Unlock()
	cl.Close()
}

func (c *conn) call(ctx context.Context, method string, args, reply any) error {
	cl, err := c.client()
	if err != nil {
		return fmt.Errorf("%s: %w", c.addr, err)
	}
	call := cl.Go(method, args, reply, make(chan *rpc.Call, 1))
	select {
	case <-call.Done:
		var se rpc.ServerError
		if call.Error != nil && !errors.As(call.Error, &se) {
			c.drop(cl)
		}
		if call.Error != nil {
			return fmt.Errorf("%s: %w", c.addr, call.Error)
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *Client) next() *conn { return c.conns[c.rr.Add(1)%uint64(len(c.conns))] }

// Exec bir işlemi bir düğüme gönderir ve sonucunu bekler. Bağlantı hatası
// durumunda isteğin işlenip işlenmediği bilinemez; bu yüzden otomatik
// olarak yeniden denenmez.
func (c *Client) Exec(ctx context.Context, req calvin.Request) (calvin.Result, error) {
	var w calvin.WireResult
	if err := c.next().call(ctx, "Client.Exec", &calvin.ExecArgs{Req: req}, &w); err != nil {
		return calvin.Result{}, err
	}
	return w.Result(), nil
}

// ExecDependent bir bağımlı işlemi OLLP ile çalıştırır (keşif ve yeniden
// denemeler düğüm tarafında yapılır).
func (c *Client) ExecDependent(ctx context.Context, proc string, args []byte, maxAttempts int) (calvin.Result, error) {
	var w calvin.WireResult
	a := &calvin.ExecDependentArgs{Proc: proc, Args: args, MaxAttempts: maxAttempts}
	if err := c.next().call(ctx, "Client.ExecDependent", a, &w); err != nil {
		return calvin.Result{}, err
	}
	return w.Result(), nil
}

// Checkpoint kümede bir checkpoint alır ve tüm düğümler yazana kadar bekler.
func (c *Client) Checkpoint(ctx context.Context) (uint64, error) {
	var seq uint64
	err := c.conns[0].call(ctx, "Client.Checkpoint", &calvin.Empty{}, &seq)
	return seq, err
}

// all, her düğümde aynı çağrıyı paralel yapar.
func all[T any](ctx context.Context, c *Client, method string) ([]T, error) {
	out := make([][]T, len(c.conns))
	errs := make([]error, len(c.conns))
	var wg sync.WaitGroup
	for i, cn := range c.conns {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs[i] = cn.call(ctx, method, &calvin.Empty{}, &out[i])
		}()
	}
	wg.Wait()
	if err := errors.Join(errs...); err != nil {
		return nil, err
	}
	var flat []T
	for _, o := range out {
		flat = append(flat, o...)
	}
	return flat, nil
}

// Stats tüm düğümlerin durumunu toplar.
func (c *Client) Stats(ctx context.Context) ([]calvin.NodeStats, error) {
	return all[calvin.NodeStats](ctx, c, "Client.Stats")
}

// Snapshots her düğümün güncel durumunu toplar. Tutarlı bir görüntü için
// önce yük durdurulmalı ve WaitQuiescent çağrılmalıdır.
func (c *Client) Snapshots(ctx context.Context) ([]calvin.NodeSnapshot, error) {
	return all[calvin.NodeSnapshot](ctx, c, "Client.Snapshot")
}

// CheckpointSnapshots her düğümün son checkpoint verisini toplar.
func (c *Client) CheckpointSnapshots(ctx context.Context) ([]calvin.NodeSnapshot, error) {
	return all[calvin.NodeSnapshot](ctx, c, "Client.CheckpointData")
}

// WaitReady tüm düğümler yanıt verene kadar bekler.
func (c *Client) WaitReady(ctx context.Context) error {
	for _, cn := range c.conns {
		for {
			pctx, cancel := context.WithTimeout(ctx, time.Second)
			err := cn.call(pctx, "Client.Ping", &calvin.Empty{}, &calvin.Empty{})
			cancel()
			if err == nil {
				break
			}
			select {
			case <-ctx.Done():
				return fmt.Errorf("waiting for %s: %w", cn.addr, ctx.Err())
			case <-time.After(500 * time.Millisecond):
			}
		}
	}
	return nil
}

// WaitQuiescent, istemci yükü durdurduktan (ve tüm isteklerin yanıtını
// aldıktan) sonra kümedeki tüm işlerin bitmesini bekler. İki aşamalıdır:
//  1. Liderlerde batch'lenmeyi bekleyen işlem kalmayınca liderlerin kapattığı
//     son epoch E sabitlenir. Bundan sonraki epoch'lar boştur.
//  2. Her scheduler E'ye kadar kilit istemiş ve tüm işlemlerini bitirmiş
//     olana kadar beklenir.
//
// Liderler her epoch'ta yeni (boş) bir batch açtığı için E'yi her turda yeniden
// okumak hedefi sürekli ileri kaydırır ve koşul hiç sağlanmayabilir.
func (c *Client) WaitQuiescent(ctx context.Context) error {
	var target uint64
	for {
		stats, err := c.Stats(ctx)
		if err != nil {
			return err
		}
		busy := false
		target = 0
		for _, s := range stats {
			if s.Replica == 0 {
				busy = busy || s.SequencerQueue > 0
				target = max(target, s.SequencedEpoch)
			}
		}
		if !busy {
			break
		}
		if err := pause(ctx); err != nil {
			return err
		}
	}
	for {
		stats, err := c.Stats(ctx)
		if err != nil {
			return err
		}
		done := true
		for _, s := range stats {
			if s.Inflight > 0 || s.ScheduledEpoch < target {
				done = false
				break
			}
		}
		if done {
			return nil
		}
		if err := pause(ctx); err != nil {
			return err
		}
	}
}

func pause(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(20 * time.Millisecond):
		return nil
	}
}

// ReplicaStates düğüm görüntülerini replika başına birleştirir.
func ReplicaStates(snaps []calvin.NodeSnapshot, replicas int) []map[calvin.Key][]byte {
	out := make([]map[calvin.Key][]byte, replicas)
	for i := range out {
		out[i] = make(map[calvin.Key][]byte)
	}
	for _, s := range snaps {
		for k, v := range s.Data {
			out[s.ID.Replica][k] = v
		}
	}
	return out
}

// Close tüm bağlantıları kapatır.
func (c *Client) Close() {
	for _, cn := range c.conns {
		cn.mu.Lock()
		if cn.cl != nil {
			cn.cl.Close()
			cn.cl = nil
		}
		cn.mu.Unlock()
	}
}
