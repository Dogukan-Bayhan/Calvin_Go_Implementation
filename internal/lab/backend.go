package lab

import (
	"context"
	"maps"

	"calvin"
	"calvin/client"
)

// Executor işlem çalıştırabilen her şeydir: tek süreçli *calvin.Cluster veya
// uzak kümeye bağlı *client.Client.
type Executor interface {
	Exec(ctx context.Context, req calvin.Request) (calvin.Result, error)
	ExecDependent(ctx context.Context, proc string, args []byte, maxAttempts int) (calvin.Result, error)
}

// Loader başlangıç verisi yükler (*calvin.Cluster yalnızca kendi düğümlerine
// düşen anahtarları yükler).
type Loader interface {
	Load(k calvin.Key, v []byte)
}

// Backend, lab'ın yük üretip doğrulama yaptığı kümedir.
type Backend interface {
	Executor
	Checkpoint(ctx context.Context) (uint64, error)
	// Quiesce, gönderilmiş tüm işlerin tüm düğümlerde bitmesini bekler.
	// Çağıran yükü durdurmuş olmalıdır.
	Quiesce(ctx context.Context) error
	// ReplicaStates her replikanın tüm partition'larının birleşik durumudur.
	ReplicaStates(ctx context.Context) ([]map[calvin.Key][]byte, error)
	// CheckpointStates her replikanın son checkpoint'inin birleşik durumudur.
	CheckpointStates(ctx context.Context) ([]map[calvin.Key][]byte, error)
}

// LocalBackend tek süreçli bir kümedir.
type LocalBackend struct {
	*calvin.Cluster
	Replicas, Partitions int
}

func (b LocalBackend) Quiesce(context.Context) error {
	b.Drain()
	return nil
}

func (b LocalBackend) ReplicaStates(context.Context) ([]map[calvin.Key][]byte, error) {
	out := make([]map[calvin.Key][]byte, b.Replicas)
	for r := range out {
		out[r] = b.Snapshot(r)
	}
	return out, nil
}

func (b LocalBackend) CheckpointStates(context.Context) ([]map[calvin.Key][]byte, error) {
	out := make([]map[calvin.Key][]byte, b.Replicas)
	for r := range out {
		out[r] = make(map[calvin.Key][]byte)
		for p := 0; p < b.Partitions; p++ {
			maps.Copy(out[r], b.CheckpointData(r, p))
		}
	}
	return out, nil
}

// RemoteBackend RPC ile bağlanılan dağıtık bir kümedir.
type RemoteBackend struct {
	*client.Client
	Replicas int
}

func (b RemoteBackend) Quiesce(ctx context.Context) error { return b.WaitQuiescent(ctx) }

func (b RemoteBackend) ReplicaStates(ctx context.Context) ([]map[calvin.Key][]byte, error) {
	snaps, err := b.Snapshots(ctx)
	if err != nil {
		return nil, err
	}
	return client.ReplicaStates(snaps, b.Replicas), nil
}

func (b RemoteBackend) CheckpointStates(ctx context.Context) ([]map[calvin.Key][]byte, error) {
	snaps, err := b.CheckpointSnapshots(ctx)
	if err != nil {
		return nil, err
	}
	return client.ReplicaStates(snaps, b.Replicas), nil
}
