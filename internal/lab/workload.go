package lab

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"sync/atomic"

	"calvin"
)

// Partitioner "p:..." biçimindeki anahtarları p. partition'a yerleştirir;
// böylece iş yükleri hangi kaydın hangi makinede olduğunu kontrol eder ve
// çok partition'lı işlem oranını doğrudan ayarlayabilir.
func Partitioner(k calvin.Key, n int) int {
	i := strings.IndexByte(k, ':')
	p, err := strconv.Atoi(k[:max(i, 0)])
	if i < 0 || err != nil {
		return 0
	}
	return p % n
}

func u64(b []byte) uint64 {
	if len(b) != 8 {
		return 0
	}
	return binary.BigEndian.Uint64(b)
}

func enc(v uint64) []byte { return binary.BigEndian.AppendUint64(nil, v) }

// kind anahtarın iş yükü türünü döner ("hot", "rec", "acct", "idx").
func kind(k calvin.Key) string {
	rest := k[strings.IndexByte(k, ':')+1:]
	if i := strings.IndexByte(rest, '-'); i >= 0 {
		return rest[:i]
	}
	return rest
}

// Workload, lab'da çalışan bir iş yüküdür. Step tek bir işlem yürütür ve
// değişmez kontrolü için gereken muhasebeyi tutar; Check, yük durdurulup
// tüm replikalar boşaltıldıktan sonra bir replikanın durumunu denetler.
type Workload interface {
	Name() string
	Register(reg *calvin.Registry)
	Load(l Loader)
	Step(ctx context.Context, ex Executor, rng *rand.Rand, s Settings) (calvin.Result, error)
	Check(snap map[calvin.Key][]byte) error
	// Baseline, yük başlamadan önceki durumu değişmezlerin başlangıç noktası
	// olarak kaydeder. Dağıtık kümede istemci yeniden başlarsa küme verisi
	// korunur; değişmezler bu yüzden "başlangıç + bu istemcinin etkisi"
	// olarak kontrol edilir.
	Baseline(snap map[calvin.Key][]byte)
	SampleKey(rng *rand.Rand) calvin.Key
}

// Micro, makalenin §6.2'deki microbenchmark'ıdır: 10 kayıt okunur, kısıt
// kontrolünden geçerse her birinin sayacı artırılır. Her partition'da bir
// kayıt "hot" kümeden seçilir (çekişme indeksi = 1 / hot kayıt sayısı).
// Değişmez: tüm sayaçların toplamı = 10 x commit edilen işlem sayısı.
type Micro struct {
	Partitions int
	Records    int
	committed  atomic.Uint64
	base       uint64
}

func (w *Micro) sum(snap map[calvin.Key][]byte) (sum uint64) {
	for k, v := range snap {
		if t := kind(k); t == "hot" || t == "rec" {
			sum += u64(v)
		}
	}
	return sum
}

func (w *Micro) Baseline(snap map[calvin.Key][]byte) { w.base = w.sum(snap) }

func (w *Micro) Name() string { return "micro" }

func (w *Micro) Register(reg *calvin.Registry) {
	reg.Register("micro", func(tx *calvin.Tx) error {
		vals := make([]uint64, len(tx.WriteSet()))
		for i, k := range tx.WriteSet() {
			v, _ := tx.Read(k)
			vals[i] = u64(v)
			if vals[i] == ^uint64(0) {
				return errors.New("counter overflow")
			}
		}
		for i, k := range tx.WriteSet() {
			tx.Write(k, enc(vals[i]+1))
		}
		return nil
	})
}

func (w *Micro) Load(Loader) {} // sayaçlar 0 (yok) olarak başlar

func (w *Micro) Step(ctx context.Context, ex Executor, rng *rand.Rand, s Settings) (calvin.Result, error) {
	hot := max(int(1/s.Contention+0.5), 1)
	p := rng.Intn(w.Partitions)
	parts := []int{p}
	if w.Partitions > 1 && rng.Float64() < s.MultiPartition {
		parts = append(parts, otherPartition(rng, p, w.Partitions))
	}
	seen := map[calvin.Key]bool{}
	var keys []calvin.Key
	for _, q := range parts {
		k := fmt.Sprintf("%d:hot-%d", q, rng.Intn(hot))
		seen[k] = true
		keys = append(keys, k)
	}
	for len(keys) < 10 {
		k := fmt.Sprintf("%d:rec-%d", parts[rng.Intn(len(parts))], rng.Intn(w.Records))
		if !seen[k] {
			seen[k] = true
			keys = append(keys, k)
		}
	}
	res, err := ex.Exec(ctx, calvin.Request{Proc: "micro", ReadSet: keys, WriteSet: keys})
	if err == nil && res.Committed {
		w.committed.Add(1)
	}
	return res, err
}

func (w *Micro) Check(snap map[calvin.Key][]byte) error {
	sum := w.sum(snap)
	if want := w.base + 10*w.committed.Load(); sum != want {
		return fmt.Errorf("micro: counter sum %d, want %d", sum, want)
	}
	return nil
}

func (w *Micro) SampleKey(rng *rand.Rand) calvin.Key {
	return fmt.Sprintf("%d:rec-%d", rng.Intn(w.Partitions), rng.Intn(w.Records))
}

func otherPartition(rng *rand.Rand, p, n int) int {
	q := rng.Intn(n - 1)
	if q >= p {
		q++
	}
	return q
}

// Bank, deterministik abort'ları ve OLLP'yi (§3.2.1) deneyen bir iş yüküdür:
//   - transfer: bakiye yetmezse deterministik olarak abort eder.
//   - deposit_by_name: müşteri adı ikincil indeksinden hesap anahtarını bulan
//     bağımlı işlem (OLLP keşif sorgusu + yeniden başlatma).
//   - reindex: indeksi başka bir hesaba yönlendirir; eşzamanlı deposit'lerin
//     yeniden başlatılmasına yol açar.
//
// Değişmezler: bakiyeler toplamı = başlangıç + commit edilen deposit sayısı;
// her indeks var olan bir hesabı gösterir.
type Bank struct {
	Partitions       int
	AccountsPerPart  int
	CustomersPerPart int
	InitialBalance   uint64
	deposits         atomic.Uint64
	baseDeposits     uint64
}

// Baseline, başlangıç bakiyelerinin üzerindeki fazlayı (önceki istemcilerin
// deposit'leri) kaydeder.
func (w *Bank) Baseline(snap map[calvin.Key][]byte) {
	var sum uint64
	for k, v := range snap {
		if kind(k) == "acct" {
			sum += u64(v)
		}
	}
	w.baseDeposits = sum - uint64(w.Partitions*w.AccountsPerPart)*w.InitialBalance
}

// NewWorkloads lab'ın iki iş yükünü kurar. Dağıtık modda düğümler ve istemci
// aynı parametrelerle çağırmalıdır: düğümler başlangıç verisini, istemci
// değişmez kontrollerini bu parametrelerden türetir.
func NewWorkloads(partitions, records, accounts, customers int) (*Micro, *Bank) {
	return &Micro{Partitions: partitions, Records: records},
		&Bank{Partitions: partitions, AccountsPerPart: accounts, CustomersPerPart: customers, InitialBalance: 1000}
}

type transferArgs struct {
	From, To calvin.Key
	Amount   uint64
}

func (w *Bank) Name() string { return "bank" }

func (w *Bank) acct(i int) calvin.Key { return fmt.Sprintf("%d:acct-%d", i%w.Partitions, i) }
func (w *Bank) idx(j int) calvin.Key  { return fmt.Sprintf("%d:idx-%d", j%w.Partitions, j) }

func (w *Bank) Register(reg *calvin.Registry) {
	reg.Register("transfer", func(tx *calvin.Tx) error {
		var a transferArgs
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
		func(r calvin.Reader, args []byte) ([]calvin.Key, []calvin.Key, error) {
			idx := string(args)
			target, ok := r.Read(idx)
			if !ok {
				return nil, nil, fmt.Errorf("unknown customer %q", idx)
			}
			return []calvin.Key{idx, string(target)}, []calvin.Key{string(target)}, nil
		},
		func(tx *calvin.Tx) error {
			target, _ := tx.Read(string(tx.Args()))
			v, _ := tx.Read(string(target))
			tx.Write(string(target), enc(u64(v)+1))
			return nil
		})
	reg.Register("reindex", func(tx *calvin.Tx) error {
		tx.Write(tx.WriteSet()[0], tx.Args())
		return nil
	})
}

func (w *Bank) Load(l Loader) {
	n := w.Partitions * w.AccountsPerPart
	for i := 0; i < n; i++ {
		l.Load(w.acct(i), enc(w.InitialBalance))
	}
	rng := rand.New(rand.NewSource(42))
	for j := 0; j < w.Partitions*w.CustomersPerPart; j++ {
		l.Load(w.idx(j), []byte(w.acct(rng.Intn(n))))
	}
}

func (w *Bank) Step(ctx context.Context, ex Executor, rng *rand.Rand, s Settings) (calvin.Result, error) {
	nAcct := w.Partitions * w.AccountsPerPart
	x := rng.Float64()
	switch {
	case x < s.OLLP:
		cust := w.idx(rng.Intn(w.Partitions * w.CustomersPerPart))
		res, err := ex.ExecDependent(ctx, "deposit_by_name", []byte(cust), 20)
		if err == nil && res.Committed {
			w.deposits.Add(1)
		}
		return res, err
	case x < s.OLLP+s.Reindex:
		cust := w.idx(rng.Intn(w.Partitions * w.CustomersPerPart))
		target := w.acct(rng.Intn(nAcct))
		return ex.Exec(ctx, calvin.Request{Proc: "reindex", Args: []byte(target), WriteSet: []calvin.Key{cust}})
	default:
		from := rng.Intn(nAcct)
		p := from % w.Partitions
		q := p
		if w.Partitions > 1 && rng.Float64() < s.MultiPartition {
			q = otherPartition(rng, p, w.Partitions)
		}
		to := rng.Intn(w.AccountsPerPart)*w.Partitions + q
		if to == from {
			to = (to + w.Partitions) % nAcct
		}
		// Tutar bazen bakiyeyi aşar: deterministik abort'lar düzenli görülür.
		amount := uint64(rng.Int63n(int64(w.InitialBalance)) + 1)
		args, _ := json.Marshal(transferArgs{w.acct(from), w.acct(to), amount})
		keys := []calvin.Key{w.acct(from), w.acct(to)}
		return ex.Exec(ctx, calvin.Request{Proc: "transfer", Args: args, ReadSet: keys, WriteSet: keys})
	}
}

func (w *Bank) Check(snap map[calvin.Key][]byte) error {
	var sum uint64
	accounts := 0
	for k, v := range snap {
		if kind(k) == "acct" {
			sum += u64(v)
			accounts++
		}
	}
	n := w.Partitions * w.AccountsPerPart
	if accounts != n {
		return fmt.Errorf("bank: %d accounts, want %d", accounts, n)
	}
	if want := uint64(n)*w.InitialBalance + w.baseDeposits + w.deposits.Load(); sum != want {
		return fmt.Errorf("bank: balance sum %d, want %d", sum, want)
	}
	for k, v := range snap {
		if kind(k) == "idx" {
			if _, ok := snap[string(v)]; !ok {
				return fmt.Errorf("bank: index %s points to missing account %q", k, v)
			}
		}
	}
	return nil
}

func (w *Bank) SampleKey(rng *rand.Rand) calvin.Key {
	return w.acct(rng.Intn(w.Partitions * w.AccountsPerPart))
}
