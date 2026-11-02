package calvin

import (
	"fmt"
	"sync"
)

// Procedure işlem mantığıdır (makalede C++ fonksiyonları). Deterministik
// olmalıdır: aynı okumalar ve argümanlar için aynı yazmaları üretmelidir.
// Hata dönmesi işlemi deterministik olarak abort eder.
type Procedure func(tx *Tx) error

// Reader, OLLP keşif sorgularının kullandığı salt okunur arayüzdür.
type Reader interface {
	Read(k Key) ([]byte, bool)
}

// ReconFunc bağımlı bir işlemin okuma/yazma kümesini okumalar yaparak keşfeder.
type ReconFunc func(r Reader, args []byte) (readSet, writeSet []Key, err error)

type procEntry struct {
	fn    Procedure
	recon ReconFunc
}

// Registry işlemleri isimle saklar. Replikalar işlemin etkisini değil girdisini
// (isim + argüman) aldığı için kod tüm düğümlerde aynı olmalıdır.
type Registry struct {
	mu    sync.RWMutex
	procs map[string]procEntry
}

func NewRegistry() *Registry {
	return &Registry{procs: make(map[string]procEntry)}
}

// Register, okuma/yazma kümesi istemci tarafından bilinen bir işlem kaydeder.
func (r *Registry) Register(name string, fn Procedure) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.procs[name] = procEntry{fn: fn}
}

// RegisterDependent, okuma/yazma kümesi okumalara bağlı olan bir işlemi
// keşif fonksiyonuyla birlikte kaydeder (OLLP, §3.2.1).
func (r *Registry) RegisterDependent(name string, recon ReconFunc, fn Procedure) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.procs[name] = procEntry{fn: fn, recon: recon}
}

func (r *Registry) lookup(name string) (procEntry, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	e, ok := r.procs[name]
	return e, ok
}

type readVal struct {
	val   []byte
	found bool
}

type write struct {
	val     []byte
	deleted bool
}

// Tx, işlem mantığının gördüğü yürütme bağlamıdır. Okumalar yerel ve uzak
// okuma sonuçlarından gelir; yazmalar tamponlanır ve commit'te yalnızca yerel
// anahtarlar uygulanır (yerel olmayanları karşı düğüm uygular).
type Tx struct {
	txn       *Txn
	reads     map[Key]readVal
	writeSet  map[Key]bool
	writes    map[Key]write
	output    []byte
	violation error
}

func newTx(t *Txn, reads map[Key]readVal) *Tx {
	ws := make(map[Key]bool, len(t.WriteSet))
	for _, k := range t.WriteSet {
		ws[k] = true
	}
	return &Tx{txn: t, reads: reads, writeSet: ws, writes: make(map[Key]write)}
}

func (t *Tx) Args() []byte       { return t.txn.Args }
func (t *Tx) Seq() uint64        { return t.txn.Seq }
func (t *Tx) ReadSet() []Key     { return t.txn.ReadSet }
func (t *Tx) WriteSet() []Key    { return t.txn.WriteSet }
func (t *Tx) SetOutput(b []byte) { t.output = append([]byte(nil), b...) }

// Read bir anahtarın değerini döner. İşlemin kendi yazmaları önceliklidir.
// Okuma kümesinde olmayan bir anahtar okunursa işlem abort edilir.
func (t *Tx) Read(k Key) ([]byte, bool) {
	if w, ok := t.writes[k]; ok {
		return w.val, !w.deleted
	}
	rv, ok := t.reads[k]
	if !ok {
		t.fail(k)
		return nil, false
	}
	return rv.val, rv.found
}

func (t *Tx) Write(k Key, v []byte) {
	if !t.writeSet[k] {
		t.fail(k)
		return
	}
	t.writes[k] = write{val: append([]byte(nil), v...)}
}

func (t *Tx) Delete(k Key) {
	if !t.writeSet[k] {
		t.fail(k)
		return
	}
	t.writes[k] = write{deleted: true}
}

func (t *Tx) fail(k Key) {
	if t.violation == nil {
		t.violation = fmt.Errorf("%w: %q", ErrUndeclaredKey, k)
	}
}
