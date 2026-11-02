package calvin

import (
	"errors"
	"time"
)

// Key bir kaydın mantıksal anahtarıdır. Calvin fiziksel yapılara (sayfa,
// indeks) erişemediği için tüm kilitleme ve loglama anahtar üzerinden yapılır.
type Key = string

var (
	// ErrAborted, işlem mantığının deterministik olarak abort ettiğini
	// belirtir (örn. stok sıfırın altına düşecekse). Tüm aktif katılımcılar
	// aynı okumaları gördüğü için aynı kararı verir; 2PC gerekmez.
	ErrAborted = errors.New("calvin: transaction aborted")
	// ErrRestart, OLLP keşif sorgusunun bulduğu okuma/yazma kümesinin artık
	// geçerli olmadığını belirtir. İstemci işlemi yeniden göndermelidir.
	ErrRestart = errors.New("calvin: reconnoitered read/write set is stale")
	// ErrUndeclaredKey, işlem kodunun önceden bildirmediği bir anahtara
	// erişmeye çalıştığını belirtir.
	ErrUndeclaredKey = errors.New("calvin: access to undeclared key")
	// ErrStopped, küme durdurulduğunda bekleyen isteklere döner.
	ErrStopped = errors.New("calvin: cluster stopped")
)

// Request istemcinin gönderdiği işlem isteğidir. Calvin'in deterministik
// kilitleme protokolü, tam okuma/yazma kümelerinin önceden bilinmesini ister.
type Request struct {
	Proc     string
	Args     []byte
	ReadSet  []Key
	WriteSet []Key
}

// Result bir işlemin sonucudur.
type Result struct {
	// Seq, işlemin global seri sıradaki konumudur.
	Seq       uint64
	Committed bool
	Err       error
	Output    []byte
}

type txnKind uint8

const (
	kindNormal txnKind = iota
	kindCheckpoint
)

// Txn sequencer'dan geçmiş, global sırada yeri belirlenmiş işlem girdisidir.
// Sequence edildikten sonra değiştirilmez; tüm replikalar aynı nesneyi paylaşır.
type Txn struct {
	Seq       uint64
	ClientID  uint64
	Proc      string
	Args      []byte
	ReadSet   []Key
	WriteSet  []Key
	Dependent bool

	kind      txnKind
	parts     []int // okuma/yazma kümesindeki anahtarların partition'ları (sıralı)
	submitted time.Time
}

// batch, bir sequencer'ın bir epoch boyunca topladığı işlemlerdir.
type batch struct {
	epoch uint64
	from  int // sequencer'ın partition id'si
	txns  []*Txn
}

const (
	partBits   = 12
	indexBits  = 20
	maxBatch   = 1<<indexBits - 1
	maxParts   = 1 << partBits
	seqMaxRead = ^uint64(0)
)

// makeSeq global sırayı (epoch, sequencer, batch içi sıra) olarak kodlar.
// Scheduler'lar bir epoch'un batch'lerini sequencer id sırasıyla birleştirir;
// bu yüzden her scheduler, kendisine gelen alt kümeler üzerinden aynı göreli
// sırayı görür. (Alt kümeleri round-robin karıştırmak farklı partition'larda
// farklı göreli sıralar üretebilirdi.)
func makeSeq(epoch uint64, part, idx int) uint64 {
	return epoch<<(partBits+indexBits) | uint64(part)<<indexBits | uint64(idx)
}
