package calvin

import (
	"errors"
	"slices"
)

// Bu dosyadaki tipler, süreçler arası RPC'de (net/rpc + gob) taşınan
// mesajlardır. Uygulama kodunun doğrudan kullanması gerekmez.

type Empty struct{}

type WireTxn struct {
	Seq, ClientID     uint64
	Proc              string
	Args              []byte
	ReadSet, WriteSet []Key
	Dependent         bool
	Kind              uint8
	Parts             []int
	Origin            NodeID
}

type WireBatch struct {
	To    NodeID
	Epoch uint64
	From  int
	CutAt int64
	Txns  []WireTxn
}

type SubmitArgs struct {
	To  NodeID
	Txn WireTxn
}

type PrepareArgs struct {
	To NodeID
	N  uint64
	ID int
}

type WireAccepted struct {
	N     uint64
	ID    int
	Batch WireBatch
}

type PrepareReply struct {
	OK       bool
	Accepted map[uint64]WireAccepted
}

type AcceptArgs struct {
	To    NodeID
	N     uint64
	ID    int
	Slot  uint64
	Batch WireBatch
}

type WireVal struct {
	Val   []byte
	Found bool
}

type RemoteReadArgs struct {
	To     NodeID
	Seq    uint64
	From   int
	Values map[Key]WireVal
}

type CompleteArgs struct {
	To       NodeID
	ClientID uint64
	From     NodeID
	Result   WireResult
}

type ReadArgs struct {
	To  NodeID
	Key Key
}

type ReadReply struct {
	Val   []byte
	Found bool
}

type ExecArgs struct{ Req Request }

type ExecDependentArgs struct {
	Proc        string
	Args        []byte
	MaxAttempts int
}

// NodeSnapshot bir düğümün durumudur (doğrulama için).
type NodeSnapshot struct {
	ID   NodeID
	Data map[Key][]byte
}

const (
	errNone uint8 = iota
	errAborted
	errUndeclared
	errRestart
	errStopped
	errOther
)

// WireResult, Result'ın ağ üzerinden taşınabilen biçimidir.
type WireResult struct {
	Seq       uint64
	Committed bool
	ErrKind   uint8
	ErrMsg    string
	Output    []byte
}

func ToWireResult(r Result) WireResult {
	w := WireResult{Seq: r.Seq, Committed: r.Committed, Output: r.Output}
	if r.Err != nil {
		w.ErrMsg = r.Err.Error()
		switch {
		case errors.Is(r.Err, ErrUndeclaredKey):
			w.ErrKind = errUndeclared
		case errors.Is(r.Err, ErrAborted):
			w.ErrKind = errAborted
		case errors.Is(r.Err, ErrRestart):
			w.ErrKind = errRestart
		case errors.Is(r.Err, ErrStopped):
			w.ErrKind = errStopped
		default:
			w.ErrKind = errOther
		}
	}
	return w
}

// Result, WireResult'ı geri çevirir; errors.Is(ErrAborted) vb. korunur.
func (w WireResult) Result() Result {
	r := Result{Seq: w.Seq, Committed: w.Committed, Output: w.Output}
	switch w.ErrKind {
	case errNone:
	case errAborted:
		r.Err = &remoteError{w.ErrMsg, []error{ErrAborted}}
	case errUndeclared:
		r.Err = &remoteError{w.ErrMsg, []error{ErrAborted, ErrUndeclaredKey}}
	case errRestart:
		r.Err = &remoteError{w.ErrMsg, []error{ErrRestart}}
	case errStopped:
		r.Err = &remoteError{w.ErrMsg, []error{ErrStopped}}
	default:
		r.Err = &remoteError{w.ErrMsg, nil}
	}
	return r
}

type remoteError struct {
	msg   string
	kinds []error
}

func (e *remoteError) Error() string        { return e.msg }
func (e *remoteError) Is(target error) bool { return slices.Contains(e.kinds, target) }

func toWireTxn(t *Txn) WireTxn {
	return WireTxn{
		Seq: t.Seq, ClientID: t.ClientID, Proc: t.Proc, Args: t.Args,
		ReadSet: t.ReadSet, WriteSet: t.WriteSet, Dependent: t.Dependent,
		Kind: uint8(t.kind), Parts: t.parts, Origin: t.origin,
	}
}

func fromWireTxn(w WireTxn) *Txn {
	return &Txn{
		Seq: w.Seq, ClientID: w.ClientID, Proc: w.Proc, Args: w.Args,
		ReadSet: w.ReadSet, WriteSet: w.WriteSet, Dependent: w.Dependent,
		kind: txnKind(w.Kind), parts: w.Parts, origin: w.Origin,
	}
}

func toWireBatch(to NodeID, b *batch) *WireBatch {
	w := &WireBatch{To: to, Epoch: b.epoch, From: b.from, CutAt: b.cutAt, Txns: make([]WireTxn, len(b.txns))}
	for i, t := range b.txns {
		w.Txns[i] = toWireTxn(t)
	}
	return w
}

func fromWireBatch(w *WireBatch) *batch {
	b := &batch{epoch: w.Epoch, from: w.From, cutAt: w.CutAt, txns: make([]*Txn, len(w.Txns))}
	for i, t := range w.Txns {
		b.txns[i] = fromWireTxn(t)
	}
	return b
}

func toWireVals(m map[Key]readVal) map[Key]WireVal {
	out := make(map[Key]WireVal, len(m))
	for k, v := range m {
		out[k] = WireVal{Val: v.val, Found: v.found}
	}
	return out
}

func fromWireVals(m map[Key]WireVal) map[Key]readVal {
	out := make(map[Key]readVal, len(m))
	for k, v := range m {
		out[k] = readVal{val: v.Val, found: v.Found}
	}
	return out
}
