package calvin

import (
	"sync"
	"sync/atomic"
	"time"
)

// Engine, Calvin'in altına takılan işlemsel olmayan depolama motorudur
// (makaledeki CRUD arayüzü). Eşzamanlı kullanıma güvenli olmalıdır.
type Engine interface {
	Get(k Key) ([]byte, bool)
	Put(k Key, v []byte)
	Delete(k Key)
	// Scan tüm kayıtları gezer; f false dönerse durur.
	Scan(f func(k Key, v []byte) bool)
}

// Prefetcher, soğuk (diskte duran) kayıtları bilen motorların uyguladığı
// isteğe bağlı arayüzdür. Sequencer bunu kayıtları önceden ısıtmak için
// kullanır (§4).
type Prefetcher interface {
	IsHot(k Key) bool
	Prefetch(k Key)
}

// MemEngine tamamen bellekte duran basit bir anahtar-değer deposudur.
type MemEngine struct {
	mu sync.RWMutex
	m  map[Key][]byte
}

func NewMemEngine() *MemEngine { return &MemEngine{m: make(map[Key][]byte)} }

func (e *MemEngine) Get(k Key) ([]byte, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	v, ok := e.m[k]
	return v, ok
}

func (e *MemEngine) Put(k Key, v []byte) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.m[k] = v
}

func (e *MemEngine) Delete(k Key) {
	e.mu.Lock()
	defer e.mu.Unlock()
	delete(e.m, k)
}

func (e *MemEngine) Scan(f func(Key, []byte) bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	for k, v := range e.m {
		if !f(k, v) {
			return
		}
	}
}

// SimDiskEngine "soğuk" kayıtları sabit gecikmeli bir diskte tutuyormuş gibi
// davranan bir motordur. Soğuk bir kaydı okumak latency kadar bekletir ve
// kaydı belleğe ("hot") taşır. Yürütme sırasında yaşanan bu beklemeler
// Stalls() ile sayılır; bunlar kilit tutulurken yaşandığından contention
// footprint'i uzatır.
type SimDiskEngine struct {
	mu       sync.Mutex
	hot      map[Key][]byte
	cold     map[Key][]byte
	fetching map[Key]chan struct{}
	latency  time.Duration
	stalls   atomic.Int64
}

func NewSimDiskEngine(latency time.Duration) *SimDiskEngine {
	return &SimDiskEngine{
		hot:      make(map[Key][]byte),
		cold:     make(map[Key][]byte),
		fetching: make(map[Key]chan struct{}),
		latency:  latency,
	}
}

func (e *SimDiskEngine) Get(k Key) ([]byte, bool) {
	e.mu.Lock()
	if v, ok := e.hot[k]; ok {
		e.mu.Unlock()
		return v, true
	}
	_, isCold := e.cold[k]
	ch, isFetching := e.fetching[k]
	if !isCold && !isFetching {
		e.mu.Unlock()
		return nil, false
	}
	if !isFetching {
		ch = e.startFetchLocked(k)
	}
	e.mu.Unlock()
	e.stalls.Add(1)
	<-ch
	e.mu.Lock()
	defer e.mu.Unlock()
	v, ok := e.hot[k]
	return v, ok
}

func (e *SimDiskEngine) startFetchLocked(k Key) chan struct{} {
	ch := make(chan struct{})
	e.fetching[k] = ch
	go func() {
		time.Sleep(e.latency)
		e.mu.Lock()
		if v, ok := e.cold[k]; ok {
			e.hot[k] = v
			delete(e.cold, k)
		}
		delete(e.fetching, k)
		e.mu.Unlock()
		close(ch)
	}()
	return ch
}

func (e *SimDiskEngine) Put(k Key, v []byte) {
	e.mu.Lock()
	defer e.mu.Unlock()
	delete(e.cold, k)
	e.hot[k] = v
}

func (e *SimDiskEngine) Delete(k Key) {
	e.mu.Lock()
	defer e.mu.Unlock()
	delete(e.cold, k)
	delete(e.hot, k)
}

func (e *SimDiskEngine) Scan(f func(Key, []byte) bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	for k, v := range e.hot {
		if !f(k, v) {
			return
		}
	}
	for k, v := range e.cold {
		if !f(k, v) {
			return
		}
	}
}

func (e *SimDiskEngine) IsHot(k Key) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	_, cold := e.cold[k]
	return !cold
}

func (e *SimDiskEngine) Prefetch(k Key) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if _, cold := e.cold[k]; !cold {
		return
	}
	if _, ok := e.fetching[k]; !ok {
		e.startFetchLocked(k)
	}
}

// Evict bir kaydı diske (soğuğa) taşır.
func (e *SimDiskEngine) Evict(k Key) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if v, ok := e.hot[k]; ok {
		e.cold[k] = v
		delete(e.hot, k)
	}
}

// Stalls, yürütme sırasında soğuk veriye çarpıp beklemek zorunda kalan
// okuma sayısıdır.
func (e *SimDiskEngine) Stalls() int64 { return e.stalls.Load() }

// store, Engine'in üstünde Calvin'in checkpoint mantığını (§5) uygular.
// Sanal tutarlılık noktası cpSeq aktifken, global sırada bu noktadan sonra
// gelen işlemler "after" sürümlerine yazar; motorun kendisi "before"
// sürümlerini tutar ve noktadan önceki işlemler bitince değişmez hale gelir.
type store struct {
	mu    sync.RWMutex
	eng   Engine
	cpSeq uint64
	after map[Key]write
	last  map[Key][]byte
}

func newStore(eng Engine) *store { return &store{eng: eng} }

func (s *store) get(k Key, seq uint64) ([]byte, bool) {
	s.mu.RLock()
	if s.cpSeq != 0 && seq > s.cpSeq {
		if w, ok := s.after[k]; ok {
			s.mu.RUnlock()
			return w.val, !w.deleted
		}
	}
	s.mu.RUnlock()
	// Soğuk okumalar disk gecikmesi kadar sürebilir; latch'i tutmuyoruz.
	return s.eng.Get(k)
}

func (s *store) put(k Key, w write, seq uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cpSeq != 0 && seq > s.cpSeq {
		s.after[k] = w
		return
	}
	if w.deleted {
		s.eng.Delete(k)
	} else {
		s.eng.Put(k, w.val)
	}
}

func (s *store) beginCheckpoint(seq uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cpSeq = seq
	s.after = make(map[Key]write)
}

// snapshotAndMerge yalnızca noktadan önceki tüm işlemler bittikten sonra
// çağrılır: "before" sürümlerini kopyalar, ardından "after" sürümlerini motora
// uygulayıp çift sürümleri çöpe atar.
func (s *store) snapshotAndMerge() map[Key][]byte {
	snap := make(map[Key][]byte)
	s.eng.Scan(func(k Key, v []byte) bool {
		snap[k] = v
		return true
	})
	s.mu.Lock()
	defer s.mu.Unlock()
	for k, w := range s.after {
		if w.deleted {
			s.eng.Delete(k)
		} else {
			s.eng.Put(k, w.val)
		}
	}
	s.after = nil
	s.cpSeq = 0
	s.last = snap
	return snap
}

func (s *store) lastCheckpoint() map[Key][]byte {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.last
}

// scanAll, "after" sürümlerini de hesaba katarak güncel durumu gezer.
func (s *store) scanAll(f func(Key, []byte)) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	s.eng.Scan(func(k Key, v []byte) bool {
		if _, ok := s.after[k]; !ok {
			f(k, v)
		}
		return true
	})
	for k, w := range s.after {
		if !w.deleted {
			f(k, w.val)
		}
	}
}
