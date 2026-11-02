package lab

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// NewLogger stderr'e (ve file boş değilse dosyaya) JSON log yazan bir logger
// kurar; stdout komut çıktıları için temiz kalır. Süreler "12.5ms" biçiminde
// yazılır.
func NewLogger(file, level, service string) (*slog.Logger, func()) {
	var lvl slog.Level
	if err := lvl.UnmarshalText([]byte(level)); err != nil {
		log.Fatalf("invalid log level %q: %v", level, err)
	}
	var w io.Writer = os.Stderr
	closeFn := func() {}
	if file != "" {
		if err := os.MkdirAll(filepath.Dir(file), 0o755); err != nil {
			log.Fatal(err)
		}
		f, err := os.OpenFile(file, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
		if err != nil {
			log.Fatal(err)
		}
		w = io.MultiWriter(os.Stderr, f)
		closeFn = func() { f.Close() }
	}
	opts := &slog.HandlerOptions{Level: lvl, ReplaceAttr: func(_ []string, a slog.Attr) slog.Attr {
		if a.Value.Kind() == slog.KindDuration {
			a.Value = slog.StringValue(a.Value.Duration().String())
		}
		return a
	}}
	return slog.New(slog.NewJSONHandler(w, opts)).With("service", service), closeFn
}

// Every ctx bitene kadar f'i d aralıklarla çalıştırır (d <= 0 ise hiç).
func Every(ctx context.Context, d time.Duration, f func()) {
	if d <= 0 {
		return
	}
	t := time.NewTicker(d)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			f()
		}
	}
}

// MetricsMux /metrics ve /healthz uçlarını sunar.
func MetricsMux(reg *prometheus.Registry, ready func() bool) *http.ServeMux {
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		if ready != nil && !ready() {
			http.Error(w, "starting", http.StatusServiceUnavailable)
			return
		}
		io.WriteString(w, "ok\n")
	})
	return mux
}

// ControlMux, MetricsMux'a yük üreticisinin kontrol API'sini ekler:
//
//	GET  /api/settings     aktif yük ayarları
//	POST /api/settings     ayarları kısmen günceller, örn. {"clients":512}
//	POST /api/verify       hemen doğrulama çalıştırır
//	POST /api/checkpoint   hemen checkpoint alır
func ControlMux(reg *prometheus.Registry, r *Runner) *http.ServeMux {
	mux := MetricsMux(reg, nil)
	mux.HandleFunc("GET /api/settings", func(w http.ResponseWriter, _ *http.Request) {
		WriteJSON(w, http.StatusOK, r.Settings())
	})
	mux.HandleFunc("POST /api/settings", func(w http.ResponseWriter, req *http.Request) {
		s := r.Settings()
		if err := json.NewDecoder(req.Body).Decode(&s); err != nil {
			WriteJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		if err := r.Apply(s); err != nil {
			WriteJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		WriteJSON(w, http.StatusOK, s)
	})
	mux.HandleFunc("POST /api/verify", func(w http.ResponseWriter, _ *http.Request) {
		res := r.Verify()
		status := http.StatusOK
		if !res.OK() {
			status = http.StatusConflict
		}
		WriteJSON(w, status, res)
	})
	mux.HandleFunc("POST /api/checkpoint", func(w http.ResponseWriter, req *http.Request) {
		seq, consistent, err := r.Checkpoint(req.Context())
		if err != nil {
			WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		WriteJSON(w, http.StatusOK, map[string]any{"seq": seq, "replicas_consistent": consistent})
	})
	return mux
}

func WriteJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	enc.Encode(v)
}
