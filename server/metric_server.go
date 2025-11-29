package server

import (
	"context"
	"expvar"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/pprof"
	"os"
	"sync"
	"time"

	"github.com/INLOpen/nexusbase/config"
	"github.com/arl/statsviz"
)

// MetricsServer manages the HTTP server for metrics and debugging.
type MetricsServer struct {
	server  *http.Server
	logger  *slog.Logger
	started bool
	mu      sync.Mutex
}

// NewMetricsServer creates and configures a new HTTP server.
func NewMetricsServer(cfg *config.DebugConfig, logger *slog.Logger) *MetricsServer {
	mux := http.NewServeMux()
	logger = logger.With("component", "MetricsServer")

	if cfg.PProfEnabled {
		mux.HandleFunc("/debug/pprof/", pprof.Index)
		mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
		mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
		mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
		mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
		logger.Info("pprof profiling endpoints enabled on /debug/pprof")
	}
	// Register expvar handler for metrics under /metrics
	if cfg.MetricsEnabled {
		mux.Handle("/metrics", expvar.Handler())
		logger.Info("expvar metrics endpoint enabled on /metrics")
		// Register handler for the built-in monitoring UI
		if cfg.MonitorUIEnabled {
			// The UI expects to be run from the project root.
			uiFilePath := "ui/monitor.html"
			uiMemstatPath := "ui/memstats.html"
			if _, err := os.Stat(uiFilePath); os.IsNotExist(err) {
				logger.Warn("Monitoring UI is enabled but the HTML file was not found.", "path", uiFilePath)
			} else {
				mux.HandleFunc("/monitor", func(w http.ResponseWriter, r *http.Request) {
					http.ServeFile(w, r, uiFilePath)
				})
				mux.HandleFunc("/memstats", func(w http.ResponseWriter, r *http.Request) {
					http.ServeFile(w, r, uiMemstatPath)
				})
				logger.Info("Monitoring UI is available at /monitor")
			}
		}

		_ = statsviz.Register(mux,
			statsviz.Root("/viz"),
			statsviz.SendFrequency(250*time.Millisecond),
		)

	}

	addr := cfg.ListenAddress
	if addr == "" {
		addr = ":8080"
	}

	return &MetricsServer{
		server: &http.Server{
			Addr:    addr,
			Handler: mux,
		},
		logger: logger,
	}
}

// Start starts the Metrics server. It's a blocking call.
func (s *MetricsServer) Start() error {
	s.mu.Lock()
	if s.started {
		s.mu.Unlock()
		return nil
	}
	s.started = true
	s.mu.Unlock()

	s.logger.Info("Metrics server for metrics and pprof listening", "address", s.server.Addr)
	if err := s.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		s.logger.Error("Metrics server failed", "error", err)
		return fmt.Errorf("failed to start Metrics server: %w", err)
	}

	return nil
}

// Stop gracefully shuts down the Metrics server.
func (s *MetricsServer) Stop() {
	s.mu.Lock()
	if !s.started {
		s.mu.Unlock()
		return
	}
	s.started = false
	s.mu.Unlock()

	s.logger.Info("Stopping Metrics server...")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := s.server.Shutdown(ctx); err != nil {
		s.logger.Error("Metrics server shutdown failed, attempting forced close", "error", err)
		if cerr := s.server.Close(); cerr != nil {
			s.logger.Error("Metrics server forced close failed", "error", cerr)
		} else {
			s.logger.Info("Metrics server forced closed.")
		}
	} else {
		s.logger.Info("Metrics server stopped gracefully.")
	}
}
