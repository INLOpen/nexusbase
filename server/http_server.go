package server

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/INLOpen/nexusbase/api/nbql"
	"github.com/INLOpen/nexusbase/config"
	corenbql "github.com/INLOpen/nexuscore/nbql"
)

// HTTPServer manages the HTTP server for metrics and debugging.
type HTTPServer struct {
	server  *http.Server
	logger  *slog.Logger
	started bool
	mu      sync.Mutex
}

// NewHTTPServer creates and configures a new HTTP server.
func NewHTTPServer(cfg *config.QueryServerConfig, logger *slog.Logger, executor *nbql.Executor) *HTTPServer {
	mux := http.NewServeMux()
	logger = logger.With("component", "HTTPServer")

	addr := cfg.ListenAddress
	if addr == "" {
		addr = ":8088"
	}

	mux.HandleFunc("/query", serveFile("ui/query.html", logger))
	mux.HandleFunc("/api/nbql", handleNBQLQuery(executor, logger))
	logger.Info("NBQL Query UI is available at ", addr, "/query")

	return &HTTPServer{
		server: &http.Server{
			Addr:    addr,
			Handler: mux,
		},
		logger: logger,
	}
}

// Start starts the Metrics server. It's a blocking call.
func (s *HTTPServer) Start() error {
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
func (s *HTTPServer) Stop() {
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

// serveFile is a helper to create an http.HandlerFunc that serves a static file.
func serveFile(path string, logger *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if _, err := os.Stat(path); os.IsNotExist(err) {
			logger.Warn("UI file not found.", "path", path)
			http.NotFound(w, r)
			return
		}
		http.ServeFile(w, r, path)
	}
}

// handleNBQLQuery creates an http.HandlerFunc to process NBQL queries.
func handleNBQLQuery(executor *nbql.Executor, logger *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Only POST method is allowed", http.StatusMethodNotAllowed)
			return
		}

		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "Error reading request body", http.StatusInternalServerError)
			return
		}

		var req struct {
			Query string `json:"query"`
		}
		if err := json.Unmarshal(body, &req); err != nil {
			http.Error(w, "Invalid JSON format: "+err.Error(), http.StatusBadRequest)
			return
		}

		if req.Query == "" {
			http.Error(w, "Query cannot be empty", http.StatusBadRequest)
			return
		}

		cmd, err := corenbql.Parse(req.Query)
		if err != nil {
			http.Error(w, fmt.Sprintf("Query parsing error: %v", err), http.StatusBadRequest)
			return
		}

		result, err := executor.Execute(context.Background(), cmd)
		logger.Debug("Query executed", "query", req.Query, "result", result, "error", err)
		if err != nil {
			http.Error(w, fmt.Sprintf("Query execution error: %v", err), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		// The executor returns a JSON string for queries, so we can write it directly.
		// For other commands, it returns a struct which needs to be marshaled.
		if resultStr, ok := result.(string); ok {
			w.Write([]byte(resultStr))
		} else {
			json.NewEncoder(w).Encode(result)
		}
	}
}
