package server_test

import (
	"io"
	"log/slog"
	"os"
	"testing"

	"github.com/INLOpen/nexusbase/config"
	"github.com/INLOpen/nexusbase/engine2"
	"github.com/INLOpen/nexusbase/server"
)

// Smoke test: construct an Engine2 + Engine2Adapter and make sure NewAppServer accepts it.
func TestAppServerAcceptsEngine2Adapter(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "nexusbase-engine2-smoke-")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	ai := setupEngineStart(t, engine2.StorageEngineOptions{DataDir: tmpDir})
	ad := ai.(*engine2.Engine2Adapter)
	defer ad.Close()

	// Build a minimal config with ports 0 so server doesn't listen on network.
	cfg := &config.Config{}
	cfg.Server.GRPCPort = 0
	cfg.Server.TCPPort = 0
	cfg.QueryServer.Enabled = false

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	appSrv, err := server.NewAppServer(ad, cfg, logger)
	if err != nil {
		t.Fatalf("NewAppServer failed with engine2 adapter: %v", err)
	}
	// No need to Start() — construction succeeded. Call Stop() to exercise cleanup path.
	appSrv.Stop()
}
