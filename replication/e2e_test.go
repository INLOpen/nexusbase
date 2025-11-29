package replication_test

import (
	"context"
	"log/slog"
	"net"
	"os"
	"testing"
	"time"

	"github.com/INLOpen/nexusbase/config"
	"github.com/INLOpen/nexusbase/engine2"
	"github.com/INLOpen/nexusbase/replication"
	"github.com/INLOpen/nexusbase/wal"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// replicationTestHarness holds all the components for a leader-follower test setup.
type replicationTestHarness struct {
	leaderEngine  engine2.StorageEngineInterface
	leaderCfg     *config.Config
	leaderManager *replication.Manager

	followerEngine         engine2.StorageEngineInterface
	followerCfg            *config.Config
	followerApplier        *replication.WALApplier
	followerStreamReady    chan struct{}
	leaderStreamRegistered chan struct{}
}

// setupReplicationTest creates a full leader and follower environment for integration testing.
func setupReplicationTest(t *testing.T) (*replicationTestHarness, func()) {
	t.Helper()

	// --- Leader Setup ---

	// Use a real TCP listener on a random port so the replication manager
	// and follower communicate over a real gRPC/TCP connection. This helps
	// isolate bufconn-specific harness behavior.
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	leaderAddr := lis.Addr().String()

	leaderOpts := engine2.GetBaseOptsForTest(t, "leader_")
	leaderOpts.Logger = slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug, AddSource: true}))
	leaderOpts.ReplicationMode = "leader"

	leaderCfg, err := config.LoadConfig("")
	require.NoError(t, err)
	leaderCfg.Replication.Mode = "leader"
	leaderCfg.Replication.ListenAddress = leaderAddr

	leaderEngine := setupEngineStart(t, leaderOpts)

	replicationLogger := leaderOpts.Logger.With("component", "replication")

	replicationServer := replication.NewServer(leaderEngine, replicationLogger)

	// Use the provided TCP listener so the manager will serve on that address.
	leaderManager, err := replication.NewManagerWithListener(leaderCfg.Replication, replicationServer, replicationLogger, lis)
	require.NoError(t, err)

	// --- Follower Setup ---

	followerOpts := engine2.GetBaseOptsForTest(t, "follower_")
	followerOpts.Logger = slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug, AddSource: true}))
	followerOpts.ReplicationMode = "follower"

	followerCfg, err := config.LoadConfig("")
	require.NoError(t, err)
	followerCfg.Replication.Mode = "follower"
	followerCfg.Replication.LeaderAddress = leaderAddr

	followerEngine := setupEngineStart(t, followerOpts)

	followerApplier := replication.NewWALApplier(
		followerCfg.Replication.LeaderAddress,
		followerEngine,
		followerEngine.GetSnapshotManager(),
		followerEngine.GetSnapshotsBaseDir(),
		replicationLogger.With("role", "follower-applier"),
	)

	h := &replicationTestHarness{
		leaderEngine:           leaderEngine,
		leaderCfg:              leaderCfg,
		leaderManager:          leaderManager,
		followerEngine:         followerEngine,
		followerCfg:            followerCfg,
		followerApplier:        followerApplier,
		followerStreamReady:    make(chan struct{}, 1),
		leaderStreamRegistered: make(chan struct{}, 1),
	}

	// Start servers in goroutines
	go func() { _ = h.leaderManager.Start(context.Background()) }()

	// Wait for the leader to be connectable before starting the follower.
	// This avoids race conditions in the test setup. Dial the TCP listener.
	connCtx, connCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer connCancel()
	conn, err := grpc.DialContext(connCtx, leaderAddr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	require.NoError(t, err, "could not connect to leader gRPC server in test setup")
	conn.Close()

	// Use default dial options (insecure credentials) for the WAL applier to
	// dial the leader TCP address directly.
	h.followerApplier.SetDialOptions([]grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())})

	// Wire up leader WAL testing hook so tests can wait until the WAL reader
	// registration has occurred on the leader side. Type-assert to the concrete
	// WAL implementation and set the testing channel when available.
	if lw, ok := leaderEngine.GetWAL().(*wal.WAL); ok {
		lw.TestingOnlyStreamerRegistered = h.leaderStreamRegistered
	}

	cleanup := func() {
		t.Log("Cleaning up replication test harness...")
		h.followerApplier.Stop()
		h.leaderManager.Stop()
		_ = h.followerEngine.Close()
		_ = h.leaderEngine.Close()
	}

	return h, cleanup
}

func TestReplication_HappyPath_Put(t *testing.T) {
	t.Parallel()
	h, cleanup := setupReplicationTest(t)
	defer cleanup()

	ctx := context.Background()
	// --- Test Catch-up by writing to a segment and rotating it ---
	primeTs := time.Now().UnixNano()
	primePoint := engine2.HelperDataPoint(t, "prime", map[string]string{"n": "1"}, primeTs, map[string]any{"v": 1})
	err := h.leaderEngine.Put(ctx, primePoint)
	require.NoError(t, err)

	err = h.leaderEngine.GetWAL().Rotate()
	require.NoError(t, err)

	// Now start the follower so it can catch-up from the rotated segment.
	h.followerApplier.SetTestingOnlyStreamReadyChan(h.followerStreamReady)
	h.followerApplier.Start(context.Background())

	// Wait until the leader registers a WAL stream reader for the follower.
	select {
	case <-h.leaderStreamRegistered:
		t.Log("Leader registered WAL stream reader; follower started")
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for leader WAL stream registration")
	}

	require.Eventually(t, func() bool {
		seqNum := h.followerEngine.GetLatestAppliedSeqNum()
		t.Logf("Checking follower seq num for priming point... current: %d, want >= 1", seqNum)
		return seqNum >= 1
	}, 5*time.Second, 100*time.Millisecond, "follower did not apply point from rotated WAL segment")

	// --- Test Tailing by writing to the new active segment ---
	ts := time.Now().UnixNano()
	point := engine2.HelperDataPoint(t, "cpu", map[string]string{"host": "A"}, ts, map[string]any{"value": 42.0})
	err = h.leaderEngine.Put(ctx, point)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		seqNum := h.followerEngine.GetLatestAppliedSeqNum()
		t.Logf("Checking follower seq num for tailed point... current: %d, want >= 2", seqNum)
		return seqNum >= 2
	}, 5*time.Second, 100*time.Millisecond, "follower did not apply point from active WAL segment (tailing)")

	// Verify the tailed data on the follower
	retrievedFields, err := h.followerEngine.Get(ctx, "cpu", map[string]string{"host": "A"}, ts)
	require.NoError(t, err)
	require.NotNil(t, retrievedFields)

	val, ok := retrievedFields["value"].ValueFloat64()
	require.True(t, ok)
	require.InDelta(t, 42.0, val, 1e-9)
}

func TestReplication_Tailing_Only(t *testing.T) {
	t.Parallel()
	h, cleanup := setupReplicationTest(t)
	defer cleanup()

	// Start the follower and wait until the leader registers the WAL reader
	// so the follower will receive tailed notifications.
	h.followerApplier.SetTestingOnlyStreamReadyChan(h.followerStreamReady)
	h.followerApplier.Start(context.Background())
	select {
	case <-h.leaderStreamRegistered:
		t.Log("Leader registered WAL stream reader; follower started")
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for leader WAL stream registration")
	}
	ctx := context.Background()
	ts := time.Now().UnixNano()

	// Write a point, which should be sent via the notification channel.
	point := engine2.HelperDataPoint(t, "mem", map[string]string{"host": "B"}, ts, map[string]any{"value": 12345.0})
	err := h.leaderEngine.Put(ctx, point)
	require.NoError(t, err)

	// Wait for the follower to apply the change from the notification channel
	require.Eventually(t, func() bool {
		return h.followerEngine.GetLatestAppliedSeqNum() >= 1
	}, 5*time.Second, 100*time.Millisecond, "follower did not apply tailed point")

	// Verify the data on the follower
	retrievedFields, err := h.followerEngine.Get(ctx, "mem", map[string]string{"host": "B"}, ts)
	require.NoError(t, err)
	require.NotNil(t, retrievedFields)

	val, ok := retrievedFields["value"].ValueFloat64()
	require.True(t, ok)
	require.InDelta(t, 12345.0, val, 1e-9)
}
