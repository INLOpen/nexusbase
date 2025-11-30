package server

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/INLOpen/nexusbase/api/tsdb"
	"github.com/INLOpen/nexusbase/auth"
	"github.com/INLOpen/nexusbase/compressors"
	"github.com/INLOpen/nexusbase/config"
	"github.com/INLOpen/nexusbase/core"
	"github.com/INLOpen/nexusbase/engine2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"

	grpc_health_v1 "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/test/bufconn"
)

// basicAuthCreds implements credentials.PerRPCCredentials for Basic Auth in tests.
type basicAuthCreds struct {
	username string
	password string
}

func (c basicAuthCreds) GetRequestMetadata(ctx context.Context, uri ...string) (map[string]string, error) {
	auth := c.username + ":" + c.password
	enc := base64.StdEncoding.EncodeToString([]byte(auth))
	return map[string]string{"authorization": "Basic " + enc}, nil
}

func (c basicAuthCreds) RequireTransportSecurity() bool {
	return false
}

// setupE2ETestServer sets up a full application server for end-to-end testing.
// It returns the gRPC address of the server and a cleanup function to be deferred.

type e2eOptions struct {
	WithSecure bool
	WithAuth   bool
}

func setupE2ETestServer(t *testing.T) (func(ctx context.Context, opts ...grpc.DialOption) (*grpc.ClientConn, error), string, func()) {
	return setupE2ETestServerWithSecure(t, e2eOptions{})
}

// The setup functions now return a dial function that can be used to create
// a gRPC client connection over an in-memory bufconn listener, the path to
// any generated cert file (may be empty), and a cleanup function.
func setupE2ETestServerWithSecure(t *testing.T, e2eOpts e2eOptions) (func(ctx context.Context, opts ...grpc.DialOption) (*grpc.ClientConn, error), string, func()) {
	t.Helper()

	// Use an in-memory bufconn listener so tests do not require real TCP ports.
	bufSize := 1024 * 1024
	bufListener := bufconn.Listen(bufSize)
	// dialer that uses the bufconn listener
	bufDialer := func(ctx context.Context, _ string) (net.Conn, error) {
		return bufListener.Dial()
	}
	var certFile, keyFile string

	// 2. Create a temporary directory and configuration
	// Manually construct the config struct as per user feedback.
	userDBDir := t.TempDir()
	userDBPath := filepath.Join(userDBDir, "users.db")
	if e2eOpts.WithAuth {

		users, _, err := auth.ReadUserFile(userDBPath)
		if err != nil {
			t.Fatalf("Failed to read user file: %v", err)
		}

		finalHashType := auth.HashTypeBcrypt
		userAdd := []struct {
			username string
			password string
			role     string
		}{
			{
				username: "reader_user",
				password: "password123",
				role:     auth.RoleReader,
			},
			{
				username: "writer_user",
				password: "password123",
				role:     auth.RoleWriter,
			},
		}
		for _, acc := range userAdd {
			hashedPassword, err := auth.HashPassword(acc.password, finalHashType)
			if err != nil {
				t.Fatalf("Failed to hash password: %v", err)
			}

			users[acc.username] = auth.UserRecord{
				Username:     acc.username,
				PasswordHash: hashedPassword,
				Role:         acc.role,
			}

			if err := auth.WriteUserFile(userDBPath, users, finalHashType); err != nil {
				t.Fatalf("Failed to hash password: %v", err)
			}
		}
	}

	engineCfg := config.EngineConfig{
		DataDir: t.TempDir(),
		Memtable: config.MemtableConfig{
			SizeThresholdBytes: 1 * 1024 * 1024,
			FlushInterval:      "1s",
		},
		Cache: config.CacheConfig{
			BlockCacheCapacity: 1000,
		},
		Compaction: config.CompactionConfig{
			L0TriggerFileCount:     4,
			TargetSSTableSizeBytes: 2 * 1024 * 1024,
			LevelsSizeMultiplier:   5,
			MaxLevels:              7,
			CheckInterval:          "5s",
		},
		SSTable: config.SSTableConfig{
			BloomFilterFPRate: 0.01,
			BlockSizeBytes:    4 * 1024,
			Compression:       "snappy",
		},
		WAL: config.WALConfig{
			SyncMode:      "interval",
			FlushInterval: "1000ms",
		},
		MetadataSyncInterval: "10s",
	}

	serverCfg := config.ServerConfig{
		GRPCPort: 0,
		TCPPort:  0,
		TLS: config.TLSConfig{
			Enabled: false,
		},
	}

	if e2eOpts.WithSecure {
		serverCfg.TLS.Enabled = true
		certFile, keyFile = generateTestCerts(t)
		serverCfg.TLS.CertFile = certFile
		serverCfg.TLS.KeyFile = keyFile
	}

	appCfg := &config.Config{
		Server:  serverCfg,
		Engine:  engineCfg,
		Logging: config.LoggingConfig{Level: "error", Output: "stdout"},
	}

	if e2eOpts.WithAuth {
		appCfg.Security = config.SecurityConfig{Enabled: true, UserFilePath: userDBPath}
	} else {
		appCfg.Security = config.SecurityConfig{Enabled: false, UserFilePath: ""}
	}

	// 3. Create dependencies
	logger, _, err := createLogger(appCfg.Logging)
	if err != nil {
		slog.Error("Failed to create logger", "error", err)
		os.Exit(1)
	}

	// Select compressor based on config
	var sstCompressor core.Compressor
	switch appCfg.Engine.SSTable.Compression {
	case "lz4":
		sstCompressor = &compressors.LZ4Compressor{}
		logger.Info("Using LZ4 compression for SSTables.")
	case "zstd":
		sstCompressor = compressors.NewZstdCompressor()
		logger.Info("Using ZSTD compression for SSTables.")
	case "snappy":
		sstCompressor = &compressors.SnappyCompressor{}
		logger.Info("Using Snappy compression for SSTables.")
	case "none":
		sstCompressor = &compressors.NoCompressionCompressor{}
		logger.Info("Using no compression for SSTables.")
	default:
		logger.Error("Invalid sstable_compression value in config.", "value", appCfg.Engine.SSTable.Compression)
		os.Exit(1)
	}

	// Parse durations from config strings for engine options
	memtableFlushInterval := config.ParseDuration(appCfg.Engine.Memtable.FlushInterval, 0, logger)
	compactionInterval := config.ParseDuration(appCfg.Engine.Compaction.CheckInterval, 120*time.Second, logger)
	walFlushInterval := config.ParseDuration(appCfg.Engine.WAL.FlushInterval, 1000*time.Millisecond, logger)
	metadataSyncInterval := config.ParseDuration(appCfg.Engine.MetadataSyncInterval, 60*time.Second, logger)

	// Configure StorageEngine options
	opts := engine2.StorageEngineOptions{
		DataDir:                      appCfg.Engine.DataDir,
		MemtableThreshold:            appCfg.Engine.Memtable.SizeThresholdBytes,
		MemtableFlushIntervalMs:      int(memtableFlushInterval.Milliseconds()),
		BlockCacheCapacity:           appCfg.Engine.Cache.BlockCacheCapacity,
		MaxL0Files:                   appCfg.Engine.Compaction.L0TriggerFileCount,
		TargetSSTableSize:            appCfg.Engine.Compaction.TargetSSTableSizeBytes,
		LevelsTargetSizeMultiplier:   appCfg.Engine.Compaction.LevelsSizeMultiplier,
		MaxLevels:                    appCfg.Engine.Compaction.MaxLevels,
		BloomFilterFalsePositiveRate: appCfg.Engine.SSTable.BloomFilterFPRate,
		SSTableDefaultBlockSize:      int(appCfg.Engine.SSTable.BlockSizeBytes),
		CompactionIntervalSeconds:    int(compactionInterval.Seconds()),
		MetadataSyncIntervalSeconds:  int(metadataSyncInterval.Seconds()),
		Metrics:                      engine2.NewEngineMetrics(true, "engine_"), // This might need adjustment if metrics are handled differently
		SSTableCompressor:            sstCompressor,
		WALSyncMode:                  core.WALSyncMode(appCfg.Engine.WAL.SyncMode),
		WALBatchSize:                 appCfg.Engine.WAL.BatchSize,
		WALFlushIntervalMs:           int(walFlushInterval.Milliseconds()),
		RetentionPeriod:              appCfg.Engine.RetentionPeriod,
		Logger:                       logger,
	}

	eng := setupEngineStart(t, opts)

	// 4. Create and start the server using the in-memory bufconn listener
	appServer, err := NewAppServerWithListeners(eng, appCfg, logger, bufListener, nil)
	require.NoError(t, err)

	serverErrChan := make(chan error, 1)
	go func() {
		// Start() is blocking, so it runs in a goroutine.
		// We expect a net.ErrClosed error on graceful shutdown, which is not a test failure.
		if err := appServer.Start(); err != nil && !errors.Is(err, net.ErrClosed) {
			serverErrChan <- err
		}
		close(serverErrChan)
	}()

	// Wait for the server to be ready by dialing the bufconn listener.
	dialFn := func(ctx context.Context, opts ...grpc.DialOption) (*grpc.ClientConn, error) {
		// Ensure the bufDialer is used for the connection.
		opts = append(opts, grpc.WithContextDialer(bufDialer))
		return grpc.DialContext(ctx, "bufnet", opts...)
	}

	// Try a short connect to verify readiness. Use TLS creds if server is
	// configured with TLS so the handshake succeeds.
	ctxDial, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var dialOpts []grpc.DialOption
	if e2eOpts.WithSecure {
		caCert, err := os.ReadFile(certFile)
		if err != nil {
			require.NoError(t, err, "failed to read test CA cert")
		}
		certPool := x509.NewCertPool()
		require.True(t, certPool.AppendCertsFromPEM(caCert))
		tlsCreds := credentials.NewTLS(&tls.Config{ServerName: "127.0.0.1", RootCAs: certPool})
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(tlsCreds))
	} else {
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}
	dialOpts = append(dialOpts, grpc.WithBlock())
	conn, err := dialFn(ctxDial, dialOpts...)
	require.NoError(t, err, "Failed to connect to test gRPC server")
	conn.Close()

	cleanup := func() {
		appServer.Stop()
		// Check if the server goroutine exited with an unexpected error
		err, ok := <-serverErrChan
		require.True(t, ok || err == nil, "Server exited with an unexpected error: %v", err)
		// Close the bufconn listener to release resources
		_ = bufListener.Close()
	}

	return dialFn, certFile, cleanup
}

// connectWithDial is a small helper used in tests to dial the bufconn-based
// server with a short timeout and optional dial options.
func connectWithDial(dialFn func(ctx context.Context, opts ...grpc.DialOption) (*grpc.ClientConn, error), opts ...grpc.DialOption) (*grpc.ClientConn, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	// Ensure blocking dial so we wait for readiness
	opts = append(opts, grpc.WithBlock())
	return dialFn(ctx, opts...)
}

func TestE2E_PutAndQuery(t *testing.T) {
	// Test Case: E2E-001
	// Description: Write a new Data Point via gRPC `Put` and ensure it can be queried back correctly.

	dialFn, certFile, cleanup := setupE2ETestServerWithSecure(t, e2eOptions{WithSecure: true})
	defer cleanup()

	// --- Client TLS Credentials Setup ---
	caCert, err := os.ReadFile(certFile)
	require.NoError(t, err)
	certPool := x509.NewCertPool()
	require.True(t, certPool.AppendCertsFromPEM(caCert))
	tlsCreds := credentials.NewTLS(&tls.Config{
		ServerName: "127.0.0.1", // Must match IPAddress in the cert
		RootCAs:    certPool,
	})

	// --- Client Setup ---
	// Use the created TLS credentials instead of insecure ones.
	conn, err := connectWithDial(dialFn, grpc.WithTransportCredentials(tlsCreds))
	require.NoError(t, err)
	defer conn.Close()
	client := tsdb.NewTSDBServiceClient(conn)

	// --- Test Data ---
	ctx := context.Background()
	metric := "e2e.test.cpu"
	tags := map[string]string{"host": "e2e-server-01", "region": "us-west"}
	ts := time.Now().UnixNano()
	fields, err := structpb.NewStruct(map[string]interface{}{
		"usage_user":   float64(42.5),
		"usage_system": float64(10.1),
		"active":       true,
	})
	require.NoError(t, err)

	// --- 1. Put the data point ---
	putReq := &tsdb.PutRequest{
		Metric:    metric,
		Tags:      tags,
		Timestamp: ts,
		Fields:    fields,
	}
	_, err = client.Put(ctx, putReq)
	require.NoError(t, err, "Put operation should succeed")

	// --- 2. Query the data point back ---
	queryReq := &tsdb.QueryRequest{
		Metric:    metric,
		Tags:      tags,
		StartTime: ts,
		EndTime:   ts, // Query for the exact timestamp
	}

	stream, err := client.Query(ctx, queryReq)
	require.NoError(t, err, "Query operation should succeed")

	// --- 3. Validate the result ---
	// We expect exactly one result
	result, err := stream.Recv()
	require.NoError(t, err, "Should receive one result from the stream")
	require.NotNil(t, result, "Received result should not be nil")

	// Check that the data matches what we put in
	assert.Equal(t, metric, result.Metric)
	assert.Equal(t, tags, result.Tags)
	assert.Equal(t, ts, result.Timestamp)
	assert.True(t, proto.Equal(fields, result.Fields), "Fields should match")
	assert.False(t, result.IsAggregated, "Result should be a raw data point, not aggregated")

	// --- 4. Ensure the stream is empty ---
	_, err = stream.Recv()
	assert.Equal(t, io.EOF, err, "Stream should be empty after the first result")
}

func TestE2E_PutBatchAndQuery(t *testing.T) {
	// Test Case: E2E-002
	// Description: Write a batch of data points via gRPC `PutBatch` and ensure each can be queried back correctly.

	dialFn, _, cleanup := setupE2ETestServer(t)
	defer cleanup()

	// --- Client Setup ---
	conn, err := connectWithDial(dialFn, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer conn.Close()
	client := tsdb.NewTSDBServiceClient(conn)

	// --- Test Data Preparation ---
	ctx := context.Background()
	metric := "e2e.test.batch_metric"
	baseTime := time.Now()

	fields1, err := structpb.NewStruct(map[string]interface{}{"value": 101.0})
	require.NoError(t, err)
	fields2, err := structpb.NewStruct(map[string]interface{}{"value": 202.0})
	require.NoError(t, err)
	fields3, err := structpb.NewStruct(map[string]interface{}{"value": 303.0})
	require.NoError(t, err)

	// Create a batch of points across different series and timestamps
	pointsToPut := []*tsdb.PutRequest{
		{Metric: metric, Tags: map[string]string{"host": "batch-01", "dc": "us-east"}, Timestamp: baseTime.Add(1 * time.Second).UnixNano(), Fields: fields1},
		{Metric: metric, Tags: map[string]string{"host": "batch-02", "dc": "eu-west"}, Timestamp: baseTime.Add(2 * time.Second).UnixNano(), Fields: fields2},
		{Metric: metric, Tags: map[string]string{"host": "batch-01", "dc": "us-east"}, Timestamp: baseTime.Add(3 * time.Second).UnixNano(), Fields: fields3},
	}

	// --- 1. Put the batch of data points ---
	_, err = client.PutBatch(ctx, &tsdb.PutBatchRequest{Points: pointsToPut})
	require.NoError(t, err, "PutBatch operation should succeed")

	// --- 2. Query each data point back individually to verify ---
	for _, point := range pointsToPut {
		t.Run(fmt.Sprintf("Querying_ts_%d", point.Timestamp), func(t *testing.T) {
			queryReq := &tsdb.QueryRequest{
				Metric:    point.Metric,
				Tags:      point.Tags,
				StartTime: point.Timestamp,
				EndTime:   point.Timestamp,
			}

			stream, err := client.Query(ctx, queryReq)
			require.NoError(t, err, "Query operation for a batch point should succeed")

			result, err := stream.Recv()
			require.NoError(t, err, "Should receive one result from the stream for the batch point")
			require.NotNil(t, result, "Received result should not be nil")

			assert.Equal(t, point.Metric, result.Metric)
			assert.Equal(t, point.Tags, result.Tags)
			assert.Equal(t, point.Timestamp, result.Timestamp)
			assert.True(t, proto.Equal(point.Fields, result.Fields), "Fields should match for point at ts %d", point.Timestamp)
			assert.False(t, result.IsAggregated, "Result should be a raw data point")

			_, err = stream.Recv()
			assert.Equal(t, io.EOF, err, "Stream should be empty after the first result")
		})
	}
}
func TestE2E_QueryWithTagFilter(t *testing.T) {
	// Test Case: E2E-003
	// Description: Query raw data in a given time range, filtered by tags.
	// Expected Result: Receive correct data according to the filter conditions and sorted from old to new.

	dialFn, _, cleanup := setupE2ETestServer(t)
	defer cleanup()

	// --- Client Setup ---
	conn, err := connectWithDial(dialFn, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer conn.Close()
	client := tsdb.NewTSDBServiceClient(conn)

	// --- Test Data Preparation ---
	ctx := context.Background()
	metric := "e2e.test.disk_io"
	baseTime := time.Now().Truncate(time.Hour)

	// Series 1: host=server-01, device=sda1 (3 points)
	tags1 := map[string]string{"host": "server-01", "device": "sda1"}
	points1 := make([]*tsdb.PutRequest, 3)
	for i := 0; i < 3; i++ {
		ts := baseTime.Add(time.Duration(i+1) * time.Minute).UnixNano()
		fields, _ := structpb.NewStruct(map[string]interface{}{"read_ops": float64(100 + i)})
		points1[i] = &tsdb.PutRequest{Metric: metric, Tags: tags1, Timestamp: ts, Fields: fields}
	}

	// Series 2: host=server-01, device=sdb1 (2 points)
	tags2 := map[string]string{"host": "server-01", "device": "sdb1"}
	points2 := make([]*tsdb.PutRequest, 2)
	for i := 0; i < 2; i++ {
		ts := baseTime.Add(time.Duration(i+1) * time.Minute).Add(30 * time.Second).UnixNano() // Interleave timestamps
		fields, _ := structpb.NewStruct(map[string]interface{}{"read_ops": float64(200 + i)})
		points2[i] = &tsdb.PutRequest{Metric: metric, Tags: tags2, Timestamp: ts, Fields: fields}
	}

	// Series 3: host=server-02, device=sda1 (1 point, should be filtered out)
	tags3 := map[string]string{"host": "server-02", "device": "sda1"}
	ts3 := baseTime.Add(2 * time.Minute).UnixNano()
	fields3, _ := structpb.NewStruct(map[string]interface{}{"read_ops": float64(300)})
	point3 := &tsdb.PutRequest{Metric: metric, Tags: tags3, Timestamp: ts3, Fields: fields3}

	// Combine all points and write them
	allPoints := append(points1, points2...)
	allPoints = append(allPoints, point3)
	_, err = client.PutBatch(ctx, &tsdb.PutBatchRequest{Points: allPoints})
	require.NoError(t, err, "PutBatch operation should succeed")

	// --- 2. Query with tag filter ---
	queryReq := &tsdb.QueryRequest{
		Metric:    metric,
		Tags:      map[string]string{"host": "server-01"}, // Filter by this tag
		StartTime: baseTime.UnixNano(),
		EndTime:   baseTime.Add(1 * time.Hour).UnixNano(),
	}

	stream, err := client.Query(ctx, queryReq)
	require.NoError(t, err, "Query operation with tag filter should succeed")

	// --- 3. Validate the results ---
	var results []*tsdb.QueryResult
	for {
		res, err := stream.Recv()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		results = append(results, res)
	}

	// We expect 5 points (3 from series1 + 2 from series2)
	require.Len(t, results, 5, "Should receive exactly 5 data points for host=server-01")

	// Check if results are sorted by timestamp and have the correct tag
	var lastTimestamp int64 = 0
	for i, res := range results {
		assert.Equal(t, "server-01", res.Tags["host"], "All results should have host=server-01")
		assert.False(t, res.IsAggregated, "Result should be a raw data point")

		// Check sorting
		if i > 0 {
			assert.True(t, res.Timestamp > lastTimestamp, "Results should be sorted by timestamp")
		}
		lastTimestamp = res.Timestamp
	}

	// Verify the exact order and content of the interleaved points
	assert.Equal(t, points1[0].Timestamp, results[0].Timestamp)
	assert.Equal(t, points2[0].Timestamp, results[1].Timestamp)
	assert.Equal(t, points1[1].Timestamp, results[2].Timestamp)
	assert.Equal(t, points2[1].Timestamp, results[3].Timestamp)
	assert.Equal(t, points1[2].Timestamp, results[4].Timestamp)
}

func TestE2E_DownsamplingQuery(t *testing.T) {
	// Test Case: E2E-004
	// Description: Query data with downsampling and aggregation specs.
	// Expected Result: Receive data grouped into time windows with correct aggregated values.

	dialFn, _, cleanup := setupE2ETestServer(t)
	defer cleanup()

	// --- Client Setup ---
	conn, err := connectWithDial(dialFn, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer conn.Close()
	client := tsdb.NewTSDBServiceClient(conn)

	// --- Test Data ---
	ctx := context.Background()
	metric := "e2e.test.requests"
	tags := map[string]string{"service": "api-gateway", "method": "POST"}
	baseTime := time.Now().Truncate(time.Minute) // Start at a clean minute boundary

	points := []*tsdb.PutRequest{}
	// Window 1: baseTime to baseTime + 1m
	// 2 points in the first window
	for i := 0; i < 2; i++ {
		ts := baseTime.Add(time.Duration(10+i*10) * time.Second).UnixNano()                          // T+10s, T+20s
		fields, _ := structpb.NewStruct(map[string]interface{}{"latency_ms": float64(10 * (i + 1))}) // 10, 20
		points = append(points, &tsdb.PutRequest{Metric: metric, Tags: tags, Timestamp: ts, Fields: fields})
	}

	// Window 2: baseTime + 1m to baseTime + 2m
	// 3 points in the second window
	for i := 0; i < 3; i++ {
		ts := baseTime.Add(time.Minute).Add(time.Duration(15+i*10) * time.Second).UnixNano()          // T+1m+15s, T+1m+25s, T+1m+35s
		fields, _ := structpb.NewStruct(map[string]interface{}{"latency_ms": float64(100 * (i + 1))}) // 100, 200, 300
		points = append(points, &tsdb.PutRequest{Metric: metric, Tags: tags, Timestamp: ts, Fields: fields})
	}

	// --- 1. Put the data points ---
	_, err = client.PutBatch(ctx, &tsdb.PutBatchRequest{Points: points})
	require.NoError(t, err, "PutBatch operation should succeed")

	// --- 2. Query with downsampling ---
	queryReq := &tsdb.QueryRequest{
		Metric:             metric,
		Tags:               tags,
		StartTime:          baseTime.UnixNano(),
		EndTime:            baseTime.Add(2 * time.Minute).UnixNano(),
		DownsampleInterval: "1m",
		AggregationSpecs: []*tsdb.AggregationSpec{
			{Function: tsdb.AggregationSpec_COUNT, Field: "latency_ms"},
			{Function: tsdb.AggregationSpec_SUM, Field: "latency_ms"},
			{Function: tsdb.AggregationSpec_AVERAGE, Field: "latency_ms"},
			{Function: tsdb.AggregationSpec_MIN, Field: "latency_ms"},
			{Function: tsdb.AggregationSpec_MAX, Field: "latency_ms"},
		},
	}

	stream, err := client.Query(ctx, queryReq)
	require.NoError(t, err, "Downsample query operation should succeed")

	// --- 3. Validate the results ---
	var results []*tsdb.QueryResult
	for {
		res, err := stream.Recv()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		results = append(results, res)
	}

	require.Len(t, results, 2, "Should receive exactly two aggregated windows")

	// --- Validate Window 1 ---
	win1 := results[0]
	t.Log(win1)
	assert.True(t, win1.IsAggregated)
	assert.Equal(t, baseTime.UnixNano(), win1.WindowStartTime)
	assert.Equal(t, metric, win1.Metric)
	assert.Equal(t, tags, win1.Tags)

	// Expected values for window 1 (points are 10, 20)
	assert.InDelta(t, 2, win1.AggregatedValues["count_latency_ms"], 1e-9)
	assert.InDelta(t, 30, win1.AggregatedValues["sum_latency_ms"], 1e-9) // 10 + 20
	assert.InDelta(t, 15, win1.AggregatedValues["avg_latency_ms"], 1e-9) // 30 / 2
	assert.InDelta(t, 10, win1.AggregatedValues["min_latency_ms"], 1e-9)
	assert.InDelta(t, 20, win1.AggregatedValues["max_latency_ms"], 1e-9)

	// --- Validate Window 2 ---
	win2 := results[1]
	assert.True(t, win2.IsAggregated)
	assert.Equal(t, baseTime.Add(time.Minute).UnixNano(), win2.WindowStartTime)
	assert.Equal(t, metric, win2.Metric)
	assert.Equal(t, tags, win2.Tags)

	// Expected values for window 2 (points are 100, 200, 300)
	assert.InDelta(t, 3, win2.AggregatedValues["count_latency_ms"], 1e-9)
	assert.InDelta(t, 600, win2.AggregatedValues["sum_latency_ms"], 1e-9) // 100 + 200 + 300
	assert.InDelta(t, 200, win2.AggregatedValues["avg_latency_ms"], 1e-9) // 600 / 3
	assert.InDelta(t, 100, win2.AggregatedValues["min_latency_ms"], 1e-9)
	assert.InDelta(t, 300, win2.AggregatedValues["max_latency_ms"], 1e-9)
}

func TestE2E_QueryWithEmptyWindows(t *testing.T) {
	// Test Case: E2E-005
	// Description: Query with `emit_empty_windows` set to true.
	// Expected Result: Receive windows for time ranges that contain no data.

	dialFn, _, cleanup := setupE2ETestServer(t)
	defer cleanup()

	// --- Client Setup ---
	conn, err := connectWithDial(dialFn, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer conn.Close()
	client := tsdb.NewTSDBServiceClient(conn)

	// --- Test Data Preparation ---
	ctx := context.Background()
	metric := "e2e.test.network_packets"
	tags := map[string]string{"interface": "eth0"}
	baseTime := time.Now().Truncate(time.Minute)

	points := []*tsdb.PutRequest{}

	// Window 1: Has data
	ts1 := baseTime.Add(30 * time.Second).UnixNano()
	fields1, _ := structpb.NewStruct(map[string]interface{}{"packets_in": float64(100)})
	points = append(points, &tsdb.PutRequest{Metric: metric, Tags: tags, Timestamp: ts1, Fields: fields1})

	// Window 2: Is intentionally left empty.

	// Window 3: Has data
	ts3 := baseTime.Add(2 * time.Minute).Add(30 * time.Second).UnixNano()
	fields3, _ := structpb.NewStruct(map[string]interface{}{"packets_in": float64(200)})
	points = append(points, &tsdb.PutRequest{Metric: metric, Tags: tags, Timestamp: ts3, Fields: fields3})

	// --- 1. Put the data points ---
	_, err = client.PutBatch(ctx, &tsdb.PutBatchRequest{Points: points})
	require.NoError(t, err, "PutBatch operation should succeed")

	// --- 2. Query with emit_empty_windows = true ---
	queryReq := &tsdb.QueryRequest{
		Metric:             metric,
		Tags:               tags,
		StartTime:          baseTime.UnixNano(),
		EndTime:            baseTime.Add(3 * time.Minute).UnixNano(),
		DownsampleInterval: "1m",
		AggregationSpecs: []*tsdb.AggregationSpec{
			{Function: tsdb.AggregationSpec_COUNT, Field: "packets_in"},
			{Function: tsdb.AggregationSpec_SUM, Field: "packets_in"},
		},
		EmitEmptyWindows: true,
	}

	stream, err := client.Query(ctx, queryReq)
	require.NoError(t, err, "Query with empty windows should succeed")

	// --- 3. Validate the results ---
	var results []*tsdb.QueryResult
	for {
		res, err := stream.Recv()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		results = append(results, res)
	}

	require.Len(t, results, 3, "Should receive exactly three windows (one empty)")

	// --- Validate Window 1 (has data) ---
	win1 := results[0]
	assert.Equal(t, baseTime.UnixNano(), win1.WindowStartTime)
	assert.InDelta(t, 1, win1.AggregatedValues["count_packets_in"], 1e-9)
	assert.InDelta(t, 100, win1.AggregatedValues["sum_packets_in"], 1e-9)

	// --- Validate Window 2 (is empty) ---
	win2 := results[1]
	assert.Equal(t, baseTime.Add(1*time.Minute).UnixNano(), win2.WindowStartTime)
	assert.InDelta(t, 0, win2.AggregatedValues["count_packets_in"], 1e-9, "COUNT of an empty window should be 0")
	// A common convention is that the SUM of an empty set is 0.
	assert.InDelta(t, 0, win2.AggregatedValues["sum_packets_in"], 1e-9, "SUM of an empty window should be 0")

	// --- Validate Window 3 (has data) ---
	win3 := results[2]
	assert.Equal(t, baseTime.Add(2*time.Minute).UnixNano(), win3.WindowStartTime)
	assert.InDelta(t, 1, win3.AggregatedValues["count_packets_in"], 1e-9)
	assert.InDelta(t, 200, win3.AggregatedValues["sum_packets_in"], 1e-9)
}

func TestE2E_DeleteSeries(t *testing.T) {
	// Test Case: E2E-006
	// Description: Delete an entire series using DeleteSeries.
	// Expected Result: Querying the deleted series should yield no results, while other series remain unaffected.

	dialFn, _, cleanup := setupE2ETestServer(t)
	defer cleanup()

	// --- Client Setup ---
	conn, err := connectWithDial(dialFn, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer conn.Close()
	client := tsdb.NewTSDBServiceClient(conn)

	// --- Test Data Preparation ---
	ctx := context.Background()
	metric := "e2e.test.system_load"
	baseTime := time.Now()

	// Series to be deleted
	tagsToDelete := map[string]string{"host": "host-to-delete", "cluster": "A"}
	// Series to be kept
	tagsToKeep := map[string]string{"host": "host-to-keep", "cluster": "A"}

	points := []*tsdb.PutRequest{}
	for i := 0; i < 3; i++ {
		ts := baseTime.Add(time.Duration(i+1) * time.Minute).UnixNano()
		fields, _ := structpb.NewStruct(map[string]interface{}{"load1": float64(i)})
		points = append(points, &tsdb.PutRequest{Metric: metric, Tags: tagsToDelete, Timestamp: ts, Fields: fields})
		points = append(points, &tsdb.PutRequest{Metric: metric, Tags: tagsToKeep, Timestamp: ts, Fields: fields})
	}

	// --- 1. Put the data points ---
	_, err = client.PutBatch(ctx, &tsdb.PutBatchRequest{Points: points})
	require.NoError(t, err, "PutBatch operation should succeed")

	// --- 2. Delete the specific series ---
	_, err = client.DeleteSeries(ctx, &tsdb.DeleteSeriesRequest{
		Metric: metric,
		Tags:   tagsToDelete,
	})
	require.NoError(t, err, "DeleteSeries operation should succeed")

	// --- 3. Validate the deleted series ---
	t.Run("QueryDeletedSeries", func(t *testing.T) {
		queryReq := &tsdb.QueryRequest{
			Metric:    metric,
			Tags:      tagsToDelete,
			StartTime: baseTime.UnixNano(),
			EndTime:   baseTime.Add(10 * time.Minute).UnixNano(),
		}
		stream, err := client.Query(ctx, queryReq)
		require.NoError(t, err)

		// We expect no results
		_, err = stream.Recv()
		assert.Equal(t, io.EOF, err, "Stream for deleted series should be empty")
	})

	// --- 4. Validate the unaffected series ---
	t.Run("QueryUnaffectedSeries", func(t *testing.T) {
		queryReq := &tsdb.QueryRequest{
			Metric:    metric,
			Tags:      tagsToKeep,
			StartTime: baseTime.UnixNano(),
			EndTime:   baseTime.Add(10 * time.Minute).UnixNano(),
		}
		stream, err := client.Query(ctx, queryReq)
		require.NoError(t, err)

		var results []*tsdb.QueryResult
		for {
			res, err := stream.Recv()
			if err == io.EOF {
				break
			}
			require.NoError(t, err)
			results = append(results, res)
		}
		// We expect all 3 points to still be there
		require.Len(t, results, 3, "Unaffected series should still have all its data points")
	})
}

func TestE2E_DeleteByTimeRange(t *testing.T) {
	// Test Case: E2E-007
	// Description: Delete data within a specific time range.
	// Expected Result: Data within the range is deleted, data outside the range remains.

	dialFn, _, cleanup := setupE2ETestServer(t)
	defer cleanup()

	// --- Client Setup ---
	conn, err := connectWithDial(dialFn, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer conn.Close()
	client := tsdb.NewTSDBServiceClient(conn)

	// --- Test Data Preparation ---
	ctx := context.Background()
	metric := "e2e.test.sensor_reading"
	tags := map[string]string{"sensor_id": "temp-001"}
	baseTime := time.Now().Truncate(time.Hour)

	points := []*tsdb.PutRequest{}
	expectedTimestampsAfterDelete := make(map[int64]bool)

	for i := 1; i <= 5; i++ {
		ts := baseTime.Add(time.Duration(i) * time.Minute).UnixNano()
		fields, _ := structpb.NewStruct(map[string]interface{}{"temperature": float64(20 + i)})
		points = append(points, &tsdb.PutRequest{Metric: metric, Tags: tags, Timestamp: ts, Fields: fields})
		// We will delete points at T+2m, T+3m, T+4m
		if i == 1 || i == 5 {
			expectedTimestampsAfterDelete[ts] = true
		}
	}

	// --- 1. Put the data points ---
	_, err = client.PutBatch(ctx, &tsdb.PutBatchRequest{Points: points})
	require.NoError(t, err, "PutBatch operation should succeed")

	// --- 2. Delete the time range ---
	// Delete from T+1m30s to T+4m30s, which should cover points at 2, 3, and 4 minutes.
	startTimeToDelete := baseTime.Add(1 * time.Minute).Add(30 * time.Second).UnixNano()
	endTimeToDelete := baseTime.Add(4 * time.Minute).Add(30 * time.Second).UnixNano()

	_, err = client.DeletesByTimeRange(ctx, &tsdb.DeletesByTimeRangeRequest{
		Metric:    metric,
		Tags:      tags,
		StartTime: startTimeToDelete,
		EndTime:   endTimeToDelete,
	})
	require.NoError(t, err, "DeletesByTimeRange operation should succeed")

	// --- 3. Validate the results ---
	queryReq := &tsdb.QueryRequest{
		Metric:    metric,
		Tags:      tags,
		StartTime: baseTime.UnixNano(),
		EndTime:   baseTime.Add(10 * time.Minute).UnixNano(),
	}
	stream, err := client.Query(ctx, queryReq)
	require.NoError(t, err)

	var results []*tsdb.QueryResult
	for {
		res, err := stream.Recv()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		results = append(results, res)
	}

	// We expect only 2 points to remain (at T+1m and T+5m)
	require.Len(t, results, 2, "Should have exactly 2 points remaining after range deletion")

	// Check that the remaining points are the correct ones
	for _, res := range results {
		_, ok := expectedTimestampsAfterDelete[res.Timestamp]
		assert.True(t, ok, "Found an unexpected timestamp %d which should have been deleted", res.Timestamp)
	}
}

func TestE2E_DeleteAtPointInTime(t *testing.T) {
	// Test Case: E2E-012
	// Description: Delete a single data point at a specific timestamp, simulating `REMOVE ... AT ...`.
	// Expected Result: The specific data point is deleted, while others in the same series remain.

	dialFn, _, cleanup := setupE2ETestServer(t)
	defer cleanup()

	// --- Client Setup ---
	conn, err := connectWithDial(dialFn, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer conn.Close()
	client := tsdb.NewTSDBServiceClient(conn)

	// --- Test Data Preparation ---
	ctx := context.Background()
	metric := "e2e.test.events"
	tags := map[string]string{"event_type": "login"}
	baseTime := time.Now().Truncate(time.Hour)

	points := make([]*tsdb.PutRequest, 3)
	timestamps := []int64{
		baseTime.Add(1 * time.Minute).UnixNano(),
		baseTime.Add(2 * time.Minute).UnixNano(),
		baseTime.Add(3 * time.Minute).UnixNano(),
	}
	timestampToDelete := timestamps[1] // Delete the middle point

	for i, ts := range timestamps {
		fields, _ := structpb.NewStruct(map[string]interface{}{"user_id": float64(100 + i)})
		points[i] = &tsdb.PutRequest{Metric: metric, Tags: tags, Timestamp: ts, Fields: fields}
	}

	// --- 1. Put the data points ---
	_, err = client.PutBatch(ctx, &tsdb.PutBatchRequest{Points: points})
	require.NoError(t, err, "PutBatch operation should succeed")

	// --- 2. Delete the single point in time ---
	// This simulates `REMOVE FROM "e2e.test.events" TAGGED (...) AT <timestampToDelete>`
	_, err = client.DeletesByTimeRange(ctx, &tsdb.DeletesByTimeRangeRequest{
		Metric:    metric,
		Tags:      tags,
		StartTime: timestampToDelete,
		EndTime:   timestampToDelete, // Start and End are the same for a single point
	})
	require.NoError(t, err, "DeletesByTimeRange for a single point should succeed")

	// --- 3. Validate the results ---
	queryReq := &tsdb.QueryRequest{
		Metric:    metric,
		Tags:      tags,
		StartTime: baseTime.UnixNano(),
		EndTime:   baseTime.Add(10 * time.Minute).UnixNano(),
	}
	stream, err := client.Query(ctx, queryReq)
	require.NoError(t, err)

	results := readAllQueryResults(t, stream)

	// We expect 2 points to remain (at T+1m and T+3m)
	require.Len(t, results, 2, "Should have exactly 2 points remaining after point-in-time deletion")

	// Check that the remaining points are the correct ones
	assert.Equal(t, timestamps[0], results[0].Timestamp, "Point at T+1m should remain")
	assert.Equal(t, timestamps[2], results[1].Timestamp, "Point at T+3m should remain")
}

func TestE2E_GetSeriesByTags(t *testing.T) {
	// Test Case: E2E-008
	// Description: GetSeriesByTags by using Metric and Tags.
	// Expected Result: Shows a list of all Series Keys that match the criteria.

	dialFn, _, cleanup := setupE2ETestServer(t)
	defer cleanup()

	// --- Client Setup ---
	conn, err := connectWithDial(dialFn, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer conn.Close()
	client := tsdb.NewTSDBServiceClient(conn)

	// --- Test Data Preparation ---
	ctx := context.Background()
	baseTime := time.Now()
	fields, _ := structpb.NewStruct(map[string]interface{}{"value": 1.0})

	pointsToPut := []*tsdb.PutRequest{
		{Metric: "cpu", Tags: map[string]string{"host": "server1", "region": "us-east"}, Timestamp: baseTime.UnixNano(), Fields: fields},
		{Metric: "cpu", Tags: map[string]string{"host": "server2", "region": "us-east"}, Timestamp: baseTime.UnixNano(), Fields: fields},
		{Metric: "mem", Tags: map[string]string{"host": "server1", "region": "us-east"}, Timestamp: baseTime.UnixNano(), Fields: fields}, // Different metric
		{Metric: "cpu", Tags: map[string]string{"host": "server3", "region": "us-west"}, Timestamp: baseTime.UnixNano(), Fields: fields}, // Different region
	}
	_, err = client.PutBatch(ctx, &tsdb.PutBatchRequest{Points: pointsToPut})
	require.NoError(t, err)

	// --- 1. Query for series ---
	resp, err := client.GetSeriesByTags(ctx, &tsdb.GetSeriesByTagsRequest{
		Metric: "cpu",
		Tags:   map[string]string{"region": "us-east"},
	})
	require.NoError(t, err, "GetSeriesByTags should succeed")

	// --- 2. Validate results ---
	require.NotNil(t, resp)
	seriesKeys := resp.GetSeriesKeys()
	require.Len(t, seriesKeys, 2, "Should find exactly 2 series")

	// Sort for deterministic comparison
	sort.Strings(seriesKeys)

	// The exact string format depends on core.EncodeSeriesKeyWithString
	// Assuming a format like "metric,key1=val1,key2=val2"
	expectedKey1 := string(core.EncodeSeriesKeyWithString("cpu", map[string]string{"host": "server1", "region": "us-east"}))
	expectedKey2 := string(core.EncodeSeriesKeyWithString("cpu", map[string]string{"host": "server2", "region": "us-east"}))

	assert.Equal(t, expectedKey1, seriesKeys[0])
	assert.Equal(t, expectedKey2, seriesKeys[1])
}

func TestE2E_Subscribe(t *testing.T) {
	// Test Case: E2E-009
	// Description: Subscribe to track a specific Metric.
	// Expected Result: Client receives real-time DataPointUpdate when a Put or Delete occurs.

	dialFn, _, cleanup := setupE2ETestServer(t)
	defer cleanup()

	// --- Client Setup ---
	conn, err := connectWithDial(dialFn, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer conn.Close()
	client := tsdb.NewTSDBServiceClient(conn)

	// --- Test Data ---
	metric := "e2e.realtime.metric"
	tags := map[string]string{"source": "test"}
	ts := time.Now().UnixNano()
	fields, _ := structpb.NewStruct(map[string]interface{}{"value": 99.9})

	// --- 1. Start subscription in a goroutine ---
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel() // Ensure subscription goroutine is cleaned up

	updateChan := make(chan *tsdb.DataPointUpdate, 2) // Buffer for 2 updates (put, delete)

	go func() {
		stream, err := client.Subscribe(ctx, &tsdb.SubscribeRequest{Metric: metric})
		if err != nil {
			t.Logf("Subscription failed: %v", err)
			return
		}
		for {
			update, err := stream.Recv()
			if err != nil {
				// If context is cancelled, this is expected.
				if status.Code(err) == codes.Canceled || err == io.EOF {
					return
				}
				t.Logf("Error receiving update: %v", err)
				return
			}
			updateChan <- update
		}
	}()

	// Give the subscription a moment to establish
	time.Sleep(100 * time.Millisecond)

	// --- 2. Perform a Put and validate the update ---
	_, err = client.Put(ctx, &tsdb.PutRequest{Metric: metric, Tags: tags, Timestamp: ts, Fields: fields})
	require.NoError(t, err)

	select {
	case update := <-updateChan:
		assert.Equal(t, tsdb.DataPointUpdate_PUT, update.UpdateType)
		assert.Equal(t, metric, update.Metric)
		assert.Equal(t, tags, update.Tags)
		assert.Equal(t, ts, update.Timestamp)
	case <-time.After(2 * time.Second):
		t.Fatal("Timed out waiting for PUT update from subscription")
	}
}

func TestE2E_SecurityPermissionDenied(t *testing.T) {
	// Test Case: E2E-010
	// Description: Call an API requiring writer permission with a user having only reader permission.
	// Expected Result: Receive gRPC status PermissionDenied.

	dialFn, _, cleanup := setupE2ETestServerWithSecure(t, e2eOptions{
		WithAuth: true,
	})
	defer cleanup()

	// --- Client Setup with Reader User ---
	readerCreds := basicAuthCreds{username: "reader_user", password: "password123"}
	conn, err := connectWithDial(dialFn, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithPerRPCCredentials(readerCreds))
	require.NoError(t, err)
	defer conn.Close()
	client := tsdb.NewTSDBServiceClient(conn)

	// --- Attempt to perform a write operation ---
	_, err = client.Put(context.Background(), &tsdb.PutRequest{Metric: "test"})

	// --- Validate the error ---
	t.Log(err)
	require.Error(t, err, "Write operation with reader role should fail")
	st, ok := status.FromError(err)
	require.True(t, ok, "Error should be a gRPC status error")
	assert.Equal(t, codes.PermissionDenied, st.Code(), "Expected PermissionDenied error code")
}

func TestE2E_Validation(t *testing.T) {
	// Test Case: E2E-011
	// Description: Put data with an invalid Metric or Tag Key.
	// Expected Result: Receive gRPC status InvalidArgument.

	dialFn, _, cleanup := setupE2ETestServer(t)
	defer cleanup()

	// --- Client Setup ---
	conn, err := connectWithDial(dialFn, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer conn.Close()
	client := tsdb.NewTSDBServiceClient(conn)

	validFields, err := structpb.NewStruct(map[string]interface{}{"value": 1.0})
	require.NoError(t, err)

	testCases := []struct {
		name    string
		req     *tsdb.PutRequest
		wantErr codes.Code
	}{
		{
			name:    "Invalid Metric Name",
			req:     &tsdb.PutRequest{Metric: "", Fields: validFields},
			wantErr: codes.InvalidArgument,
		},
		{
			name:    "Invalid Tag Key",
			req:     &tsdb.PutRequest{Metric: "valid_metric", Tags: map[string]string{"__tag": "value"}, Fields: validFields},
			wantErr: codes.InvalidArgument,
		},
		{
			name:    "Valid Request",
			req:     &tsdb.PutRequest{Metric: "valid_metric", Tags: map[string]string{"valid_tag": "value"}, Timestamp: time.Now().UnixNano(), Fields: validFields},
			wantErr: codes.OK,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := client.Put(context.Background(), tc.req)
			if tc.wantErr == codes.OK {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				st, ok := status.FromError(err)
				require.True(t, ok)
				assert.Equal(t, tc.wantErr, st.Code())
			}
		})
	}
}

// createLogger creates a slog.Logger based on the provided configuration.
func createLogger(cfg config.LoggingConfig) (*slog.Logger, io.Closer, error) {
	var level slog.Level
	switch strings.ToLower(cfg.Level) {
	case "debug":
		level = slog.LevelDebug
	case "info":
		level = slog.LevelInfo
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	default:
		return nil, nil, fmt.Errorf("invalid log level: %s", cfg.Level)
	}

	var output io.Writer
	var closer io.Closer
	switch strings.ToLower(cfg.Output) {
	case "stdout":
		output = os.Stdout
	case "file":
		if cfg.File == "" {
			return nil, nil, fmt.Errorf("log output is 'file' but no file path is specified")
		}
		file, err := os.OpenFile(cfg.File, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to open log file %s: %w", cfg.File, err)
		}
		output = file
		closer = file // The file handle is the closer.
	case "none":
		output = io.Discard
	default:
		return nil, nil, fmt.Errorf("invalid log output: %s", cfg.Output)
	}

	logger := slog.New(slog.NewJSONHandler(output, &slog.HandlerOptions{Level: level}))
	return logger, closer, nil
}

// readAllQueryResults is a helper to drain a query stream into a slice.
func readAllQueryResults(t *testing.T, stream tsdb.TSDBService_QueryClient) []*tsdb.QueryResult {
	t.Helper()
	var results []*tsdb.QueryResult
	for {
		res, err := stream.Recv()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		results = append(results, res)
	}
	return results
}

func TestReplication_HealthCheckIntegration(t *testing.T) {
	// เตรียม AppServer ที่มี gRPC health service
	cfg := &config.Config{
		Server: config.ServerConfig{
			GRPCPort: 50051,
			TCPPort:  0,
			TLS:      config.TLSConfig{Enabled: false},
		},
		Engine: config.EngineConfig{
			DataDir: t.TempDir(),
		},
		Logging: config.LoggingConfig{Level: "error", Output: "stdout"},
	}
	logger, _, err := createLogger(cfg.Logging)
	assert.NoError(t, err)

	eng := setupEngineNoStart(t, engine2.StorageEngineOptions{DataDir: cfg.Engine.DataDir})

	appServer, err := NewAppServerWithListeners(eng, cfg, logger, nil, nil)
	assert.NoError(t, err)

	go func() {
		_ = appServer.Start()
	}()
	defer appServer.Stop()

	// รอ server ready
	<-time.After(500 * time.Millisecond)

	// Dial follower จริง
	addr := ""
	if cfg.Server.GRPCPort != 0 {
		addr = ":50051"
	}
	conn, err := grpc.Dial(addr, grpc.WithInsecure(), grpc.WithBlock(), grpc.WithTimeout(2*time.Second))
	assert.NoError(t, err)
	defer conn.Close()

	// Health check
	healthClient := grpc_health_v1.NewHealthClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	resp, err := healthClient.Check(ctx, &grpc_health_v1.HealthCheckRequest{})
	assert.NoError(t, err)
	assert.Equal(t, grpc_health_v1.HealthCheckResponse_SERVING, resp.GetStatus())
}

func listenTCP(addr string) net.Listener {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		panic(err)
	}
	return ln
}
