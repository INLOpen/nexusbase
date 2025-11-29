package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal" // New import
	"strings"
	"syscall" // New import
	"time"

	// Generated gRPC code
	// Core utilities
	"github.com/INLOpen/nexusbase/compressors"
	"github.com/INLOpen/nexusbase/config"
	"github.com/INLOpen/nexusbase/core"
	"github.com/INLOpen/nexusbase/engine2"
	"github.com/INLOpen/nexusbase/hooks"
	"github.com/INLOpen/nexusbase/hooks/listeners"
	"github.com/INLOpen/nexusbase/sys"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.21.0"

	// For sstable.DefaultBlockSize and ErrNotFound

	optsPackage "github.com/INLOpen/nexusbase/cmd/server/opts"
	"github.com/INLOpen/nexusbase/levels"
	"github.com/INLOpen/nexusbase/server" // Your gRPC server implementation
)

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

// initTracerProvider creates and configures an OpenTelemetry TracerProvider.
// It sets up an exporter based on the configuration to send traces to a collector.
func initTracerProvider(cfg config.TracingConfig, logger *slog.Logger) (*sdktrace.TracerProvider, func(), error) {
	if !cfg.Enabled {
		logger.Info("Distributed tracing is disabled.")
		// Return a no-op provider and an empty cleanup function.
		return sdktrace.NewTracerProvider(), func() {}, nil
	}

	logger.Info("Initializing distributed tracing...", "protocol", cfg.Protocol, "endpoint", cfg.Endpoint)

	ctx := context.Background()
	var exporter sdktrace.SpanExporter
	var err error

	// Create an OTLP exporter (gRPC or HTTP)
	switch strings.ToLower(cfg.Protocol) {
	case "http":
		exporter, err = otlptrace.New(ctx, otlptracehttp.NewClient(otlptracehttp.WithEndpoint(cfg.Endpoint), otlptracehttp.WithInsecure()))
	case "grpc":
		exporter, err = otlptrace.New(ctx, otlptracegrpc.NewClient(otlptracegrpc.WithEndpoint(cfg.Endpoint), otlptracegrpc.WithInsecure()))
	default:
		return nil, nil, fmt.Errorf("unsupported tracing protocol: %q", cfg.Protocol)
	}

	// Define the service resource
	res, err := resource.New(ctx, resource.WithAttributes(semconv.ServiceNameKey.String("nexusbase")))
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create trace resource: %w", err)
	}

	// Create the TracerProvider
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
	)

	// Set the global TracerProvider
	otel.SetTracerProvider(tp)

	cleanup := func() {
		logger.Info("Shutting down tracer provider...")
		// Create a context with a timeout to prevent shutdown from hanging.
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := tp.Shutdown(shutdownCtx); err != nil {
			logger.Error("Error shutting down tracer provider", "error", err)
		}
	}

	return tp, cleanup, nil
}

// buildEngineOptions maps the loaded configuration into engine2.StorageEngineOptions.
// Extracted for testability.
func buildEngineOptions(cfg *config.Config, hookManager hooks.HookManager, logger *slog.Logger, sstCompressor core.Compressor) engine2.StorageEngineOptions {
	// Map compaction fallback strategy
	compactionFallback := levels.PickOldest
	switch strings.ToLower(cfg.Engine.Compaction.FallbackStrategy) {
	case "picklargest", "largest":
		compactionFallback = levels.PickLargest
	case "picksmallest", "smallest":
		compactionFallback = levels.PickSmallest
	case "pickmostkeys", "mostkeys":
		compactionFallback = levels.PickMostKeys
	case "picksmallestavgkeysize", "smallestavgkeysize":
		compactionFallback = levels.PickSmallestAvgKeySize
	case "pickoldestbytimestamp", "oldestbytimestamp":
		compactionFallback = levels.PickOldestByTimestamp
	case "pickfewestkeys", "fewestkeys":
		compactionFallback = levels.PickFewestKeys
	case "pickrandom", "random":
		compactionFallback = levels.PickRandom
	case "picklargestavgkeysize", "largestavgkeysize":
		compactionFallback = levels.PickLargestAvgKeySize
	case "picknewest", "newest":
		compactionFallback = levels.PickNewest
	case "pickhighesttombstonedensity", "highesttombstonedensity":
		compactionFallback = levels.PickHighestTombstoneDensity
	default:
		compactionFallback = levels.PickOldest
	}

	compactionIntervalSeconds := int(config.ParseDuration(cfg.Engine.Compaction.CheckInterval, 120*time.Second, logger).Seconds())

	opts := engine2.StorageEngineOptions{
		DataDir:                      cfg.Engine.DataDir,
		HookManager:                  hookManager,
		Logger:                       logger,
		EnableSSTablePreallocate:     cfg.Engine.SSTable.Preallocate,
		SSTablePreallocMultiplier:    cfg.Engine.SSTable.PreallocMultiplierBytesPerKey,
		SSTableDefaultBlockSize:      int(cfg.Engine.SSTable.BlockSizeBytes),
		BloomFilterFalsePositiveRate: cfg.Engine.SSTable.BloomFilterFPRate,
		SSTableCompressor:            sstCompressor,
		SSTableRestartPointInterval:  cfg.Engine.SSTable.RestartPointInterval,

		// Index-related configuration mapped from config.Engine.Index
		IndexMemtableThreshold:         cfg.Engine.Index.MemtableThreshold,
		IndexFlushIntervalMs:           int(config.ParseDuration(cfg.Engine.Index.FlushInterval, 60*time.Second, logger).Milliseconds()),
		IndexCompactionIntervalSeconds: int(config.ParseDuration(cfg.Engine.Index.CompactionCheckInterval, 20*time.Second, logger).Seconds()),
		IndexMaxL0Files:                cfg.Engine.Index.L0TriggerFileCount,
		IndexBaseTargetSize:            cfg.Engine.Index.BaseTargetSizeBytes,

		// Global compaction and tuning options mapped from config.Engine.Compaction
		L0CompactionTriggerSize:           cfg.Engine.Compaction.L0TriggerSizeBytes,
		TargetSSTableSize:                 cfg.Engine.Compaction.TargetSSTableSizeBytes,
		LevelsTargetSizeMultiplier:        cfg.Engine.Compaction.LevelsSizeMultiplier,
		MaxLevels:                         cfg.Engine.Compaction.MaxLevels,
		MaxL0Files:                        cfg.Engine.Compaction.L0TriggerFileCount,
		CompactionTombstoneWeight:         cfg.Engine.Compaction.TombstoneWeight,
		CompactionOverlapWeight:           cfg.Engine.Compaction.OverlapPenaltyWeight,
		IntraL0CompactionTriggerFiles:     cfg.Engine.Compaction.IntraL0TriggerFileCount,
		IntraL0CompactionMaxFileSizeBytes: cfg.Engine.Compaction.IntraL0MaxFileSizeBytes,
		CompactionFallbackStrategy:        compactionFallback,
		CompactionIntervalSeconds:         compactionIntervalSeconds,
	}

	return opts
}

func main() {
	// Define a command-line flag for the config file path
	configPath := flag.String("config", "config.yaml", "Path to the configuration file")
	flag.Parse()

	// Load configuration
	cfg, err := config.LoadConfig(*configPath)
	if err != nil {
		// Use a temporary logger for pre-config errors
		slog.Error("Failed to load configuration", "path", *configPath, "error", err)
		os.Exit(1)
	}

	// Create the logger based on the loaded configuration
	logger, logCloser, err := createLogger(cfg.Logging)
	if err != nil {
		slog.Error("Failed to create logger", "error", err)
		os.Exit(1)
	}
	// Defer closing the log file if one was opened.
	if logCloser != nil {
		defer logCloser.Close()
	}

	// The engine will now create its own subdirectories. We just need to ensure the base dir is specified.
	if cfg.Engine.DataDir == "" {
		logger.Error("Engine data_dir must be specified in the configuration file.")
		os.Exit(1)
	}
	logger.Info("Using data directory", "path", cfg.Engine.DataDir)

	// compressor selection removed: Engine2 currently manages its own SSTable settings
	var metricSrv *server.MetricsServer
	if cfg.Debug.Enabled {
		metricSrv = server.NewMetricsServer(&cfg.Debug, logger)
		go func() {
			if err := metricSrv.Start(); err != nil {
				logger.Error("Failed to start metrics server", "error", err)
			}
		}()
	}

	// Initialize the TracerProvider
	_, tracerCleanup, err := initTracerProvider(cfg.Tracing, logger)
	if err != nil {
		logger.Error("Failed to initialize tracer provider", "error", err)
		os.Exit(1)
	}

	// Manifest lock TTL controls how long an external lockfile is considered valid
	// before this process may attempt to break it (useful when other process crashed).
	manifestLockTTL := config.ParseDuration(cfg.Engine.ManifestLockTTL, 30*time.Second, logger)
	sys.SetDefaultLockStaleTTL(manifestLockTTL)

	// Legacy StorageEngine options removed; Engine2 is the primary storage engine.

	// Engine2-only startup: create a HookManager, register listeners, and start Engine2.
	hookManager := hooks.NewHookManager(logger.With("component", "HookManager"))
	waListener := listeners.NewWriteAmplificationListener(logger)
	cardinalityListener := listeners.NewCardinalityAlerterListener(logger)
	outlierRules := []listeners.OutlierRule{
		{MetricName: "http.requests.latency", FieldName: "ms", Thresholds: listeners.Thresholds{Min: 0, Max: 2000}},
		{MetricName: "environment.temperature", FieldName: "celsius", Thresholds: listeners.Thresholds{Min: -10, Max: 50}},
	}
	outlierListener := listeners.NewOutlierDetectionListener(logger, outlierRules)
	hookManager.Register(hooks.EventPostCompaction, waListener)
	hookManager.Register(hooks.EventOnSeriesCreate, cardinalityListener)
	hookManager.Register(hooks.EventPrePutBatch, outlierListener)

	// System metrics collector
	systemCollector := server.NewSystemCollector(cfg.Engine.DataDir, 2*time.Second, logger)
	systemCollector.Start()
	// Choose compressor implementation based on config string.
	var sstCompressor core.Compressor
	switch strings.ToLower(cfg.Engine.SSTable.Compression) {
	case "snappy":
		sstCompressor = compressors.NewSnappyCompressor()
	case "lz4":
		sstCompressor = compressors.NewLz4Compressor()
	case "zstd":
		sstCompressor = compressors.NewZstdCompressor()
	case "none":
		sstCompressor = &compressors.NoCompressionCompressor{}
	default:
		logger.Warn("Unknown sstable compressor; defaulting to snappy", "requested", cfg.Engine.SSTable.Compression)
		sstCompressor = compressors.NewSnappyCompressor()
	}

	// Build and log applied engine options for quick verification.
	opts := optsPackage.BuildEngineOptions(cfg, hookManager, logger, sstCompressor)
	logger.Info("Applying engine options", "data_dir", opts.DataDir, "l0_compaction_trigger_size", opts.L0CompactionTriggerSize, "target_sstable_size", opts.TargetSSTableSize, "compaction_interval_seconds", opts.CompactionIntervalSeconds, "compaction_fallback", opts.CompactionFallbackStrategy, "index_compaction_interval_seconds", opts.IndexCompactionIntervalSeconds)
	engAi, eng2Err := engine2.NewStorageEngine(opts)
	if eng2Err != nil {
		logger.Error("Failed to create Engine2 instance", "error", eng2Err)
		os.Exit(1)
	}
	eng2Adapter := engAi.(*engine2.Engine2Adapter)
	// Apply configured max chunk bytes to engine2 adapter if provided.
	if cfg.Engine.MaxChunkBytes > 0 {
		eng2Adapter.SetMaxChunkBytes(cfg.Engine.MaxChunkBytes)
	}
	if startErr := eng2Adapter.Start(); startErr != nil {
		logger.Error("Failed to start Engine2 adapter", "error", startErr)
		_ = eng2Adapter.Close()
		os.Exit(1)
	}
	var primaryEngine engine2.StorageEngineInterface = eng2Adapter

	// Create and initialize the application server using the selected primary engine.
	appServer, err := server.NewAppServer(primaryEngine, cfg, logger)
	if err != nil {
		logger.Error("Failed to create application server", "error", err)
		// Cleanup started components
		if eng2Adapter != nil {
			_ = eng2Adapter.Close()
		}
		os.Exit(1)
	}

	// Keep the main goroutine alive to allow the gRPC server to run
	// In a real application, you'd typically have a signal handler here
	// to gracefully shut down the server on SIGINT/SIGTERM.
	logger.Info("Application running. Press Ctrl+C to exit.")

	// Graceful shutdown: Wait for interrupt signal
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)

	serverErrChan := make(chan error, 1)
	go func() {
		serverErrChan <- appServer.Start()
	}()

	select {
	case err := <-serverErrChan:
		logger.Error("Server exited with an error", "error", err)
	case <-quit:
		logger.Info("Shutdown signal received. Stopping server...")
		appServer.Stop() // 1. ส่งสัญญาณให้ AppServer หยุด (ไม่ block)

		// 2. รอจนกว่า goroutine ของ AppServer จะทำงานเสร็จสิ้น (block)
		<-serverErrChan

		// 3. เมื่อ Server หยุดสนิทแล้ว จึงสั่งปิด Engine2 adapter และ Database Engine (block จนกว่าจะ flush เสร็จ)
		if eng2Adapter != nil {
			_ = eng2Adapter.Close()
		}

		tracerCleanup() // Shutdown the tracer provider
		systemCollector.Stop()
		if metricSrv != nil {
			metricSrv.Stop()
		}

		logger.Info("Application exited gracefully.")
	}
}
