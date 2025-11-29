package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/INLOpen/nexusbase/api/tsdb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/structpb"
)

// basicAuthCreds implements credentials.PerRPCCredentials for Basic Auth.
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
	return false // Set to true in production with TLS
}

func main() {
	// --- Configuration Flags ---
	// Connection flags
	serverAddr := flag.String("addr", "localhost:50051", "The gRPC server address")
	tlsEnabled := flag.Bool("tls", false, "Enable TLS for the connection")
	caFile := flag.String("cacert", "certs/server.crt", "The CA certificate file to trust")
	username := flag.String("username", "", "Username for authentication")
	password := flag.String("password", "", "Password for authentication")

	// Benchmark flags (PERF-001)
	numPoints := flag.Int("points", 1000000, "Total number of data points to write")
	numSeries := flag.Int("series", 100, "Number of unique time series to generate")
	batchSize := flag.Int("batch-size", 1000, "Number of data points per PutBatch request")
	concurrency := flag.Int("concurrency", 10, "Number of concurrent writers (goroutines)")
	verbose := flag.Bool("verbose", false, "Enable verbose logging")

	flag.Parse()

	// Configure logging according to verbose flag. When not verbose, silence the
	// `log` package output; final results printed with fmt remain visible.
	if !*verbose {
		log.SetOutput(io.Discard)
	} else {
		log.SetFlags(log.LstdFlags | log.Lmicroseconds)
		log.Println("Verbose logging enabled")
	}

	// --- gRPC Connection Setup ---
	var opts []grpc.DialOption
	if *tlsEnabled {
		// Secure TLS setup
		log.Println("--- Connecting with secure TLS ---")
		cert, err := os.ReadFile(*caFile)
		if err != nil {
			log.Fatalf("Failed to read CA certificate file: %v", err)
		}
		certPool := x509.NewCertPool()
		if !certPool.AppendCertsFromPEM(cert) {
			log.Fatalf("Failed to append CA certificate to pool")
		}
		creds := credentials.NewTLS(&tls.Config{
			ServerName: "localhost", // Must match the CN in the server's certificate
			RootCAs:    certPool,
		})
		opts = append(opts, grpc.WithTransportCredentials(creds))
	} else {
		// Insecure connection
		log.Println("--- Connecting with TLS disabled (insecure) ---")
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}

	if *username != "" {
		opts = append(opts, grpc.WithPerRPCCredentials(basicAuthCreds{username: *username, password: *password}))
	}

	conn, err := grpc.NewClient(*serverAddr, opts...)
	if err != nil {
		log.Fatalf("Did not connect: %v", err)
	}
	defer conn.Close()

	client := tsdb.NewTSDBServiceClient(conn)

	// --- Data Generation ---
	log.Printf("Generating %d data points for %d series...", *numPoints, *numSeries)
	points := make([]*tsdb.PutRequest, 0, *numPoints)
	seriesDefs := make([]map[string]string, *numSeries)
	for i := 0; i < *numSeries; i++ {
		seriesDefs[i] = map[string]string{
			"host":   fmt.Sprintf("host-%d", i),
			"region": fmt.Sprintf("region-%d", i%5), // Cycle through 5 regions
		}
	}

	// Generate random data points across the defined series
	startTime := time.Now().UnixNano()
	for i := 0; i < *numPoints; i++ {
		seriesIndex := i % *numSeries
		fields, _ := structpb.NewStruct(map[string]interface{}{
			"value": rand.Float64() * 100,
		})
		points = append(points, &tsdb.PutRequest{
			Metric:    "perf.test.metric",
			Tags:      seriesDefs[seriesIndex],
			Timestamp: startTime + int64(i)*int64(time.Second), // 1 point per second
			Fields:    fields,
		})
	}
	log.Println("Data generation complete.")

	// --- Benchmark Execution ---
	var wg sync.WaitGroup
	var totalWritten int64
	pointsPerWorker := *numPoints / *concurrency

	// Slice to store latencies for each PutBatch request.
	latencies := make([]time.Duration, 0, *numPoints / *batchSize)
	var latenciesMu sync.Mutex

	log.Printf("Starting benchmark with %d concurrent writers...", *concurrency)
	start := time.Now()

	for i := 0; i < *concurrency; i++ {
		wg.Add(1)
		workerStart := i * pointsPerWorker
		workerEnd := (i + 1) * pointsPerWorker
		if i == *concurrency-1 {
			workerEnd = *numPoints // Last worker takes the remainder
		}

		go func(workerPoints []*tsdb.PutRequest) {
			defer wg.Done()
			for j := 0; j < len(workerPoints); j += *batchSize {
				batchEnd := j + *batchSize
				if batchEnd > len(workerPoints) {
					batchEnd = len(workerPoints)
				}
				batch := workerPoints[j:batchEnd]

				if len(batch) == 0 {
					continue
				}

				reqStart := time.Now()
				_, err := client.PutBatch(context.Background(), &tsdb.PutBatchRequest{Points: batch})
				reqDuration := time.Since(reqStart)

				if err != nil {
					log.Printf("PutBatch failed: %v", err)
					// In a real test, you might want to count errors
					continue
				}

				latenciesMu.Lock()
				latencies = append(latencies, reqDuration)
				latenciesMu.Unlock()
				atomic.AddInt64(&totalWritten, int64(len(batch)))
			}
		}(points[workerStart:workerEnd])
	}

	wg.Wait()
	duration := time.Since(start)

	// --- Results ---
	pointsPerSec := float64(totalWritten) / duration.Seconds()

	// --- Calculate Latency Percentiles ---
	sort.Slice(latencies, func(i, j int) bool {
		return latencies[i] < latencies[j]
	})

	var p50, p90, p99 time.Duration
	if len(latencies) > 0 {
		p50 = latencies[int(float64(len(latencies))*0.50)]
		p90 = latencies[int(float64(len(latencies))*0.90)]
		p99 = latencies[int(float64(len(latencies))*0.99)]
	}

	fmt.Println("\n--- Benchmark Results (PERF-001) ---")
	fmt.Printf("Total Points Written: %d\n", totalWritten)
	fmt.Printf("Total Time Taken:     %.2f seconds\n", duration.Seconds())
	fmt.Printf("Write Throughput:     %.2f points/sec\n", pointsPerSec)
	fmt.Println("\n--- Latency Distribution (PutBatch) ---")
	fmt.Printf("P50 (Median): %v\n", p50)
	fmt.Printf("P90:          %v\n", p90)
	fmt.Printf("P99:          %v\n", p99)
	fmt.Println("------------------------------------")
	fmt.Println("Note: CPU/Memory usage and Compaction time should be monitored on the server-side using tools like 'top', 'htop', or Prometheus.")
}
