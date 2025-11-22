package engine2

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// ManifestManager maintains an in-memory view of the SSTable manifest and
// provides helpers to persist and query entries.
type ManifestManager struct {
	mu sync.RWMutex
	// addMu serializes submissions to the background queue to reduce
	// contention and make AddEntry behavior more predictable under
	// extreme concurrent load.
	addMu        sync.Mutex
	entries      []SSTableManifestEntry
	manifestPath string
	// background queue for serializing manifest writes to disk. Entries are
	// sent to the queue and processed by a single worker goroutine which
	// performs persistence and updates the in-memory view.
	queue       chan queuedEntry
	shutdownCh  chan struct{}
	workerClose sync.WaitGroup
}

type queuedEntry struct {
	e    SSTableManifestEntry
	resp chan error
}

// NewManifestManager constructs a manager for the given manifest path and loads
// any existing entries. It does not error on a missing manifest file.
func NewManifestManager(manifestPath string) (*ManifestManager, error) {
	m := &ManifestManager{manifestPath: manifestPath}
	// Ensure directory exists
	_ = ensureDir(filepath.Dir(manifestPath))
	if err := m.loadFromDisk(); err != nil {
		return nil, err
	}
	// initialize background queue and worker
	m.queue = make(chan queuedEntry, 1024)
	m.shutdownCh = make(chan struct{})
	m.workerClose.Add(1)
	go m.manifestWriter()
	return m, nil
}

func ensureDir(dir string) error {
	if dir == "" {
		return nil
	}
	return os.MkdirAll(dir, 0o755)
}

func (m *ManifestManager) loadFromDisk() error {
	manifest, err := LoadManifest(m.manifestPath)
	if err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.entries = manifest.Entries
	return nil
}

// Reload refreshes the in-memory manifest from disk.
func (m *ManifestManager) Reload() error {
	return m.loadFromDisk()
}

// AddEntry persists the new entry and updates the in-memory state.
func (m *ManifestManager) AddEntry(e SSTableManifestEntry) error {
	// Serialize only the enqueue operation to avoid queue contention.
	// Do NOT hold the lock while waiting on the background worker response,
	// otherwise AddEntry callers will be serialized behind the full disk
	// persist latency which causes severe bottlenecks under high load.
	qe := queuedEntry{e: e, resp: make(chan error, 1)}
	// Delegate enqueue/fallback logic to helper while serializing the
	// submission operation with addMu to reduce contention.
	if err := m.enqueueQueuedEntry(qe); err != nil {
		return err
	}
	// wait for result without holding addMu
	return <-qe.resp
}

// enqueueQueuedEntry centralizes the logic for submitting a queuedEntry to
// the manager's internal queue. It tries a non-blocking send while holding
// `addMu` and, if the queue is full, enqueues asynchronously so callers are
// not blocked on queue backpressure. Returns an error if the manager is
// shutting down.
func (m *ManifestManager) enqueueQueuedEntry(qe queuedEntry) error {
	m.addMu.Lock()

	select {
	case m.queue <- qe:
		m.addMu.Unlock()
		return nil
	case <-m.shutdownCh:
		m.addMu.Unlock()
		return fmt.Errorf("manifest manager shutting down")
	default:
		// Queue full: release lock and enqueue asynchronously so the caller
		// doesn't block on a channel send.
		m.addMu.Unlock()
		go func(q queuedEntry) {
			select {
			case m.queue <- q:
				// enqueued
			case <-m.shutdownCh:
				q.resp <- fmt.Errorf("manifest manager shutting down")
			}
		}(qe)
		return nil
	}
}

// Close stops the background writer and waits for outstanding work to finish.
func (m *ManifestManager) Close() {
	// Idempotent close
	select {
	case <-m.shutdownCh:
		return
	default:
	}
	close(m.shutdownCh)
	// drain queue to avoid blocking producer callers (should be none in tests)
	// wait for worker to finish
	m.workerClose.Wait()
}

// manifestWriter runs as a single background goroutine processing queued entries.
func (m *ManifestManager) manifestWriter() {
	defer m.workerClose.Done()
	// Increase batch size and wait window to reduce the number of disk
	// persist operations under high contention. This improves throughput by
	// amortizing the cost of SaveManifest across more entries.
	const maxBatch = 1024
	const batchWait = 5 * time.Millisecond

	for {
		select {
		case first := <-m.queue:
			// Start a batch with the first entry
			batch := []queuedEntry{first}
			// Collect additional entries up to maxBatch or until timeout
		collectLoop:
			for len(batch) < maxBatch {
				select {
				case more := <-m.queue:
					batch = append(batch, more)
				case <-time.After(batchWait):
					// small wait window to collect more writers
					break collectLoop
				case <-m.shutdownCh:
					// stop collecting further entries
					break collectLoop
				}
			}

			// Build the new entries list by snapshotting the current in-memory
			// entries and appending the batch. This avoids re-loading the
			// manifest from disk on every batch (which causes repeated re-writes
			// of already persisted entries) and greatly improves throughput.
			m.mu.RLock()
			current := make([]SSTableManifestEntry, len(m.entries))
			copy(current, m.entries)
			m.mu.RUnlock()
			newEntries := make([]SSTableManifestEntry, 0, len(current)+len(batch))
			newEntries = append(newEntries, current...)
			for _, be := range batch {
				newEntries = append(newEntries, be.e)
			}
			// Persist once for the batch with a few retries for transient failures
			var persistErr error
			const persistAttempts = 3
			for attempt := 1; attempt <= persistAttempts; attempt++ {
				persistErr = SaveManifest(m.manifestPath, SSTableManifest{Entries: newEntries})
				if persistErr == nil {
					break
				}
				// small backoff
				time.Sleep(time.Duration(attempt) * 20 * time.Millisecond)
			}
			if persistErr != nil {
				for _, be := range batch {
					be.resp <- persistErr
				}
				continue
			}
			// Update in-memory view under lock
			m.mu.Lock()
			m.entries = newEntries
			m.mu.Unlock()
			for _, be := range batch {
				be.resp <- nil
			}
		case <-m.shutdownCh:
			return
		}
	}
}

// ListEntries returns a copy of current manifest entries.
func (m *ManifestManager) ListEntries() []SSTableManifestEntry {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]SSTableManifestEntry, len(m.entries))
	copy(out, m.entries)
	return out
}
