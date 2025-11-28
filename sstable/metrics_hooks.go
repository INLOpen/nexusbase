package sstable

// Hooks for signalling bloom filter events to higher layers (engine adapter).
// These are optional and set by callers (e.g., engine2 adapter) when they
// want to convert sstable-level events into higher-level metrics.

var (
	bloomCheckHook         func()
	bloomFalsePositiveHook func()
)

// SetBloomFilterHooks registers callbacks invoked by sstable when a Bloom
// filter check occurs or when a false-positive is detected. Both callbacks
// are optional and may be nil. This allows the engine to increment its own
// metrics without creating a circular dependency.
func SetBloomFilterHooks(check func(), falsePos func()) {
	bloomCheckHook = check
	bloomFalsePositiveHook = falsePos
}

// internal helpers used by reader methods
func invokeBloomCheckHook() {
	if bloomCheckHook != nil {
		bloomCheckHook()
	}
}

func invokeBloomFalsePositiveHook() {
	if bloomFalsePositiveHook != nil {
		bloomFalsePositiveHook()
	}
}
