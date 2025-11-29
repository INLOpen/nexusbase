package sstable

import "sync/atomic"

// Hooks for signalling bloom filter events to higher layers (engine adapter).
// These are optional and set by callers (e.g., engine2 adapter) when they
// want to convert sstable-level events into higher-level metrics.

type bfHooks struct {
	check    func()
	falsePos func()
}

var bloomHooks atomic.Value // stores *bfHooks

// SetBloomFilterHooks registers callbacks invoked by sstable when a Bloom
// filter check occurs or when a false-positive is detected. Both callbacks
// are optional and may be nil. This allows the engine to increment its own
// metrics without creating a circular dependency. This function is safe to
// call concurrently from multiple goroutines.
func SetBloomFilterHooks(check func(), falsePos func()) {
	bloomHooks.Store(&bfHooks{check: check, falsePos: falsePos})
}

// internal helpers used by reader methods
func invokeBloomCheckHook() {
	v := bloomHooks.Load()
	if v == nil {
		return
	}
	hooks := v.(*bfHooks)
	if hooks.check != nil {
		hooks.check()
	}
}

func invokeBloomFalsePositiveHook() {
	v := bloomHooks.Load()
	if v == nil {
		return
	}
	hooks := v.(*bfHooks)
	if hooks.falsePos != nil {
		hooks.falsePos()
	}
}
