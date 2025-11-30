package engine2

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"runtime/pprof"
)

// StartCPUProfile creates the file at path and starts CPU profiling.
// Caller should call StopCPUProfile with the returned file when done.
func StartCPUProfile(path string) (*os.File, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("failed to create cpu profile file: %w", err)
	}
	if err := pprof.StartCPUProfile(f); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("pprof.StartCPUProfile: %w", err)
	}
	return f, nil
}

// StopCPUProfile stops the CPU profiler and closes the file.
func StopCPUProfile(f *os.File) error {
	if f == nil {
		return nil
	}
	pprof.StopCPUProfile()
	if err := f.Close(); err != nil {
		return fmt.Errorf("failed to close cpu profile file: %w", err)
	}
	return nil
}

// WriteHeapProfile writes a heap profile to the provided path.
func WriteHeapProfile(path string) error {
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("failed to create heap profile file: %w", err)
	}
	defer f.Close()
	if err := pprof.WriteHeapProfile(f); err != nil {
		return fmt.Errorf("pprof.WriteHeapProfile: %w", err)
	}
	return nil
}

// RunPprofTop runs `go tool pprof -top` on the given test/binary and profile
// and returns the textual output. This is a convenience wrapper used in CI
// or local analysis to summarize a profile programmatically.
func RunPprofTop(binaryPath, profilePath string) (string, error) {
	// `go tool pprof -top <binary> <profile>`
	cmd := exec.Command("go", "tool", "pprof", "-top", binaryPath, profilePath)
	var out bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return stderr.String(), fmt.Errorf("pprof top failed: %w: %s", err, stderr.String())
	}
	return out.String(), nil
}
