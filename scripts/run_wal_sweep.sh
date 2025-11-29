#!/bin/bash
set -euo pipefail
mkdir -p /mnt/d/go/nexusbase/diagnostics
for d in 0 1 5 10; do
  log=/mnt/d/go/nexusbase/diagnostics/wal_diag_${d}ms.log
  iostatf=/mnt/d/go/nexusbase/diagnostics/iostat_${d}ms.txt
  echo "=== RUN delay=${d}ms ===" > "$log"
  echo "WAL_COMMIT_MAX_DELAY_MS=${d}" >> "$log"
  echo "Starting iostat -> $iostatf" >> "$log"
  # sample iostat extended stats every 1s
  iostat -x 1 > "$iostatf" &
  iostat_pid=$!
  echo "iostat pid: $iostat_pid" >> "$log"
  WAL_COMMIT_MAX_DELAY_MS=${d} go test ./wal -run TestWALDiagCommit -v -count=1 2>&1 | tee -a "$log"
  # give iostat a sec to write final sample then stop it
  sleep 1
  kill "$iostat_pid" || true
  echo "Finished run delay=${d}ms" >> "$log"
done

echo "Sweep finished; logs and iostat files are in /mnt/d/go/nexusbase/diagnostics"
