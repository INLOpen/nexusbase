#!/usr/bin/env python3
import glob, re, statistics, sys
from pathlib import Path

def pct(xs, p):
    if not xs: return None
    xs=sorted(xs)
    k=(len(xs)-1)*p/100.0
    f=int(k); c=min(f+1,len(xs)-1)
    if f==c: return xs[int(k)]
    d=k-f
    return xs[f]*(1-d)+xs[c]*d

logdir='/mnt/d/go/nexusbase/diagnostics'
paths=sorted(glob.glob(logdir+'/wal_diag_*ms.log'))
if not paths:
    print('No log files found in', logdir)
    sys.exit(1)

for p in paths:
    m = re.search(r'wal_diag_(\d+)ms.log$', p)
    delay = m.group(1) if m else p
    payloads=[]
    commits=[]
    with open(p,'r', errors='ignore') as fh:
        for line in fh:
            m1 = re.search(r'payload .* duration_ms=(\d+)', line)
            if m1:
                payloads.append(int(m1.group(1)))
            m2 = re.search(r'commit .* duration_ms=(\d+)', line)
            if m2:
                commits.append(int(m2.group(1)))
    def summarize(name, arr):
        if not arr:
            return name + ': no samples'
        return ('{name}: count={count} min={min_} mean={mean:.3f} '
                'p50={p50:.3f} p90={p90:.3f} p95={p95:.3f} p99={p99:.3f}').format(
            name=name,
            count=len(arr),
            min_=min(arr),
            mean=statistics.mean(arr),
            p50=pct(arr,50),
            p90=pct(arr,90),
            p95=pct(arr,95),
            p99=pct(arr,99),
        )
    # iostat parsing (if present)
    iostat_file = f"{logdir}/iostat_{delay}ms.txt"
    iostat_samples = []  # each sample: dict with total_tps and avg_await (or None)
    avg_tps = None
    avg_await = None
    if Path(iostat_file).exists():
        # We'll read the file and capture device records; iostat repeats headers, so we do a simple
        # global aggregation across all device lines (this approximates overall load during the run).
        device_records = []
        with open(iostat_file, 'r', errors='ignore') as fh:
            header_cols = None
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                if line.startswith('Device') or line.startswith('Device:'):
                    header_cols = line.replace('Device:', 'Device').split()
                    continue
                if header_cols is None:
                    continue
                parts = line.split()
                if len(parts) < len(header_cols):
                    continue
                # Map header->value
                try:
                    rec = {header_cols[i]: float(parts[i]) for i in range(len(header_cols))}
                except Exception:
                    continue
                device_records.append(rec)
        if device_records:
            total_tps = 0.0
            awaits = []
            for dev in device_records:
                if 'tps' in dev:
                    total_tps += dev.get('tps', 0.0)
                else:
                    total_tps += dev.get('r/s', 0.0) + dev.get('w/s', 0.0)
                if 'await' in dev:
                    awaits.append(dev.get('await', 0.0))
            avg_tps = total_tps
            avg_await = statistics.mean(awaits) if awaits else None
        else:
            # Fallback: attempt to parse lines that contain device names and trailing numbers
            dev_re = re.compile(r'\b(sd[a-z0-9]*|nvme[^\s]+|vd[a-z0-9]*)\b')
            records = []
            with open(iostat_file, 'r', errors='ignore') as fh:
                for line in fh:
                    # find device tokens (e.g., sda, sdb, sdd)
                    m = dev_re.search(line)
                    if not m:
                        continue
                    # split tokens and find the first token that looks like a device name
                    toks = line.split()
                    for i, tok in enumerate(toks):
                        if dev_re.fullmatch(tok):
                            # collect next numeric tokens (up to 15) as floats
                            nums = []
                            for j in range(i+1, min(i+16, len(toks))):
                                try:
                                    nums.append(float(toks[j]))
                                except Exception:
                                    break
                            if nums:
                                records.append(nums)
                            break
            if records:
                total_tps = 0.0
                awaits = []
                for nums in records:
                    # typical ordering (after device): r/s,rkB/s,rrqm/s,%rrqm,r_await,rareq-sz,w/s,...
                    r_s = nums[0] if len(nums) > 0 else 0.0
                    w_s = nums[6] if len(nums) > 6 else 0.0
                    total_tps += (r_s + w_s)
                    if len(nums) > 4:
                        awaits.append(nums[4])
                avg_tps = total_tps
                avg_await = statistics.mean(awaits) if awaits else None

    print('--- Delay {}ms ---'.format(delay))
    print(summarize('payload', payloads))
    print(summarize('commit', commits))
    if avg_tps is not None:
        print(f'iostat: approx_total_tps={avg_tps:.3f} avg_await={avg_await if avg_await is not None else "N/A"}')
    else:
        print('iostat: no data')

    # append row to summary CSV
    summary_file = Path(f"{logdir}/summary.csv")
    header = 'delay_ms,payload_count,payload_mean_ms,commit_mean_ms,approx_total_tps,avg_await_ms\n'
    row = f"{delay},{len(payloads)},{statistics.mean(payloads) if payloads else ''},{statistics.mean(commits) if commits else ''},{avg_tps if avg_tps is not None else ''},{avg_await if avg_await is not None else ''}\n"
    if not summary_file.exists():
        summary_file.write_text(header)
    with summary_file.open('a') as fh:
        fh.write(row)
