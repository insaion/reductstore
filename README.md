# ingest_probe.py

A/B two `reductstore` binaries on the write path. It starts each binary in turn on a fresh
data dir, writes random records over one keep-alive HTTP connection, and reads the server's
CPU time, peak RSS and write syscalls from `/proc`. It runs the binaries alternately
(5 runs each by default) and prints medians and deltas.

Linux only, Python 3.8+, no dependencies.

## Usage

```bash
# 1. Build both sides
git switch main          && cargo build --release -p reductstore && cp target/release/reductstore /tmp/rs-base
git switch <branch>      && cargo build --release -p reductstore && cp target/release/reductstore /tmp/rs-changed

# 2. Compare (about 3 minutes per line)
curl -sO https://raw.githubusercontent.com/insaion/reductstore/tools/ingest-probe/ingest_probe.py
python3 ingest_probe.py /tmp/rs-base /tmp/rs-changed --size 65536                      # small records, max rate
python3 ingest_probe.py /tmp/rs-base /tmp/rs-changed --size 1048576 --rate 200 --cpus 2,3  # 1 MiB camera frames at 200 MB/s
```

Output:

```
65536 B records, max rate, 4 entries, median of 5 runs of 15 s
                                base     changed    delta
MB/s                          ...
CPU us/record                 ...
CPU s/GB                      ...
write syscalls/record         ...
peak RSS MB                   ...
p99 latency ms                ...
```

`CPU us/record` and `write syscalls/record` are the stable numbers. MB/s at max rate is
bound by the disk and is noisy on consumer NVMe drives.

## Tips

- `--data-dir` must be on the disk you want to test, not tmpfs. The default is
  `./ingest-probe-data`, recreated for every run.
- `--cpus 2,3` pins the server and cuts run-to-run noise. Run the probe itself on other
  cores, for example `taskset -c 0,1 python3 ingest_probe.py ...`.
- `--workers 2` sets `TOKIO_WORKER_THREADS`, as on a small edge device.
- The server runs with `RS_API_TOKEN=probe` and port 8383 (`--port` to change it).
