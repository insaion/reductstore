#!/usr/bin/env python3
"""A/B the ReductStore write path: CPU, throughput, memory and syscalls per record.

Starts each server binary in turn on a fresh data dir, writes random records over one
keep-alive connection, and reads the server's CPU, RSS and write syscalls from /proc.
Linux only, Python 3.8+, no dependencies.

  python3 ingest_probe.py BASE_BINARY CHANGED_BINARY [--size BYTES] [--rate MBps] [--reps N]

Example (64 KiB records as fast as possible, then 1 MiB records at 200 MB/s):
  python3 ingest_probe.py ./rs-main ./rs-branch --size 65536
  python3 ingest_probe.py ./rs-main ./rs-branch --size 1048576 --rate 200
"""
import argparse, os, shutil, socket, statistics, subprocess, sys, time, urllib.request

CLK = os.sysconf("SC_CLK_TCK")
AUTH = {"Authorization": "Bearer probe"}


def cpu_s(pid):
    with open(f"/proc/{pid}/stat") as f:
        p = f.read().rsplit(")", 1)[1].split()
    return (int(p[11]) + int(p[12])) / CLK


def peak_rss_mb(pid):
    with open(f"/proc/{pid}/status") as f:
        for line in f:
            if line.startswith("VmHWM:"):
                return int(line.split()[1]) / 1024
    return 0.0


def write_syscalls(pid):
    with open(f"/proc/{pid}/io") as f:
        return int(next(l for l in f if l.startswith("syscw:")).split()[1])


class Conn:
    def __init__(self, port):
        self.s = socket.create_connection(("127.0.0.1", port))
        self.s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.buf = b""

    def post(self, path, body):
        head = f"POST {path} HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer probe\r\nContent-Type: application/octet-stream\r\nContent-Length: {len(body)}\r\n\r\n"
        self.s.sendall(head.encode())
        self.s.sendall(body)
        while b"\r\n\r\n" not in self.buf:
            d = self.s.recv(65536)
            if not d:
                raise RuntimeError("server closed the connection")
            self.buf += d
        head, self.buf = self.buf.split(b"\r\n\r\n", 1)
        lines = head.decode().split("\r\n")
        hdrs = {l.split(":", 1)[0].lower(): l.split(":", 1)[1].strip() for l in lines[1:]}
        n = int(hdrs.get("content-length", "0"))
        while len(self.buf) < n:
            self.buf += self.s.recv(65536)
        self.buf = self.buf[n:]
        status = int(lines[0].split()[1])
        if status != 200:
            raise RuntimeError(f"HTTP {status}: {hdrs.get('x-reduct-error')}")


def run_once(binary, a, data_dir):
    shutil.rmtree(data_dir, ignore_errors=True)
    os.makedirs(data_dir)
    env = dict(os.environ, RS_DATA_PATH=data_dir, RS_PORT=str(a.port), RS_LOG_LEVEL="WARN", RS_API_TOKEN="probe")
    if a.workers:
        env["TOKIO_WORKER_THREADS"] = str(a.workers)
    cpus = {int(c) for c in a.cpus.split(",")} if a.cpus else None
    srv = subprocess.Popen([os.path.abspath(binary)], env=env, stdout=subprocess.DEVNULL,
                           preexec_fn=(lambda: os.sched_setaffinity(0, cpus)) if cpus else None)
    try:
        base = f"http://127.0.0.1:{a.port}/api/v1"
        for _ in range(100):
            try:
                urllib.request.urlopen(urllib.request.Request(f"{base}/alive", headers=AUTH), timeout=1)
                break
            except OSError:
                if srv.poll() is not None:
                    raise RuntimeError(f"{binary} exited with code {srv.returncode}")
                time.sleep(0.1)
        req = urllib.request.Request(f"{base}/b/probe", data=b'{"quota_type":"NONE"}', method="POST", headers=AUTH)
        urllib.request.urlopen(req, timeout=5)

        payload = os.urandom(a.size)
        conn = Conn(a.port)
        ts = int(time.time() * 1e6)
        n, lat = 0, []
        cpu0, sc0 = cpu_s(srv.pid), write_syscalls(srv.pid)
        t0 = time.monotonic()
        stop = t0 + a.seconds
        while time.monotonic() < stop:
            if a.rate:
                wait = t0 + (n + 1) * a.size / (a.rate * 1e6) - time.monotonic()
                if wait > 0:
                    time.sleep(wait)
            r0 = time.monotonic()
            conn.post(f"/api/v1/b/probe/e{n % a.entries}?ts={ts + n}", payload)
            lat.append(time.monotonic() - r0)
            n += 1
        el = time.monotonic() - t0
        cpu, sc = cpu_s(srv.pid) - cpu0, write_syscalls(srv.pid) - sc0
        lat.sort()
        return {
            "MB/s": n * a.size / el / 1e6,
            "CPU us/record": 1e6 * cpu / n,
            "CPU s/GB": cpu / (n * a.size / 1e9),
            "write syscalls/record": sc / n,
            "peak RSS MB": peak_rss_mb(srv.pid),
            "p99 latency ms": 1e3 * lat[min(len(lat) - 1, int(0.99 * len(lat)))],
        }
    finally:
        srv.terminate()
        srv.wait()
        shutil.rmtree(data_dir, ignore_errors=True)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("base", help="reductstore binary to compare against (e.g. built from main)")
    ap.add_argument("changed", help="reductstore binary with the change")
    ap.add_argument("--size", type=int, default=65536, help="record size in bytes (default 65536)")
    ap.add_argument("--rate", type=float, default=0, help="offered MB/s, 0 = as fast as possible (default)")
    ap.add_argument("--entries", type=int, default=4, help="entries written round robin (default 4)")
    ap.add_argument("--seconds", type=float, default=15, help="length of each run (default 15)")
    ap.add_argument("--reps", type=int, default=5, help="runs per binary, alternating (default 5)")
    ap.add_argument("--cpus", help="pin the server to these CPUs, e.g. 2,3 (less noise)")
    ap.add_argument("--workers", type=int, help="TOKIO_WORKER_THREADS for the server")
    ap.add_argument("--port", type=int, default=8383)
    ap.add_argument("--data-dir", default="./ingest-probe-data",
                    help="data dir, recreated for every run; keep it on the disk you want to test, not tmpfs")
    a = ap.parse_args()

    runs = {"base": [], "changed": []}
    for r in range(a.reps):
        for side in ("base", "changed"):
            res = run_once(getattr(a, side), a, a.data_dir)
            runs[side].append(res)
            print(f"rep {r + 1} {side:8s} " + "  ".join(f"{k} {v:.2f}" for k, v in res.items()), file=sys.stderr)

    rate = f"{a.rate:g} MB/s" if a.rate else "max rate"
    print(f"\n{a.size} B records, {rate}, {a.entries} entries, median of {a.reps} runs of {a.seconds:g} s")
    print(f"{'':24s}{'base':>12s}{'changed':>12s}{'delta':>9s}")
    for k in runs["base"][0]:
        b = statistics.median(x[k] for x in runs["base"])
        c = statistics.median(x[k] for x in runs["changed"])
        d = f"{100 * (c - b) / b:+.1f}%" if b else ""
        print(f"{k:24s}{b:12.2f}{c:12.2f}{d:>9s}")


if __name__ == "__main__":
    main()
