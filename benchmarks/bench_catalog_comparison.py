#!/usr/bin/env python3
import statistics
import time
import subprocess
import json
import argparse
import sys
import uuid
import os

BENCHMARK_SCHEMA_KEYS = [
    "count",
    "total_ops",
    "mean",
    "mean_ms",
    "median",
    "median_ms",
    "p95",
    "p95_ms",
    "p99",
    "p99_ms",
    "stddev",
    "stddev_ms",
    "ops_per_sec",
]

def compute_statistics(durations, num_ops=None, unit="s"):
    """
    Computes statistical metrics over an array of operation durations.
    Supports durations in seconds (unit="s"), milliseconds (unit="ms"),
    or nanoseconds (unit="ns").
    """
    if unit not in ("s", "ns", "ms"):
        raise ValueError(f"Unsupported unit '{unit}'")

    if num_ops is None:
        num_ops = len(durations) if durations else 0

    if not durations:
        return {
            "count": 0,
            "total_ops": num_ops,
            "mean": 0.0,
            "mean_ms": 0.0,
            "median": 0.0,
            "median_ms": 0.0,
            "p95": 0.0,
            "p95_ms": 0.0,
            "p99": 0.0,
            "p99_ms": 0.0,
            "stddev": 0.0,
            "stddev_ms": 0.0,
            "ops_per_sec": 0.0,
        }

    if unit == "ns":
        sorted_ms = sorted([float(d) / 1_000_000.0 for d in durations])
        total_time_sec = sum(durations) / 1_000_000_000.0
    elif unit == "ms":
        sorted_ms = sorted([float(d) for d in durations])
        total_time_sec = sum(durations) / 1000.0
    else:  # unit == "s"
        sorted_ms = sorted([float(d) * 1000.0 for d in durations])
        total_time_sec = sum(durations)

    n = len(sorted_ms)
    mean_ms = statistics.mean(sorted_ms)
    median_ms = statistics.median(sorted_ms)
    stddev_ms = statistics.stdev(sorted_ms) if n > 1 else 0.0

    p95_idx = max(0, min(int(round(0.95 * n + 0.5)) - 1, n - 1))
    p99_idx = max(0, min(int(round(0.99 * n + 0.5)) - 1, n - 1))
    p95_ms = sorted_ms[p95_idx]
    p99_ms = sorted_ms[p99_idx]

    ops_per_sec = (num_ops / total_time_sec) if total_time_sec > 0 else 0.0

    return {
        "count": n,
        "total_ops": num_ops,
        "mean": round(mean_ms, 6),
        "mean_ms": round(mean_ms, 6),
        "median": round(median_ms, 6),
        "median_ms": round(median_ms, 6),
        "p95": round(p95_ms, 6),
        "p95_ms": round(p95_ms, 6),
        "p99": round(p99_ms, 6),
        "p99_ms": round(p99_ms, 6),
        "stddev": round(stddev_ms, 6),
        "stddev_ms": round(stddev_ms, 6),
        "ops_per_sec": round(ops_per_sec, 2),
    }

def compute_speedup(pg_stats, l3_stats):
    """
    Computes speedup factor as pg_latency / l3_latency.
    Handles dictionaries containing either 'mean_ms' or 'mean'.
    """
    pg_mean = pg_stats.get("mean_ms") if pg_stats.get("mean_ms") is not None else pg_stats.get("mean", 0.0)
    l3_mean = l3_stats.get("mean_ms") if l3_stats.get("mean_ms") is not None else l3_stats.get("mean", 0.0)
    if pg_mean and l3_mean and l3_mean > 0 and pg_mean > 0:
        return round(float(pg_mean) / float(l3_mean), 2)
    return 1.0

def run_in_container(container, cmd, as_irods=True, timeout=60.0):
    """
    Runs a shell command inside a docker container.
    """
    if as_irods:
        full_cmd = ["docker", "exec", container, "su", "-", "irods", "-c", cmd]
    else:
        full_cmd = ["docker", "exec", container, "bash", "-c", cmd]
    try:
        res = subprocess.run(full_cmd, capture_output=True, text=True, timeout=timeout)
        return res.stdout, res.stderr, res.returncode
    except subprocess.TimeoutExpired:
        return "", f"Command timed out after {timeout}s", -1


def run_python_in_container(container, code, timeout=300.0):
    """
    Executes a Python script inside a docker container under the 'irods' user.
    Pipes code directly via stdin to eliminate Docker CLI spawning overhead.
    """
    full_cmd = ["docker", "exec", "-i", container, "su", "-", "irods", "-c", "python3 -"]
    try:
        res = subprocess.run(full_cmd, input=code, capture_output=True, text=True, timeout=timeout)
        return res.stdout, res.stderr, res.returncode
    except subprocess.TimeoutExpired:
        return "", f"In-container Python script timed out after {timeout}s", -1


TIER_CONFIGS = {
    "test_small": {
        "num_objects": 10,
        "num_colls": 3,
        "depth": 2,
        "query_iterations": 5,
        "metadata_sample": 5,
        "ils_samples": 3,
    },
    "1k": {
        "num_objects": 1000,
        "num_colls": 50,
        "depth": 2,
        "query_iterations": 100,
        "metadata_sample": 20,
        "ils_samples": 5,
    },
    "10k": {
        "num_objects": 10000,
        "num_colls": 200,
        "depth": 5,
        "query_iterations": 10,
        "metadata_sample": 50,
        "ils_samples": 5,
    },
    "50k": {
        "num_objects": 50000,
        "num_colls": 1000,
        "depth": 10,
        "query_iterations": 5,
        "metadata_sample": 100,
        "ils_samples": 5,
    },
}

TIER_TIMEOUTS = {
    "test_small": 60.0,
    "1k": 900.0,
    "10k": 7200.0,
    "50k": 28800.0,
}

BACKEND_CONTAINERS = {
    "l3kvg": "ubuntu-2404-l3kvg-irods-catalog-provider-1",
    "postgres": "test-postgres-bench-irods-catalog-provider-1",
}


def generate_mkdir_worker_script(base_coll, num_colls, depth):
    return f"""
import sys, time, subprocess, json

base_coll = {json.dumps(base_coll)}
num_colls = {int(num_colls)}
depth = max(1, {int(depth)})

res_base = subprocess.run(["imkdir", "-p", base_coll], capture_output=True, text=True)
if res_base.returncode != 0:
    sys.stderr.write(f"imkdir -p failed on {{base_coll}}: {{res_base.stderr}}\\n")
    sys.exit(1)

colls_by_depth = {{0: [base_coll]}}
coll_list = []
durations = []

for i in range(num_colls):
    target_parent_depth = i % depth
    while target_parent_depth not in colls_by_depth or not colls_by_depth[target_parent_depth]:
        target_parent_depth = (target_parent_depth - 1) % depth
    parent_list = colls_by_depth[target_parent_depth]
    parent = parent_list[(i // depth) % len(parent_list)]
    node_depth = target_parent_depth + 1
    coll_path = f"{{parent}}/coll_{{i}}"

    t0 = time.perf_counter_ns()
    res = subprocess.run(["imkdir", coll_path], capture_output=True, text=True)
    t1 = time.perf_counter_ns()
    if res.returncode != 0:
        sys.stderr.write(f"imkdir failed for {{coll_path}}: {{res.stderr}}\\n")
        sys.exit(1)

    durations.append(t1 - t0)
    coll_list.append(coll_path)
    if node_depth not in colls_by_depth:
        colls_by_depth[node_depth] = []
    colls_by_depth[node_depth].append(coll_path)

print(json.dumps({{"coll_list": coll_list, "durations": durations}}))
"""


def generate_registration_worker_script(base_coll, coll_list, num_objects, bulk=None):
    if bulk is None:
        use_bulk = num_objects >= 1000
    else:
        use_bulk = bool(bulk)

    if not use_bulk:
        return f"""
import sys, time, subprocess, json

base_coll = {json.dumps(base_coll)}
coll_list = {json.dumps(coll_list)}
num_objects = {int(num_objects)}

target_colls = coll_list if coll_list else [base_coll]
obj_list = []
durations = []

for i in range(num_objects):
    target_coll = target_colls[i % len(target_colls)]
    obj_path = f"{{target_coll}}/bench_obj_{{i}}"

    t0 = time.perf_counter_ns()
    res = subprocess.run(["itouch", obj_path], capture_output=True, text=True)
    t1 = time.perf_counter_ns()
    if res.returncode != 0:
        sys.stderr.write(f"itouch failed for {{obj_path}}: {{res.stderr}}\\n")
        sys.exit(1)

    durations.append(t1 - t0)
    obj_list.append(obj_path)

print(json.dumps({{"obj_list": obj_list, "durations": durations}}))
"""

    return f"""
import sys, time, subprocess, json, os, uuid, shutil

base_coll = {json.dumps(base_coll)}
coll_list = {json.dumps(coll_list)}
num_objects = {int(num_objects)}

target_colls = coll_list if coll_list else [base_coll]
staging_dir = f"/tmp/staging_{{uuid.uuid4().hex}}"
os.makedirs(staging_dir, exist_ok=True)

obj_list = []
try:
    for i in range(num_objects):
        target_coll = target_colls[i % len(target_colls)]
        obj_name = f"bench_obj_{{i}}"
        obj_path = f"{{target_coll}}/{{obj_name}}"
        obj_list.append(obj_path)

        rel = os.path.relpath(target_coll, base_coll)
        loc_dir = staging_dir if rel == "." else os.path.join(staging_dir, rel)
        os.makedirs(loc_dir, exist_ok=True)
        loc_file = os.path.join(loc_dir, obj_name)
        open(loc_file, "wb").close()

    has_dirs = any(os.path.isdir(os.path.join(staging_dir, f)) for f in os.listdir(staging_dir))
    if has_dirs:
        cmd = f"iput -b -r {{staging_dir}}/* {{base_coll}}/"
    else:
        cmd = f"iput -b {{staging_dir}}/* {{base_coll}}/"

    t0 = time.perf_counter_ns()
    res = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    t1 = time.perf_counter_ns()

    if res.returncode != 0:
        sys.stderr.write(f"bulk iput failed: {{res.stderr}} {{res.stdout}}\\n")
        sys.exit(1)

    total_duration_ns = t1 - t0
    base_dur = total_duration_ns // num_objects if num_objects > 0 else 0
    rem = total_duration_ns % num_objects if num_objects > 0 else 0
    durations = [base_dur + 1 if j < rem else base_dur for j in range(num_objects)]

    print(json.dumps({{"obj_list": obj_list, "durations": durations}}))
finally:
    shutil.rmtree(staging_dir, ignore_errors=True)
"""



def generate_ils_worker_script(base_coll, recursive=True, samples=5):
    cmd_list = ["ils", "-r", base_coll] if recursive else ["ils", base_coll]
    return f"""
import sys, time, subprocess, json

base_coll = {json.dumps(base_coll)}
cmd = {json.dumps(cmd_list)}
samples = max(1, {int(samples)})

durations = []
for _ in range(samples):
    t0 = time.perf_counter_ns()
    res = subprocess.run(cmd, capture_output=True, text=True)
    t1 = time.perf_counter_ns()
    if res.returncode != 0:
        sys.stderr.write(f"ils failed for {{base_coll}}: {{res.stderr}}\\n")
        sys.exit(1)
    durations.append(t1 - t0)

print(json.dumps({{"durations": durations}}))
"""


def generate_metadata_worker_script(sample, avus_per_obj=3):
    return f"""
import sys, time, subprocess, json

sample = {json.dumps(sample)}
avus_per_obj = {int(avus_per_obj)}

durations = []
for obj in sample:
    for k in range(avus_per_obj):
        t0 = time.perf_counter_ns()
        res = subprocess.run(
            ["imeta", "add", "-d", obj, f"attr_{{k}}", f"val_{{k}}", f"unit_{{k}}"],
            capture_output=True,
            text=True
        )
        t1 = time.perf_counter_ns()
        if res.returncode != 0:
            sys.stderr.write(f"imeta add failed for {{obj}}: {{res.stderr}}\\n")
            sys.exit(1)
        durations.append(t1 - t0)

print(json.dumps({{"durations": durations}}))
"""


def generate_query_worker_script(base_coll, query_iterations=100, target_obj_name="bench_obj_0"):
    return f"""
import sys, time, subprocess, json

base_coll = {json.dumps(base_coll)}
target_obj_name = {json.dumps(target_obj_name)}
query_iterations = max(1, {int(query_iterations)})

# Detect imeta qu support
test_qu = subprocess.run(["imeta", "qu", "-d", "attr_0", "=", "val_0"], capture_output=True, text=True)
if test_qu.returncode == 0 or ("Unrecognized subcommand" not in test_qu.stderr and "Unrecognized subcommand" not in test_qu.stdout):
    cmd_a = ["imeta", "qu", "-d", "attr_0", "=", "val_0"]
else:
    cmd_a = ["iquest", "%s/%s", f"SELECT COLL_NAME, DATA_NAME WHERE COLL_NAME LIKE '{{base_coll}}%' AND META_DATA_ATTR_NAME = 'attr_0' AND META_DATA_ATTR_VALUE = 'val_0'"]

cmd_b = ["iquest", "%s/%s", f"SELECT COLL_NAME, DATA_NAME WHERE COLL_NAME LIKE '{{base_coll}}%' AND DATA_NAME LIKE 'bench_obj_%'"]
cmd_c = ["iquest", "%s/%s/%s/%s", f"SELECT COLL_NAME, DATA_NAME, RESC_NAME, DATA_REPL_NUM WHERE COLL_NAME LIKE '{{base_coll}}%' AND DATA_NAME = '{{target_obj_name}}'"]

durations_a, durations_b, durations_c, durations_all = [], [], [], []

for _ in range(query_iterations):
    # a) AVU exact match lookup
    t0 = time.perf_counter_ns()
    res_a = subprocess.run(cmd_a, capture_output=True, text=True)
    t1 = time.perf_counter_ns()
    if res_a.returncode != 0 and "CAT_NO_ROWS_FOUND" not in res_a.stderr and "CAT_NO_ROWS_FOUND" not in res_a.stdout:
        sys.stderr.write(f"Query a failed with rc={{res_a.returncode}}: {{res_a.stderr}} {{res_a.stdout}}\\n")
        sys.exit(1)
    durations_a.append(t1 - t0)
    durations_all.append(t1 - t0)

    # b) GenQuery wildcard scan
    t0 = time.perf_counter_ns()
    res_b = subprocess.run(cmd_b, capture_output=True, text=True)
    t1 = time.perf_counter_ns()
    if res_b.returncode != 0 and "CAT_NO_ROWS_FOUND" not in res_b.stderr and "CAT_NO_ROWS_FOUND" not in res_b.stdout:
        sys.stderr.write(f"Query b failed with rc={{res_b.returncode}}: {{res_b.stderr}} {{res_b.stdout}}\\n")
        sys.exit(1)
    durations_b.append(t1 - t0)
    durations_all.append(t1 - t0)

    # c) Branching join
    t0 = time.perf_counter_ns()
    res_c = subprocess.run(cmd_c, capture_output=True, text=True)
    t1 = time.perf_counter_ns()
    if res_c.returncode != 0 and "CAT_NO_ROWS_FOUND" not in res_c.stderr and "CAT_NO_ROWS_FOUND" not in res_c.stdout:
        sys.stderr.write(f"Query c failed with rc={{res_c.returncode}}: {{res_c.stderr}} {{res_c.stdout}}\\n")
        sys.exit(1)
    durations_c.append(t1 - t0)
    durations_all.append(t1 - t0)

print(json.dumps({{"avu_lookup": durations_a, "wildcard_scan": durations_b, "branching_join": durations_c, "all": durations_all}}))
"""


def benchmark_mkdir(container, base_coll, num_colls, depth, timeout=60.0):
    """
    Creates a deterministic directory tree with num_colls collections down to depth depth under base_coll.
    Runs via an in-container Python worker to isolate pure catalog execution latencies.
    Returns list of created collection paths and timing array.
    """
    if num_colls <= 0:
        return [], []

    code = generate_mkdir_worker_script(base_coll, num_colls, depth)
    out, err, rc = run_python_in_container(container, code, timeout=timeout)
    if rc != 0:
        raise RuntimeError(f"imkdir benchmark failed in container: {err} {out}")

    data = json.loads(out)
    return data["coll_list"], data["durations"]


def benchmark_registration(container, base_coll, coll_list, num_objects, timeout=60.0, bulk=None):
    """
    Distributes num_objects across coll_list using zero-byte files.
    When bulk=True or num_objects >= 1000, stages files locally and ingests via iput -b -r.
    Otherwise uses sequential itouch.
    Runs via an in-container Python worker to isolate pure catalog execution latencies.
    Returns list of object paths and timing array.
    """
    if num_objects <= 0:
        return [], []

    code = generate_registration_worker_script(base_coll, coll_list, num_objects, bulk=bulk)
    out, err, rc = run_python_in_container(container, code, timeout=timeout)
    if rc != 0:
        raise RuntimeError(f"registration benchmark failed in container: {err} {out}")

    data = json.loads(out)
    return data["obj_list"], data["durations"]


def benchmark_ils(container, base_coll, recursive=True, samples=5, timeout=60.0):
    """
    Executes recursive directory tree walk (ils -r <base_coll>) and measures total traversal duration.
    Runs via an in-container Python worker.
    Returns timing array of sample durations in nanoseconds.
    """
    code = generate_ils_worker_script(base_coll, recursive=recursive, samples=samples)
    out, err, rc = run_python_in_container(container, code, timeout=timeout)
    if rc != 0:
        raise RuntimeError(f"ils benchmark failed in container: {err} {out}")

    data = json.loads(out)
    return data["durations"]


def benchmark_metadata(container, obj_list, avus_per_obj=3, sample_size=None, timeout=60.0):
    """
    For a sample of objects, adds AVUs (imeta add -d <obj> attr_<k> val_<k> unit_<k>).
    Runs via an in-container Python worker.
    Returns timing array of durations in nanoseconds.
    """
    if not obj_list:
        return []

    if sample_size is not None:
        sample = obj_list[:sample_size]
    elif len(obj_list) > 20:
        sample = obj_list[:20]
    else:
        sample = obj_list

    code = generate_metadata_worker_script(sample, avus_per_obj=avus_per_obj)
    out, err, rc = run_python_in_container(container, code, timeout=timeout)
    if rc != 0:
        raise RuntimeError(f"metadata benchmark failed in container: {err} {out}")

    data = json.loads(out)
    return data["durations"]


def benchmark_query(container, base_coll, query_iterations=100, target_obj_name="bench_obj_0", timeout=60.0):
    """
    Executes collection-scoped queries inside the container:
      a) imeta qu -d attr_0 = val_0 (AVU lookup; fallback to scoped iquest)
      b) iquest "%s/%s" "SELECT COLL_NAME, DATA_NAME WHERE COLL_NAME LIKE '{base_coll}%' AND DATA_NAME LIKE 'bench_obj_%'"
      c) iquest "%s/%s/%s/%s" "SELECT COLL_NAME, DATA_NAME, RESC_NAME, DATA_REPL_NUM WHERE COLL_NAME LIKE '{base_coll}%' AND DATA_NAME = '{target_obj_name}'"
    Asserts returncode == 0 for all query executions (allowing CAT_NO_ROWS_FOUND).
    Returns dict of timing arrays in nanoseconds.
    """
    code = generate_query_worker_script(base_coll, query_iterations, target_obj_name)
    out, err, rc = run_python_in_container(container, code, timeout=timeout)
    if rc != 0:
        raise RuntimeError(f"query benchmark failed in container: {err} {out}")

    data = json.loads(out)
    return data


def cleanup_benchmark_data(container, base_coll, timeout=60.0):
    """
    Runs irm -rf <base_coll> and irmtrash -M -f (with echo y | irmtrash -M fallback) to cleanly reset catalog state.
    Asserts returncode == 0 and prevents stdin blocking.
    Returns timing array with cleanup duration in nanoseconds.
    """
    durations = []
    t0 = time.perf_counter_ns()

    out_rm, err_rm, rc_rm = run_in_container(container, f"irm -rf '{base_coll}'", timeout=timeout)
    if rc_rm != 0:
        raise RuntimeError(f"irm -rf failed on {base_coll}: {err_rm} {out_rm}")

    out_trash, err_trash, rc_trash = run_in_container(container, "irmtrash -M -f", timeout=timeout)
    if rc_trash != 0:
        out_trash2, err_trash2, rc_trash2 = run_in_container(
            container, "echo y | irmtrash -M", timeout=timeout
        )
        if rc_trash2 != 0:
            raise RuntimeError(f"irmtrash failed on {base_coll}: {err_trash2} {out_trash2}")

    t1 = time.perf_counter_ns()
    durations.append(t1 - t0)
    return durations


def run_benchmark_orchestrator(
    backends,
    tiers,
    iterations=5,
    output_file="benchmark_results.json",
    dry_run=False,
    query_iterations=None,
    metadata_sample=None,
    bulk=None,
):
    """
    Orchestrates execution of macro workloads across specified backends and tiers.
    """
    selected_backends = []
    for b in backends:
        b_clean = b.strip().lower()
        if b_clean == "both":
            for default_b in ["postgres", "l3kvg"]:
                if default_b not in selected_backends:
                    selected_backends.append(default_b)
        elif b_clean in BACKEND_CONTAINERS:
            if b_clean not in selected_backends:
                selected_backends.append(b_clean)
        else:
            raise ValueError(f"Unknown backend '{b}'. Supported: 'l3kvg', 'postgres', 'both'")

    existing_data = {}
    if not dry_run and os.path.exists(output_file):
        try:
            with open(output_file, "r") as f:
                existing_data = json.load(f)
        except Exception as e:
            print(f"[WARN] Could not parse existing results file '{output_file}': {e}. Starting fresh.")
            existing_data = {}

    if "metadata" not in existing_data:
        existing_data["metadata"] = {}
    existing_data["metadata"]["timestamp"] = time.time()
    existing_data["metadata"]["iterations"] = iterations
    if "tiers" not in existing_data:
        existing_data["tiers"] = {}

    for tier in tiers:
        if tier not in TIER_CONFIGS:
            raise ValueError(f"Unknown tier '{tier}'. Supported tiers: {list(TIER_CONFIGS.keys())}")
        tier_cfg = TIER_CONFIGS[tier].copy()
        tier_timeout = TIER_TIMEOUTS.get(tier, 120.0)
        q_iters = query_iterations if query_iterations is not None else tier_cfg["query_iterations"]
        m_sample = metadata_sample if metadata_sample is not None else tier_cfg["metadata_sample"]
        ils_samps = tier_cfg.get("ils_samples", 5)

        print(f"\n{'='*70}")
        print(f"Starting Tier: '{tier}' ({tier_cfg['num_objects']} objects, {tier_cfg['num_colls']} colls, depth {tier_cfg['depth']})")
        print(f"Backends to benchmark: {selected_backends} | Iterations: {iterations} | Scale Timeout: {tier_timeout}s")
        print(f"{'='*70}")

        if tier not in existing_data["tiers"]:
            existing_data["tiers"][tier] = {
                "config": tier_cfg,
                "backends": {},
                "speedup": {},
            }

        tier_entry = existing_data["tiers"][tier]

        for backend in selected_backends:
            container = BACKEND_CONTAINERS[backend]
            print(f"\n>>> Running Backend: {backend.upper()} (container: {container})")

            mkdir_all = []
            reg_all = []
            ils_all = []
            meta_all = []
            q_avu_all = []
            q_wildcard_all = []
            q_join_all = []
            q_all = []
            cleanup_all = []

            for it in range(1, iterations + 1):
                base_coll = f"/tempZone/home/rods/bench_{uuid.uuid4().hex[:8]}"
                print(f"  Iteration {it}/{iterations} on {backend} (base_coll: {base_coll})...")

                # Pre-cleanup
                cleanup_benchmark_data(container, base_coll, timeout=tier_timeout)

                # 1. mkdir
                colls, d_mkdir = benchmark_mkdir(
                    container, base_coll, tier_cfg["num_colls"], tier_cfg["depth"], timeout=tier_timeout
                )
                mkdir_all.extend(d_mkdir)

                # 2. registration
                objs, d_reg = benchmark_registration(
                    container, base_coll, colls, tier_cfg["num_objects"], timeout=tier_timeout, bulk=bulk
                )
                reg_all.extend(d_reg)

                # 3. ils
                d_ils = benchmark_ils(container, base_coll, recursive=True, samples=ils_samps, timeout=tier_timeout)
                ils_all.extend(d_ils)

                # 4. metadata
                d_meta = benchmark_metadata(
                    container, objs, avus_per_obj=3, sample_size=m_sample, timeout=tier_timeout
                )
                meta_all.extend(d_meta)

                # 5. query
                d_query = benchmark_query(container, base_coll, query_iterations=q_iters, timeout=tier_timeout)
                q_avu_all.extend(d_query["avu_lookup"])
                q_wildcard_all.extend(d_query["wildcard_scan"])
                q_join_all.extend(d_query["branching_join"])
                q_all.extend(d_query["all"])

                # 6. cleanup
                d_clean = cleanup_benchmark_data(container, base_coll, timeout=tier_timeout)
                cleanup_all.extend(d_clean)

            # Compute statistics for backend workloads
            stats_mkdir = compute_statistics(mkdir_all, num_ops=len(mkdir_all), unit="ns")
            stats_reg = compute_statistics(reg_all, num_ops=len(reg_all), unit="ns")
            stats_ils = compute_statistics(ils_all, num_ops=len(ils_all), unit="ns")
            stats_meta = compute_statistics(meta_all, num_ops=len(meta_all), unit="ns")
            stats_q_all = compute_statistics(q_all, num_ops=len(q_all), unit="ns")
            stats_q_avu = compute_statistics(q_avu_all, num_ops=len(q_avu_all), unit="ns")
            stats_q_wildcard = compute_statistics(q_wildcard_all, num_ops=len(q_wildcard_all), unit="ns")
            stats_q_join = compute_statistics(q_join_all, num_ops=len(q_join_all), unit="ns")
            stats_clean = compute_statistics(cleanup_all, num_ops=len(cleanup_all), unit="ns")

            stats_q_all["sub_workloads"] = {
                "avu_lookup": stats_q_avu,
                "wildcard_scan": stats_q_wildcard,
                "branching_join": stats_q_join,
            }

            backend_metrics = {
                "mkdir": stats_mkdir,
                "collection_ingestion": stats_mkdir,
                "registration": stats_reg,
                "ils": stats_ils,
                "listing": stats_ils,
                "metadata": stats_meta,
                "query": stats_q_all,
                "genquery": stats_q_all,
                "cleanup": stats_clean,
            }

            tier_entry["backends"][backend] = backend_metrics
            tier_entry[backend] = backend_metrics

            # Print summary table for this backend
            print(f"\nResults for {backend.upper()} on Tier '{tier}':")
            print(f"{'-'*75}")
            print(f"{'Workload':<22} | {'Mean (ms)':<10} | {'Median (ms)':<11} | {'P95 (ms)':<9} | {'Ops/sec':<9}")
            print(f"{'-'*75}")
            for name, key in [
                ("mkdir", "mkdir"),
                ("registration", "registration"),
                ("ils", "ils"),
                ("metadata", "metadata"),
                ("query (overall)", "query"),
                ("cleanup", "cleanup"),
            ]:
                st = backend_metrics[key]
                print(f"{name:<22} | {st['mean_ms']:<10.3f} | {st['median_ms']:<11.3f} | {st['p95_ms']:<9.3f} | {st['ops_per_sec']:<9.2f}")
                if key == "query" and "sub_workloads" in st:
                    for sub_name, sub_st in st["sub_workloads"].items():
                        print(f"  - {sub_name:<18} | {sub_st['mean_ms']:<10.3f} | {sub_st['median_ms']:<11.3f} | {sub_st['p95_ms']:<9.3f} | {sub_st['ops_per_sec']:<9.2f}")
            print(f"{'-'*75}")

        # Compute speedups if both backends exist in this tier
        if "postgres" in tier_entry["backends"] and "l3kvg" in tier_entry["backends"]:
            pg_wks = tier_entry["backends"]["postgres"]
            l3_wks = tier_entry["backends"]["l3kvg"]
            speedups = {}
            for wk in ["mkdir", "collection_ingestion", "registration", "ils", "listing", "metadata", "query", "genquery", "cleanup"]:
                if wk in pg_wks and wk in l3_wks:
                    speedups[wk] = compute_speedup(pg_wks[wk], l3_wks[wk])

            # Query sub-workload speedups
            query_sub_speedups = {}
            pg_q_subs = pg_wks.get("query", {}).get("sub_workloads", {})
            l3_q_subs = l3_wks.get("query", {}).get("sub_workloads", {})
            for sub_k in ["avu_lookup", "wildcard_scan", "branching_join"]:
                if sub_k in pg_q_subs and sub_k in l3_q_subs:
                    query_sub_speedups[sub_k] = compute_speedup(pg_q_subs[sub_k], l3_q_subs[sub_k])
            speedups["query_sub_workloads"] = query_sub_speedups
            tier_entry["speedup"] = speedups

            print(f"\nSpeedup Factors (PostgreSQL Latency / L3KVG Latency):")
            print(f"{'-'*45}")
            for wk in ["mkdir", "registration", "ils", "metadata", "query", "cleanup"]:
                if wk in speedups:
                    print(f"  {wk:<20}: {speedups[wk]:.2f}x")
            if "query_sub_workloads" in speedups:
                for sub_k, sub_val in speedups["query_sub_workloads"].items():
                    print(f"    - {sub_k:<18}: {sub_val:.2f}x")
            print(f"{'-'*45}")

        # Also expose tier at root for convenience
        existing_data[tier] = tier_entry

    if dry_run:
        print("\n[DRY RUN] Execution completed successfully. Output JSON not written to file.")
    else:
        # Measure footprint and record in existing_data
        fp = measure_footprint()
        existing_data["footprint"] = fp

        with open(output_file, "w") as f:
            json.dump(existing_data, f, indent=2)
        print(f"\n[INFO] Benchmark results saved to: {output_file}")

    return existing_data


def measure_footprint():
    """
    Measures on-disk database sizes and memory RSS for L3KVG and PostgreSQL containers.
    Returns dict containing footprint metrics.
    """
    footprint = {
        "disk_bytes": {},
        "disk_human": {},
        "rss_kb": {},
    }

    # 1. On-disk database sizes
    # L3KVG iRODS provider directory
    out, _, rc = run_in_container("ubuntu-2404-l3kvg-irods-catalog-provider-1", "du -sh /var/lib/irods/", as_irods=False)
    if rc == 0 and out.strip():
        footprint["disk_human"]["l3kvg_irods"] = out.strip().split()[0]
    out, _, rc = run_in_container("ubuntu-2404-l3kvg-irods-catalog-provider-1", "du -sb /var/lib/irods/", as_irods=False)
    if rc == 0 and out.strip():
        try:
            footprint["disk_bytes"]["l3kvg_irods"] = int(out.strip().split()[0])
        except (ValueError, IndexError):
            pass

    # L3KVG database directory
    out, _, rc = run_in_container("ubuntu-2404-l3kvg-irods-catalog-provider-1", "du -sh /var/lib/irods/l3kvg_db", as_irods=False)
    if rc == 0 and out.strip():
        footprint["disk_human"]["l3kvg_db"] = out.strip().split()[0]
    out, _, rc = run_in_container("ubuntu-2404-l3kvg-irods-catalog-provider-1", "du -sb /var/lib/irods/l3kvg_db", as_irods=False)
    if rc == 0 and out.strip():
        try:
            footprint["disk_bytes"]["l3kvg_db"] = int(out.strip().split()[0])
        except (ValueError, IndexError):
            pass

    # L3KVG catalog container (if separate volume exists)
    out, _, rc = run_in_container("ubuntu-2404-l3kvg-catalog-1", "du -sh /var/lib/l3kvg/", as_irods=False)
    if rc == 0 and out.strip():
        footprint["disk_human"]["l3kvg_catalog"] = out.strip().split()[0]
    elif rc != 0:
        footprint["disk_human"]["l3kvg_catalog"] = "embedded_in_provider"

    # PostgreSQL database data directory
    out, _, rc = run_in_container("test-postgres-bench-catalog-1", "du -sh /var/lib/postgresql/data/", as_irods=False)
    if rc == 0 and out.strip():
        footprint["disk_human"]["postgres_data"] = out.strip().split()[0]
    out, _, rc = run_in_container("test-postgres-bench-catalog-1", "du -sb /var/lib/postgresql/data/", as_irods=False)
    if rc == 0 and out.strip():
        try:
            footprint["disk_bytes"]["postgres_data"] = int(out.strip().split()[0])
        except (ValueError, IndexError):
            pass

    # 2. Peak / Current RSS memory
    def get_container_rss(container, target_comms):
        out, _, rc = run_in_container(container, "ps -eo pid,rss,comm", as_irods=False)
        res = {}
        if rc == 0:
            for line in out.splitlines()[1:]:
                parts = line.strip().split(None, 2)
                if len(parts) == 3:
                    _, rss_str, comm = parts
                    if comm in target_comms:
                        try:
                            res.setdefault(comm, []).append(int(rss_str))
                        except ValueError:
                            pass
        return res

    footprint["rss_kb"]["l3kvg"] = get_container_rss(
        "ubuntu-2404-l3kvg-irods-catalog-provider-1",
        ["l3kvg_server", "irodsServer", "irodsAgent"]
    )
    footprint["rss_kb"]["postgres_server"] = get_container_rss(
        "test-postgres-bench-catalog-1",
        ["postgres"]
    )
    footprint["rss_kb"]["postgres_irods_provider"] = get_container_rss(
        "test-postgres-bench-irods-catalog-provider-1",
        ["irodsServer", "irodsAgent"]
    )

    return footprint


def record_footprint(output_file):
    """
    Measures footprint and writes under 'footprint' in output_file JSON.
    """
    fp = measure_footprint()
    data = {}
    if os.path.exists(output_file):
        try:
            with open(output_file, "r") as f:
                data = json.load(f)
        except Exception:
            data = {}
    data["footprint"] = fp
    with open(output_file, "w") as f:
        json.dump(data, f, indent=2)
    print(f"\n[INFO] Footprint measurements recorded in {output_file}:")
    print(json.dumps(fp, indent=2))
    return fp


def main():
    parser = argparse.ArgumentParser(description="iRODS Catalog Benchmark Harness (L3KVG vs PostgreSQL)")
    parser.add_argument(
        "--tiers",
        default="test_small",
        help="Comma-separated tiers: test_small, 1k, 10k, 50k (default: test_small)",
    )
    parser.add_argument(
        "--backends",
        default="both",
        help="Comma-separated backends: l3kvg, postgres, both (default: both)",
    )
    parser.add_argument(
        "--iterations",
        type=int,
        default=5,
        help="Number of benchmark iterations (default: 5)",
    )
    parser.add_argument(
        "--output",
        default="benchmark_results.json",
        help="Output filepath for benchmark results JSON (default: benchmark_results.json)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Flag to run 1 iteration of small sample and print output without saving",
    )
    parser.add_argument(
        "--query-iterations",
        type=int,
        default=None,
        help="Optional override for number of query iterations per run",
    )
    parser.add_argument(
        "--metadata-sample",
        type=int,
        default=None,
        help="Optional override for metadata object sample size",
    )
    parser.add_argument(
        "--bulk",
        action="store_true",
        default=None,
        help="Force bulk registration via iput -b -r",
    )
    parser.add_argument(
        "--no-bulk",
        action="store_false",
        dest="bulk",
        help="Disable bulk registration and use sequential itouch",
    )
    parser.add_argument(
        "--footprint",
        action="store_true",
        help="Measure on-disk sizes and memory RSS, record to output file under 'footprint', and exit",
    )

    args = parser.parse_args()

    if args.footprint:
        record_footprint(args.output)
        return

    tier_list = [t.strip() for t in args.tiers.split(",") if t.strip()]
    backend_list = [b.strip() for b in args.backends.split(",") if b.strip()]
    iterations = 1 if args.dry_run else args.iterations

    run_benchmark_orchestrator(
        backends=backend_list,
        tiers=tier_list,
        iterations=iterations,
        output_file=args.output,
        dry_run=args.dry_run,
        query_iterations=args.query_iterations,
        metadata_sample=args.metadata_sample,
        bulk=args.bulk,
    )


if __name__ == "__main__":
    main()
