#!/usr/bin/env python3
import statistics
import time
import subprocess
import json
import argparse
import sys
import uuid

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
