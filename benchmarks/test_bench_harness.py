import unittest
from unittest.mock import patch, MagicMock
import subprocess
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from bench_catalog_comparison import (
    compute_statistics,
    compute_speedup,
    run_in_container,
    benchmark_mkdir,
    benchmark_registration,
    benchmark_ils,
    benchmark_metadata,
    benchmark_query,
    cleanup_benchmark_data,
    run_benchmark_orchestrator,
    BENCHMARK_SCHEMA_KEYS,
    TIER_CONFIGS,
)

class TestBenchHarness(unittest.TestCase):
    def test_compute_statistics_seconds(self):
        durations_sec = [0.010, 0.012, 0.011, 0.013, 0.011, 0.012, 0.014, 0.010, 0.011, 0.012]
        stats = compute_statistics(durations_sec, num_ops=10, unit="s")
        self.assertAlmostEqual(stats["median_ms"], 11.5, delta=0.5)
        self.assertGreater(stats["p95_ms"], stats["median_ms"])
        self.assertGreater(stats["ops_per_sec"], 0)
        self.assertEqual(stats["count"], 10)
        self.assertEqual(stats["total_ops"], 10)

    def test_compute_statistics_nanoseconds(self):
        durations_ns = [10_000_000, 12_000_000, 11_000_000, 13_000_000, 11_000_000,
                        12_000_000, 14_000_000, 10_000_000, 11_000_000, 12_000_000]
        stats = compute_statistics(durations_ns, num_ops=10, unit="ns")
        self.assertAlmostEqual(stats["median_ms"], 11.5, delta=0.5)
        self.assertAlmostEqual(stats["median"], 11.5, delta=0.5)
        self.assertGreater(stats["p95_ms"], stats["median_ms"])
        self.assertGreater(stats["ops_per_sec"], 0)
        self.assertEqual(stats["count"], 10)

    def test_compute_statistics_milliseconds(self):
        durations_ms = [10.0, 12.0, 11.0, 13.0, 11.0, 12.0, 14.0, 10.0, 11.0, 12.0]
        stats = compute_statistics(durations_ms, num_ops=10, unit="ms")
        self.assertAlmostEqual(stats["median_ms"], 11.5, delta=0.5)
        self.assertAlmostEqual(stats["median"], 11.5, delta=0.5)
        self.assertGreater(stats["p95_ms"], stats["median_ms"])
        self.assertGreater(stats["ops_per_sec"], 0)
        self.assertEqual(stats["count"], 10)

    def test_compute_statistics_empty(self):
        stats = compute_statistics([], num_ops=0)
        self.assertEqual(stats["count"], 0)
        self.assertEqual(stats["total_ops"], 0)
        self.assertEqual(stats["mean"], 0.0)
        self.assertEqual(stats["mean_ms"], 0.0)
        self.assertEqual(stats["median"], 0.0)
        self.assertEqual(stats["median_ms"], 0.0)
        self.assertEqual(stats["p95"], 0.0)
        self.assertEqual(stats["p95_ms"], 0.0)
        self.assertEqual(stats["p99"], 0.0)
        self.assertEqual(stats["p99_ms"], 0.0)
        self.assertEqual(stats["stddev"], 0.0)
        self.assertEqual(stats["stddev_ms"], 0.0)
        self.assertEqual(stats["ops_per_sec"], 0.0)

    def test_compute_statistics_single_item(self):
        stats = compute_statistics([0.005], unit="s")
        self.assertEqual(stats["count"], 1)
        self.assertEqual(stats["total_ops"], 1)
        self.assertEqual(stats["stddev"], 0.0)
        self.assertEqual(stats["stddev_ms"], 0.0)
        self.assertAlmostEqual(stats["mean_ms"], 5.0, places=4)
        self.assertAlmostEqual(stats["median_ms"], 5.0, places=4)

    def test_compute_statistics_default_num_ops(self):
        stats = compute_statistics([0.010, 0.020])
        self.assertEqual(stats["count"], 2)
        self.assertEqual(stats["total_ops"], 2)

    def test_compute_statistics_precision_preservation(self):
        # Sub-microsecond duration: 200 nanoseconds = 0.0002 ms
        durations = [200]
        stats = compute_statistics(durations, unit="ns")
        self.assertEqual(stats["mean_ms"], 0.0002)
        self.assertGreater(stats["mean_ms"], 0.0)
        self.assertEqual(stats["median_ms"], 0.0002)

    def test_compute_statistics_invalid_unit(self):
        with self.assertRaises(ValueError):
            compute_statistics([1.0], unit="hours")

    def test_schema_keys_present(self):
        durations_sec = [0.005, 0.010, 0.015]
        stats = compute_statistics(durations_sec, num_ops=3)
        self.assertTrue(set(BENCHMARK_SCHEMA_KEYS).issubset(stats.keys()))

    def test_compute_speedup_mean_ms(self):
        pg_stats = {"mean_ms": 20.0, "ops_per_sec": 50.0}
        l3_stats = {"mean_ms": 5.0, "ops_per_sec": 200.0}
        speedup = compute_speedup(pg_stats, l3_stats)
        self.assertAlmostEqual(speedup, 4.0, places=2)

    def test_compute_speedup_mean(self):
        pg_stats = {"mean": 30.0, "ops_per_sec": 33.3}
        l3_stats = {"mean": 10.0, "ops_per_sec": 100.0}
        speedup = compute_speedup(pg_stats, l3_stats)
        self.assertAlmostEqual(speedup, 3.0, places=2)

    def test_compute_speedup_mixed_keys(self):
        pg_stats = {"mean_ms": 40.0}
        l3_stats = {"mean": 8.0}
        speedup = compute_speedup(pg_stats, l3_stats)
        self.assertAlmostEqual(speedup, 5.0, places=2)

        pg_stats2 = {"mean": 50.0}
        l3_stats2 = {"mean_ms": 10.0}
        speedup2 = compute_speedup(pg_stats2, l3_stats2)
        self.assertAlmostEqual(speedup2, 5.0, places=2)

    @patch("subprocess.run")
    def test_run_in_container_success(self, mock_run):
        mock_proc = MagicMock()
        mock_proc.stdout = "ils output"
        mock_proc.stderr = ""
        mock_proc.returncode = 0
        mock_run.return_value = mock_proc

        stdout, stderr, rc = run_in_container("my-container", "ils", as_irods=True, timeout=30.0)
        self.assertEqual(stdout, "ils output")
        self.assertEqual(stderr, "")
        self.assertEqual(rc, 0)
        mock_run.assert_called_once_with(
            ["docker", "exec", "my-container", "su", "-", "irods", "-c", "ils"],
            capture_output=True,
            text=True,
            timeout=30.0
        )

    @patch("subprocess.run")
    def test_run_in_container_timeout(self, mock_run):
        mock_run.side_effect = subprocess.TimeoutExpired(cmd="ils", timeout=15.0)

        stdout, stderr, rc = run_in_container("my-container", "ils", as_irods=False, timeout=15.0)
        self.assertEqual(stdout, "")
        self.assertIn("Command timed out after 15.0s", stderr)
        self.assertEqual(rc, -1)
        mock_run.assert_called_once_with(
            ["docker", "exec", "my-container", "bash", "-c", "ils"],
            capture_output=True,
            text=True,
            timeout=15.0
        )

    @patch("bench_catalog_comparison.run_in_container")
    def test_benchmark_mkdir(self, mock_run):
        mock_run.return_value = ("", "", 0)
        colls, durations = benchmark_mkdir("dummy-ctr", "/tempZone/home/rods/base", num_colls=3, depth=2)
        self.assertEqual(len(colls), 3)
        self.assertEqual(len(durations), 3)
        self.assertEqual(colls[0], "/tempZone/home/rods/base/coll_0")
        self.assertEqual(colls[1], "/tempZone/home/rods/base/coll_0/coll_1")
        self.assertEqual(colls[2], "/tempZone/home/rods/base/coll_2")
        # 1 imkdir -p for base, 3 imkdir for colls
        self.assertEqual(mock_run.call_count, 4)

    @patch("bench_catalog_comparison.run_in_container")
    def test_benchmark_registration(self, mock_run):
        mock_run.return_value = ("", "", 0)
        coll_list = ["/tempZone/home/rods/base/c0", "/tempZone/home/rods/base/c1"]
        objs, durations = benchmark_registration("dummy-ctr", "/tempZone/home/rods/base", coll_list, num_objects=4)
        self.assertEqual(len(objs), 4)
        self.assertEqual(len(durations), 4)
        self.assertEqual(objs[0], "/tempZone/home/rods/base/c0/bench_obj_0")
        self.assertEqual(objs[1], "/tempZone/home/rods/base/c1/bench_obj_1")
        self.assertEqual(objs[2], "/tempZone/home/rods/base/c0/bench_obj_2")
        self.assertEqual(objs[3], "/tempZone/home/rods/base/c1/bench_obj_3")
        self.assertEqual(mock_run.call_count, 4)

    @patch("bench_catalog_comparison.run_in_container")
    def test_benchmark_ils(self, mock_run):
        mock_run.return_value = ("ils output", "", 0)
        durations = benchmark_ils("dummy-ctr", "/tempZone/home/rods/base", recursive=True, samples=3)
        self.assertEqual(len(durations), 3)
        self.assertEqual(mock_run.call_count, 3)
        mock_run.assert_called_with("dummy-ctr", "ils -r '/tempZone/home/rods/base'")

    @patch("bench_catalog_comparison.run_in_container")
    def test_benchmark_metadata(self, mock_run):
        mock_run.return_value = ("", "", 0)
        obj_list = ["/base/obj0", "/base/obj1"]
        durations = benchmark_metadata("dummy-ctr", obj_list, avus_per_obj=2, sample_size=2)
        # 2 objects * 2 AVUs = 4 calls
        self.assertEqual(len(durations), 4)
        self.assertEqual(mock_run.call_count, 4)

    @patch("bench_catalog_comparison.run_in_container")
    def test_benchmark_query(self, mock_run):
        mock_run.return_value = ("query output", "", 0)
        result = benchmark_query("dummy-ctr", "/tempZone/home/rods/base", query_iterations=2)
        self.assertIn("avu_lookup", result)
        self.assertIn("wildcard_scan", result)
        self.assertIn("branching_join", result)
        self.assertIn("all", result)
        self.assertEqual(len(result["avu_lookup"]), 2)
        self.assertEqual(len(result["wildcard_scan"]), 2)
        self.assertEqual(len(result["branching_join"]), 2)
        self.assertEqual(len(result["all"]), 6)

    @patch("bench_catalog_comparison.run_in_container")
    def test_cleanup_benchmark_data(self, mock_run):
        mock_run.return_value = ("", "", 0)
        durations = cleanup_benchmark_data("dummy-ctr", "/tempZone/home/rods/base")
        self.assertEqual(len(durations), 1)
        self.assertGreater(mock_run.call_count, 0)

    def test_tier_configs(self):
        for tier in ["test_small", "1k", "10k", "50k"]:
            self.assertIn(tier, TIER_CONFIGS)
            cfg = TIER_CONFIGS[tier]
            self.assertIn("num_objects", cfg)
            self.assertIn("num_colls", cfg)
            self.assertIn("depth", cfg)
        self.assertEqual(TIER_CONFIGS["test_small"]["num_objects"], 10)
        self.assertEqual(TIER_CONFIGS["test_small"]["num_colls"], 3)
        self.assertEqual(TIER_CONFIGS["test_small"]["depth"], 2)

    @patch("bench_catalog_comparison.run_in_container")
    def test_orchestrator_dry_run(self, mock_run):
        mock_run.return_value = ("", "", 0)
        results = run_benchmark_orchestrator(
            backends=["l3kvg"],
            tiers=["test_small"],
            iterations=1,
            dry_run=True,
            query_iterations=1,
            metadata_sample=2,
        )
        self.assertIn("tiers", results)
        self.assertIn("test_small", results["tiers"])
        tier_data = results["tiers"]["test_small"]
        self.assertIn("backends", tier_data)
        self.assertIn("l3kvg", tier_data["backends"])
        l3_metrics = tier_data["backends"]["l3kvg"]
        for expected_key in ["mkdir", "registration", "ils", "metadata", "query", "cleanup"]:
            self.assertIn(expected_key, l3_metrics)
            self.assertTrue(set(BENCHMARK_SCHEMA_KEYS).issubset(l3_metrics[expected_key].keys()))

if __name__ == '__main__':
    unittest.main()
