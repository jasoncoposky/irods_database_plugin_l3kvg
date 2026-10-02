import unittest
from unittest.mock import patch, MagicMock
import subprocess
from bench_catalog_comparison import (
    compute_statistics,
    compute_speedup,
    run_in_container,
    BENCHMARK_SCHEMA_KEYS,
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

if __name__ == '__main__':
    unittest.main()
