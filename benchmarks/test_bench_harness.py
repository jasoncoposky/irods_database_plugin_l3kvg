import unittest
from unittest.mock import patch, MagicMock
import subprocess
import os
import sys
import json

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from bench_catalog_comparison import (
    compute_statistics,
    compute_speedup,
    run_in_container,
    run_python_in_container,
    generate_mkdir_worker_script,
    generate_registration_worker_script,
    generate_ils_worker_script,
    generate_metadata_worker_script,
    generate_query_worker_script,
    benchmark_mkdir,
    benchmark_registration,
    benchmark_ils,
    benchmark_metadata,
    benchmark_query,
    cleanup_benchmark_data,
    run_benchmark_orchestrator,
    measure_footprint,
    record_footprint,
    BENCHMARK_SCHEMA_KEYS,
    TIER_CONFIGS,
    TIER_TIMEOUTS,
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

    @patch("subprocess.run")
    def test_run_python_in_container_success(self, mock_run):
        mock_proc = MagicMock()
        mock_proc.stdout = '{"status": "ok"}'
        mock_proc.stderr = ""
        mock_proc.returncode = 0
        mock_run.return_value = mock_proc

        stdout, stderr, rc = run_python_in_container("my-container", "print('hi')", timeout=30.0)
        self.assertEqual(stdout, '{"status": "ok"}')
        self.assertEqual(stderr, "")
        self.assertEqual(rc, 0)
        mock_run.assert_called_once_with(
            ["docker", "exec", "-i", "my-container", "su", "-", "irods", "-c", "python3 -"],
            input="print('hi')",
            capture_output=True,
            text=True,
            timeout=30.0,
        )

    @patch("subprocess.run")
    def test_run_python_in_container_timeout(self, mock_run):
        mock_run.side_effect = subprocess.TimeoutExpired(cmd="python3", timeout=10.0)
        stdout, stderr, rc = run_python_in_container("my-container", "code", timeout=10.0)
        self.assertEqual(stdout, "")
        self.assertIn("timed out after 10.0s", stderr)
        self.assertEqual(rc, -1)

    def test_worker_script_generators(self):
        query_code = generate_query_worker_script("/tempZone/home/rods/base", query_iterations=5, target_obj_name="bench_obj_0")
        self.assertIn("WHERE COLL_NAME LIKE '{base_coll}%'", query_code)
        self.assertIn("CAT_NO_ROWS_FOUND", query_code)
        self.assertIn("sys.exit(1)", query_code)

        mkdir_code = generate_mkdir_worker_script("/tempZone/home/rods/base", num_colls=3, depth=2)
        self.assertIn("imkdir", mkdir_code)
        self.assertIn("colls_by_depth", mkdir_code)

        reg_code = generate_registration_worker_script("/tempZone/home/rods/base", ["/c0"], num_objects=3)
        self.assertIn("itouch", reg_code)

        ils_code = generate_ils_worker_script("/tempZone/home/rods/base", recursive=True, samples=3)
        self.assertIn("ils", ils_code)

        meta_code = generate_metadata_worker_script(["/base/obj0"], avus_per_obj=2)
        self.assertIn("imeta", meta_code)

    @patch("bench_catalog_comparison.run_python_in_container")
    def test_benchmark_mkdir_success(self, mock_run_py):
        mock_run_py.return_value = (
            '{"coll_list": ["/base/c0", "/base/c0/c1", "/base/c2"], "durations": [100, 200, 300]}',
            "",
            0,
        )
        colls, durations = benchmark_mkdir("dummy-ctr", "/base", num_colls=3, depth=2)
        self.assertEqual(len(colls), 3)
        self.assertEqual(len(durations), 3)
        self.assertEqual(colls[0], "/base/c0")
        self.assertEqual(durations[0], 100)

    @patch("bench_catalog_comparison.run_python_in_container")
    def test_benchmark_mkdir_error(self, mock_run_py):
        mock_run_py.return_value = ("", "imkdir -p failed: Permission denied", 1)
        with self.assertRaises(RuntimeError) as ctx:
            benchmark_mkdir("dummy-ctr", "/base", num_colls=3, depth=2)
        self.assertIn("Permission denied", str(ctx.exception))

    @patch("bench_catalog_comparison.run_python_in_container")
    def test_benchmark_registration_success(self, mock_run_py):
        mock_run_py.return_value = (
            '{"obj_list": ["/base/c0/bench_obj_0", "/base/c1/bench_obj_1"], "durations": [150, 160]}',
            "",
            0,
        )
        objs, durations = benchmark_registration("dummy-ctr", "/base", ["/base/c0", "/base/c1"], num_objects=2)
        self.assertEqual(len(objs), 2)
        self.assertEqual(len(durations), 2)

    @patch("bench_catalog_comparison.run_python_in_container")
    def test_benchmark_registration_error(self, mock_run_py):
        mock_run_py.return_value = ("", "itouch failed: catalog connection refused", 1)
        with self.assertRaises(RuntimeError) as ctx:
            benchmark_registration("dummy-ctr", "/base", ["/base/c0"], num_objects=2)
        self.assertIn("connection refused", str(ctx.exception))

    @patch("bench_catalog_comparison.run_python_in_container")
    def test_benchmark_ils_success(self, mock_run_py):
        mock_run_py.return_value = ('{"durations": [400, 420, 410]}', "", 0)
        durations = benchmark_ils("dummy-ctr", "/base", recursive=True, samples=3)
        self.assertEqual(len(durations), 3)
        self.assertEqual(durations[0], 400)

    @patch("bench_catalog_comparison.run_python_in_container")
    def test_benchmark_ils_error(self, mock_run_py):
        mock_run_py.return_value = ("", "ils failed: collection not found", 1)
        with self.assertRaises(RuntimeError) as ctx:
            benchmark_ils("dummy-ctr", "/base", recursive=True, samples=3)
        self.assertIn("collection not found", str(ctx.exception))

    @patch("bench_catalog_comparison.run_python_in_container")
    def test_benchmark_metadata_success(self, mock_run_py):
        mock_run_py.return_value = ('{"durations": [250, 260, 270]}', "", 0)
        durations = benchmark_metadata("dummy-ctr", ["/base/obj0"], avus_per_obj=3)
        self.assertEqual(len(durations), 3)

    @patch("bench_catalog_comparison.run_python_in_container")
    def test_benchmark_metadata_error(self, mock_run_py):
        mock_run_py.return_value = ("", "imeta add failed: duplicate attribute", 1)
        with self.assertRaises(RuntimeError) as ctx:
            benchmark_metadata("dummy-ctr", ["/base/obj0"], avus_per_obj=1)
        self.assertIn("duplicate attribute", str(ctx.exception))

    @patch("bench_catalog_comparison.run_python_in_container")
    def test_benchmark_query_success(self, mock_run_py):
        mock_run_py.return_value = (
            '{"avu_lookup": [100, 110], "wildcard_scan": [200, 210], "branching_join": [300, 310], "all": [100, 200, 300, 110, 210, 310]}',
            "",
            0,
        )
        result = benchmark_query("dummy-ctr", "/base", query_iterations=2)
        self.assertIn("avu_lookup", result)
        self.assertIn("wildcard_scan", result)
        self.assertIn("branching_join", result)
        self.assertIn("all", result)
        self.assertEqual(len(result["avu_lookup"]), 2)
        self.assertEqual(len(result["wildcard_scan"]), 2)
        self.assertEqual(len(result["branching_join"]), 2)
        self.assertEqual(len(result["all"]), 6)

    @patch("bench_catalog_comparison.run_python_in_container")
    def test_benchmark_query_error_validation(self, mock_run_py):
        mock_run_py.return_value = ("", "Query a failed with rc=4: CAT_SQL_ERR", 1)
        with self.assertRaises(RuntimeError) as ctx:
            benchmark_query("dummy-ctr", "/base", query_iterations=2)
        self.assertIn("CAT_SQL_ERR", str(ctx.exception))

    @patch("bench_catalog_comparison.run_in_container")
    def test_cleanup_benchmark_data_success(self, mock_run):
        mock_run.return_value = ("", "", 0)
        durations = cleanup_benchmark_data("dummy-ctr", "/base")
        self.assertEqual(len(durations), 1)
        self.assertGreater(mock_run.call_count, 0)

    @patch("bench_catalog_comparison.run_in_container")
    def test_cleanup_benchmark_data_fallback_and_safety(self, mock_run):
        # 1st call: irm -rf succeeds (0)
        # 2nd call: irmtrash -M -f fails (1)
        # 3rd call: echo y | irmtrash -M succeeds (0)
        mock_run.side_effect = [
            ("", "", 0),
            ("irmtrash: invalid option -- 'f'", "Option not supported", 1),
            ("", "", 0),
        ]
        durations = cleanup_benchmark_data("dummy-ctr", "/base")
        self.assertEqual(len(durations), 1)
        self.assertEqual(mock_run.call_count, 3)
        self.assertEqual(mock_run.call_args_list[2][0][1], "echo y | irmtrash -M")

    @patch("bench_catalog_comparison.run_in_container")
    def test_cleanup_benchmark_data_failure(self, mock_run):
        mock_run.return_value = ("", "Permission denied", 1)
        with self.assertRaises(RuntimeError) as ctx:
            cleanup_benchmark_data("dummy-ctr", "/base")
        self.assertIn("Permission denied", str(ctx.exception))

    def test_tier_configs_and_timeouts(self):
        for tier in ["test_small", "1k", "10k", "50k"]:
            self.assertIn(tier, TIER_CONFIGS)
            self.assertIn(tier, TIER_TIMEOUTS)
            self.assertGreater(TIER_TIMEOUTS[tier], 0)

        self.assertLessEqual(TIER_TIMEOUTS["test_small"], TIER_TIMEOUTS["1k"])
        self.assertLessEqual(TIER_TIMEOUTS["1k"], TIER_TIMEOUTS["10k"])
        self.assertLessEqual(TIER_TIMEOUTS["10k"], TIER_TIMEOUTS["50k"])

    @patch("bench_catalog_comparison.cleanup_benchmark_data")
    @patch("bench_catalog_comparison.benchmark_query")
    @patch("bench_catalog_comparison.benchmark_metadata")
    @patch("bench_catalog_comparison.benchmark_ils")
    @patch("bench_catalog_comparison.benchmark_registration")
    @patch("bench_catalog_comparison.benchmark_mkdir")
    def test_sub_workload_speedups(self, m_mkdir, m_reg, m_ils, m_meta, m_query, m_clean):
        m_mkdir.return_value = (["/c0"], [100_000_000])
        m_reg.return_value = (["/c0/o0"], [100_000_000])
        m_ils.return_value = [50_000_000]
        m_meta.return_value = [80_000_000]
        m_clean.return_value = [200_000_000]
        m_query.return_value = {
            "avu_lookup": [100_000_000],
            "wildcard_scan": [150_000_000],
            "branching_join": [200_000_000],
            "all": [100_000_000, 150_000_000, 200_000_000],
        }

        results = run_benchmark_orchestrator(
            backends=["both"],
            tiers=["test_small"],
            iterations=1,
            dry_run=True,
        )
        tier_data = results["tiers"]["test_small"]
        self.assertIn("speedup", tier_data)
        speedup = tier_data["speedup"]
        self.assertIn("query_sub_workloads", speedup)
        sub_speedups = speedup["query_sub_workloads"]
        self.assertIn("avu_lookup", sub_speedups)
        self.assertIn("wildcard_scan", sub_speedups)
        self.assertIn("branching_join", sub_speedups)

    @patch("bench_catalog_comparison.cleanup_benchmark_data")
    @patch("bench_catalog_comparison.benchmark_query")
    @patch("bench_catalog_comparison.benchmark_metadata")
    @patch("bench_catalog_comparison.benchmark_ils")
    @patch("bench_catalog_comparison.benchmark_registration")
    @patch("bench_catalog_comparison.benchmark_mkdir")
    def test_orchestrator_dry_run(self, m_mkdir, m_reg, m_ils, m_meta, m_query, m_clean):
        m_mkdir.return_value = (["/c0"], [10_000_000])
        m_reg.return_value = (["/c0/o0"], [10_000_000])
        m_ils.return_value = [10_000_000]
        m_meta.return_value = [10_000_000]
        m_clean.return_value = [10_000_000]
        m_query.return_value = {
            "avu_lookup": [10_000_000],
            "wildcard_scan": [10_000_000],
            "branching_join": [10_000_000],
            "all": [10_000_000, 10_000_000, 10_000_000],
        }

        results = run_benchmark_orchestrator(
            backends=["l3kvg"],
            tiers=["test_small"],
            iterations=1,
            dry_run=True,
        )
        self.assertIn("tiers", results)
        self.assertIn("test_small", results["tiers"])
        l3_metrics = results["tiers"]["test_small"]["backends"]["l3kvg"]
        for expected_key in ["mkdir", "registration", "ils", "metadata", "query", "cleanup"]:
            self.assertIn(expected_key, l3_metrics)
            self.assertTrue(set(BENCHMARK_SCHEMA_KEYS).issubset(l3_metrics[expected_key].keys()))

    def test_bulk_registration_script_generation(self):
        # Scale tier default (>= 1000 objects)
        bulk_code = generate_registration_worker_script("/tempZone/home/rods/base", ["/c0"], num_objects=1000)
        self.assertIn("iput -b -r", bulk_code)
        self.assertIn("staging", bulk_code)
        self.assertIn("shutil.rmtree", bulk_code)
        self.assertNotIn("itouch", bulk_code)

    def test_explicit_bulk_options(self):
        # Force bulk for small objects
        forced_bulk = generate_registration_worker_script("/tempZone/home/rods/base", ["/c0"], num_objects=5, bulk=True)
        self.assertIn("iput -b -r", forced_bulk)
        self.assertIn("staging", forced_bulk)

        # Force sequential for large objects
        forced_seq = generate_registration_worker_script("/tempZone/home/rods/base", ["/c0"], num_objects=2000, bulk=False)
        self.assertIn("itouch", forced_seq)
        self.assertNotIn("iput -b -r", forced_seq)

    @patch("bench_catalog_comparison.run_in_container")
    def test_measure_footprint(self, mock_run):
        def fake_run(container, cmd, as_irods=True):
            if "du -sh /var/lib/irods/l3kvg_db" in cmd:
                return ("5.0G\t/var/lib/irods/l3kvg_db", "", 0)
            if "du -sb /var/lib/irods/l3kvg_db" in cmd:
                return ("5000000000\t/var/lib/irods/l3kvg_db", "", 0)
            if "du -sh /var/lib/irods/" in cmd:
                return ("5.8G\t/var/lib/irods/", "", 0)
            if "du -sb /var/lib/irods/" in cmd:
                return ("6000000000\t/var/lib/irods/", "", 0)
            if "du -sh /var/lib/postgresql/data/" in cmd:
                return ("65M\t/var/lib/postgresql/data/", "", 0)
            if "du -sb /var/lib/postgresql/data/" in cmd:
                return ("68000000\t/var/lib/postgresql/data/", "", 0)
            if "ps -eo pid,rss,comm" in cmd:
                if "l3kvg" in container:
                    return ("  PID   RSS COMMAND\n 100 200000 l3kvg_server\n 200 30000 irodsServer\n 300 90000 irodsAgent\n", "", 0)
                elif "catalog-1" in container:
                    return ("  PID   RSS COMMAND\n 1 25000 postgres\n 2 10000 postgres\n", "", 0)
                else:
                    return ("  PID   RSS COMMAND\n 10 35000 irodsServer\n 20 90000 irodsAgent\n", "", 0)
            return ("", "not found", 1)

        mock_run.side_effect = fake_run
        fp = measure_footprint()
        self.assertIn("disk_human", fp)
        self.assertIn("disk_bytes", fp)
        self.assertIn("rss_kb", fp)
        self.assertEqual(fp["disk_human"]["l3kvg_irods"], "5.8G")
        self.assertEqual(fp["disk_human"]["l3kvg_db"], "5.0G")
        self.assertEqual(fp["disk_human"]["postgres_data"], "65M")
        self.assertEqual(fp["disk_bytes"]["l3kvg_irods"], 6000000000)
        self.assertIn("l3kvg", fp["rss_kb"])
        self.assertEqual(fp["rss_kb"]["l3kvg"]["l3kvg_server"], [200000])

    @patch("bench_catalog_comparison.measure_footprint")
    def test_record_footprint(self, mock_mf):
        import tempfile
        mock_mf.return_value = {"disk_human": {"test": "100M"}}
        with tempfile.NamedTemporaryFile("w+", delete=False, suffix=".json") as tmp:
            tmp.write('{"tiers": {}}')
            tmp_path = tmp.name

        try:
            record_footprint(tmp_path)
            with open(tmp_path, "r") as f:
                data = json.load(f)
            self.assertIn("footprint", data)
            self.assertEqual(data["footprint"]["disk_human"]["test"], "100M")
        finally:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)


if __name__ == '__main__':
    unittest.main()
