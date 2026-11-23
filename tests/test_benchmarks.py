from __future__ import annotations

import importlib.util
import json
import shutil
import subprocess
import sys
import tempfile
import unittest
from argparse import ArgumentTypeError
from pathlib import Path

from benchmarks._runner import benchmark_metadata, source_provenance
from benchmarks._support import positive_int

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]


class BenchmarkSupportTests(unittest.TestCase):
    def test_positive_int_parser_rejects_invalid_values(self) -> None:
        self.assertEqual(positive_int("17"), 17)
        for value in ("0", "-1", "1.5", "invalid"):
            with self.subTest(value=value), self.assertRaises(ArgumentTypeError):
                positive_int(value)


class BenchmarkMetadataTests(unittest.TestCase):
    def test_source_fingerprint_tracks_code_and_ignores_result_files(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            (root / "flowcoord").mkdir()
            (root / "benchmarks" / "results").mkdir(parents=True)
            (root / "pyproject.toml").write_text("[project]\n", encoding="utf-8")
            (root / "flowcoord" / "__init__.py").write_text("value = 1\n", encoding="utf-8")
            source = root / "benchmarks" / "benchmark.py"
            source.write_text("value = 1\n", encoding="utf-8")

            before = source_provenance(root)
            (root / "benchmarks" / "results" / "run.json").write_text("{}\n", encoding="utf-8")
            after_result = source_provenance(root)
            source.write_text("value = 2\n", encoding="utf-8")
            after_source_change = source_provenance(root)

        self.assertEqual(before["source_sha256"], after_result["source_sha256"])
        self.assertNotEqual(before["source_sha256"], after_source_change["source_sha256"])

    @unittest.skipUnless(shutil.which("git"), "Git is required for dirty-source coverage")
    def test_source_dirty_flag_ignores_results_and_detects_code_changes(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            (root / "flowcoord").mkdir()
            (root / "benchmarks" / "results").mkdir(parents=True)
            (root / "pyproject.toml").write_text("[project]\n", encoding="utf-8")
            (root / "flowcoord" / "__init__.py").write_text("value = 1\n", encoding="utf-8")
            source = root / "benchmarks" / "benchmark.py"
            source.write_text("value = 1\n", encoding="utf-8")
            subprocess.run(["git", "init", "-q"], cwd=root, check=True)
            subprocess.run(
                ["git", "add", "pyproject.toml", "flowcoord", "benchmarks"],
                cwd=root,
                check=True,
            )
            subprocess.run(
                [
                    "git",
                    "-c",
                    "user.name=EventFlow Tests",
                    "-c",
                    "user.email=tests@example.invalid",
                    "-c",
                    "commit.gpgsign=false",
                    "commit",
                    "-qm",
                    "benchmark source fixture",
                ],
                cwd=root,
                check=True,
            )

            clean = source_provenance(root)
            (root / "benchmarks" / "results" / "run.json").write_text("{}\n", encoding="utf-8")
            with_result = source_provenance(root)
            source.write_text("value = 2\n", encoding="utf-8")
            with_source_change = source_provenance(root)

        self.assertFalse(clean["source_dirty"])
        self.assertFalse(with_result["source_dirty"])
        self.assertTrue(with_source_change["source_dirty"])

    def test_metadata_records_backend_gil_and_source_identity(self) -> None:
        metadata = benchmark_metadata(REPOSITORY_ROOT)

        self.assertEqual(metadata["flowcoord_implementation"], "python")
        self.assertIn(metadata["flowcoord_gil"], {"enabled", "disabled", "unavailable"})
        self.assertIn(metadata["flowcoord_source_dirty"], {"true", "false", "unknown"})
        self.assertRegex(metadata["flowcoord_source_sha256"], r"^[0-9a-f]{64}$")


@unittest.skipUnless(importlib.util.find_spec("pyperf"), "pyperf development extra is required")
class PyperfRunnerTests(unittest.TestCase):
    def test_project_benchmarks_launch(self) -> None:
        cases = {
            "benchmark_thread_coordination": {
                "thread/permit-acquire-release",
                "thread/task-start-join",
                "thread/scheduler-once-join",
            },
            "benchmark_observability": {
                "operation-stats/record",
            },
            "benchmark_resilience": {
                "retry/sync-one-retry",
                "retry/async-one-retry",
                "circuit-breaker/sync-success",
            },
            "benchmark_lifecycle": {
                "resource-scope/defer-close",
                "bounded-work-pool/submit-result",
                "single-flight/same-key-eight-waiters",
            },
            "benchmark_portal": {
                "loop-portal-roundtrip",
            },
        }
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            for module, expected in cases.items():
                with self.subTest(module=module):
                    output = root / f"{module}.json"
                    result = subprocess.run(
                        [
                            sys.executable,
                            "-m",
                            f"benchmarks.{module}",
                            "--quiet",
                            "--processes=1",
                            "--values=1",
                            "--warmups=0",
                            "--loops=1",
                            "--output",
                            str(output),
                        ],
                        cwd=REPOSITORY_ROOT,
                        check=False,
                        capture_output=True,
                        text=True,
                        timeout=120,
                    )
                    if result.returncode:
                        self.fail(
                            f"{module} benchmark failed\nstdout:\n{result.stdout}"
                            f"\nstderr:\n{result.stderr}"
                        )
                    document = json.loads(output.read_text(encoding="utf-8"))
                    default_name = document.get("metadata", {}).get("name")
                    names = {
                        item.get("metadata", {}).get("name", default_name)
                        for item in document["benchmarks"]
                    }
                    self.assertTrue(expected.issubset(names), (module, names))


if __name__ == "__main__":
    unittest.main()
