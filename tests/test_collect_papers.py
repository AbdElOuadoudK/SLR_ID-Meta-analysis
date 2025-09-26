import json
import shutil
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import collect_papers


class CollectPapersTests(unittest.TestCase):
    def setUp(self):
        self.tmpdir = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.tmpdir)
        self.original_base = collect_papers.BASE
        collect_papers.BASE = str(self.tmpdir)
        self.addCleanup(setattr, collect_papers, "BASE", self.original_base)

    def _prepare_log_ledgers(self):
        logs_dir = self.tmpdir / "logs"
        logs_dir.mkdir()
        for mode in ["broad", "precise"]:
            (logs_dir / f"ledger_{mode}.json").write_text(json.dumps({"mode": mode}))
        return logs_dir

    def _prepare_artifact_dirs(self):
        for name in ["raw", "intermediate", "CSVs"]:
            target = self.tmpdir / name
            target.mkdir()
            (target / f"{name}.txt").write_text(f"contents for {name}")

    def test_collect_artifacts_excludes_converted_directory(self):
        self._prepare_artifact_dirs()
        converted_dir = self.tmpdir / "converted"
        converted_dir.mkdir()
        (converted_dir / "converted.txt").write_text("should be ignored")

        artifacts = collect_papers.collect_artifacts()

        self.assertNotIn(str(converted_dir / "converted.txt"), artifacts)
        expected_artifacts = {
            str(self.tmpdir / "raw" / "raw.txt"),
            str(self.tmpdir / "intermediate" / "intermediate.txt"),
            str(self.tmpdir / "CSVs" / "CSVs.txt"),
        }
        self.assertTrue(expected_artifacts.issubset(set(artifacts)))

    def test_main_does_not_create_converted_directory(self):
        self._prepare_artifact_dirs()
        logs_dir = self._prepare_log_ledgers()

        run_calls = []

        def fake_run(cmd):
            run_calls.append(cmd)

        with mock.patch.object(collect_papers, "run", side_effect=fake_run):
            collect_papers.main()

        self.assertEqual(2, len(run_calls))
        self.assertFalse((self.tmpdir / "converted").exists())
        self.assertTrue((self.tmpdir / "checksums.md").exists())
        self.assertTrue((logs_dir / "harvest_ledger.json").exists())

        checksums_content = (self.tmpdir / "checksums.md").read_text()
        self.assertNotIn("converted", checksums_content)
        for name in ["raw", "intermediate", "CSVs"]:
            self.assertIn(name, checksums_content)


if __name__ == "__main__":
    unittest.main()
