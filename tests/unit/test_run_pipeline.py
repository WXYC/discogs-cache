"""Unit tests for scripts/run_pipeline.py — streaming run_step() and arg parsing."""

from __future__ import annotations

import importlib.util
import logging
import sys
from pathlib import Path

import pytest

# Load run_pipeline as a module (it's a script, not a package).
_spec = importlib.util.spec_from_file_location(
    "run_pipeline",
    Path(__file__).parent.parent.parent / "scripts" / "run_pipeline.py",
)
run_pipeline = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(run_pipeline)


class TestRunStepStreaming:
    """run_step() streams subprocess output line-by-line."""

    def test_output_lines_appear_in_logger(self, caplog) -> None:
        """Each line of subprocess output is logged individually."""
        with caplog.at_level(logging.INFO, logger=run_pipeline.logger.name):
            run_pipeline.run_step(
                "echo test",
                [sys.executable, "-c", "print('line1'); print('line2')"],
            )
        logged = [r.message for r in caplog.records]
        assert any("line1" in msg for msg in logged)
        assert any("line2" in msg for msg in logged)

    def test_nonzero_exit_triggers_sys_exit(self) -> None:
        """Non-zero exit code triggers sys.exit(1)."""
        with pytest.raises(SystemExit, match="1"):
            run_pipeline.run_step(
                "fail test",
                [sys.executable, "-c", "import sys; sys.exit(42)"],
            )

    def test_elapsed_time_logged(self, caplog) -> None:
        """Elapsed time is logged on completion."""
        with caplog.at_level(logging.INFO, logger=run_pipeline.logger.name):
            run_pipeline.run_step(
                "elapsed test",
                [sys.executable, "-c", "pass"],
            )
        logged = [r.message for r in caplog.records]
        assert any("completed in" in msg for msg in logged)

    def test_stderr_is_captured(self, caplog) -> None:
        """Stderr output is also logged (merged with stdout)."""
        with caplog.at_level(logging.INFO, logger=run_pipeline.logger.name):
            run_pipeline.run_step(
                "stderr test",
                [sys.executable, "-c", "import sys; sys.stderr.write('err_msg\\n')"],
            )
        logged = [r.message for r in caplog.records]
        assert any("err_msg" in msg for msg in logged)


class TestArgParsing:
    """Argument parsing for --resume and --state-file flags."""

    def test_resume_flag_parsed(self) -> None:
        args = run_pipeline.parse_args(
            [
                "--csv-dir",
                "/tmp/csv",
                "--resume",
            ]
        )
        assert args.resume is True

    def test_resume_default_false(self) -> None:
        args = run_pipeline.parse_args(["--csv-dir", "/tmp/csv"])
        assert args.resume is False

    def test_state_file_flag_parsed(self) -> None:
        args = run_pipeline.parse_args(
            [
                "--csv-dir",
                "/tmp/csv",
                "--state-file",
                "/tmp/state.json",
            ]
        )
        assert args.state_file == Path("/tmp/state.json")

    def test_state_file_default(self) -> None:
        args = run_pipeline.parse_args(["--csv-dir", "/tmp/csv"])
        assert args.state_file == Path(".pipeline_state.json")

    def test_resume_invalid_with_xml(self) -> None:
        """--resume is only valid with --csv-dir, not --xml."""
        with pytest.raises(SystemExit):
            run_pipeline.parse_args(
                [
                    "--xml",
                    "/tmp/releases.xml.gz",
                    "--xml2db",
                    "/tmp/xml2db",
                    "--library-artists",
                    "/tmp/artists.txt",
                    "--resume",
                ]
            )

    def test_target_db_url_parsed(self) -> None:
        args = run_pipeline.parse_args(
            [
                "--csv-dir",
                "/tmp/csv",
                "--library-db",
                "/tmp/library.db",
                "--target-db-url",
                "postgresql://localhost/target",
            ]
        )
        assert args.target_db_url == "postgresql://localhost/target"

    def test_target_db_url_default_none(self) -> None:
        args = run_pipeline.parse_args(["--csv-dir", "/tmp/csv"])
        assert args.target_db_url is None

    def test_target_db_url_requires_library_db(self) -> None:
        """--target-db-url without --library-db should error."""
        with pytest.raises(SystemExit):
            run_pipeline.parse_args(
                [
                    "--csv-dir",
                    "/tmp/csv",
                    "--target-db-url",
                    "postgresql://localhost/target",
                ]
            )
