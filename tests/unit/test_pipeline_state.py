"""Unit tests for lib/pipeline_state.PipelineState."""

from __future__ import annotations

import json

import pytest

from lib.pipeline_state import PipelineState

STEPS = ["create_schema", "import_csv", "create_indexes", "dedup", "prune", "vacuum"]


class TestFreshState:
    """A freshly created PipelineState has all steps pending."""

    def test_no_steps_completed(self) -> None:
        state = PipelineState(
            db_url="postgresql://localhost/test",
            csv_dir="/tmp/csv",
        )
        for step in STEPS:
            assert not state.is_completed(step)


class TestMarkCompleted:
    """mark_completed() / is_completed() round-trip."""

    def test_mark_completed(self) -> None:
        state = PipelineState(db_url="postgresql://localhost/test", csv_dir="/tmp/csv")
        state.mark_completed("import_csv")
        assert state.is_completed("import_csv")

    def test_other_steps_remain_pending(self) -> None:
        state = PipelineState(db_url="postgresql://localhost/test", csv_dir="/tmp/csv")
        state.mark_completed("import_csv")
        assert not state.is_completed("create_schema")
        assert not state.is_completed("dedup")


class TestMarkFailed:
    """mark_failed() records error message."""

    def test_mark_failed(self) -> None:
        state = PipelineState(db_url="postgresql://localhost/test", csv_dir="/tmp/csv")
        state.mark_failed("create_indexes", "disk full")
        assert not state.is_completed("create_indexes")
        assert state.step_status("create_indexes") == "failed"
        assert state.step_error("create_indexes") == "disk full"


class TestSaveLoad:
    """save() writes valid JSON; load() restores state."""

    def test_save_creates_valid_json(self, tmp_path) -> None:
        state_file = tmp_path / "state.json"
        state = PipelineState(db_url="postgresql://localhost/test", csv_dir="/tmp/csv")
        state.mark_completed("create_schema")
        state.save(state_file)

        data = json.loads(state_file.read_text())
        assert data["version"] == 1
        assert data["database_url"] == "postgresql://localhost/test"
        assert data["csv_dir"] == "/tmp/csv"
        assert data["steps"]["create_schema"]["status"] == "completed"
        assert data["steps"]["import_csv"]["status"] == "pending"

    def test_load_restores_state(self, tmp_path) -> None:
        state_file = tmp_path / "state.json"
        state = PipelineState(db_url="postgresql://localhost/test", csv_dir="/tmp/csv")
        state.mark_completed("create_schema")
        state.mark_completed("import_csv")
        state.mark_failed("create_indexes", "disk full")
        state.save(state_file)

        loaded = PipelineState.load(state_file)
        assert loaded.db_url == "postgresql://localhost/test"
        assert loaded.csv_dir == "/tmp/csv"
        assert loaded.is_completed("create_schema")
        assert loaded.is_completed("import_csv")
        assert loaded.step_status("create_indexes") == "failed"
        assert loaded.step_error("create_indexes") == "disk full"
        assert not loaded.is_completed("dedup")

    def test_save_is_atomic(self, tmp_path) -> None:
        """save() writes to a temp file then renames, so partial writes don't corrupt."""
        state_file = tmp_path / "state.json"
        state = PipelineState(db_url="postgresql://localhost/test", csv_dir="/tmp/csv")
        state.save(state_file)

        # The temp file should not linger
        tmp_files = list(tmp_path.glob("*.tmp"))
        assert tmp_files == []


class TestValidateResume:
    """validate_resume() rejects mismatched db_url or csv_dir."""

    def test_matching_config_passes(self) -> None:
        state = PipelineState(db_url="postgresql://localhost/test", csv_dir="/tmp/csv")
        state.validate_resume(db_url="postgresql://localhost/test", csv_dir="/tmp/csv")

    def test_mismatched_db_url_raises(self) -> None:
        state = PipelineState(db_url="postgresql://localhost/test", csv_dir="/tmp/csv")
        with pytest.raises(ValueError, match="database_url"):
            state.validate_resume(db_url="postgresql://localhost/other", csv_dir="/tmp/csv")

    def test_mismatched_csv_dir_raises(self) -> None:
        state = PipelineState(db_url="postgresql://localhost/test", csv_dir="/tmp/csv")
        with pytest.raises(ValueError, match="csv_dir"):
            state.validate_resume(db_url="postgresql://localhost/test", csv_dir="/tmp/other")


class TestUnknownStep:
    """Operations on unknown step names raise KeyError."""

    def test_is_completed_unknown_step(self) -> None:
        state = PipelineState(db_url="postgresql://localhost/test", csv_dir="/tmp/csv")
        with pytest.raises(KeyError):
            state.is_completed("nonexistent")

    def test_mark_completed_unknown_step(self) -> None:
        state = PipelineState(db_url="postgresql://localhost/test", csv_dir="/tmp/csv")
        with pytest.raises(KeyError):
            state.mark_completed("nonexistent")


class TestVersionValidation:
    """load() rejects unknown state file versions."""

    def test_load_rejects_future_version(self, tmp_path) -> None:
        state_file = tmp_path / "state.json"
        state_file.write_text(
            json.dumps(
                {
                    "version": 99,
                    "database_url": "postgresql://localhost/test",
                    "csv_dir": "/tmp/csv",
                    "steps": {},
                }
            )
        )
        with pytest.raises(ValueError, match="version 99"):
            PipelineState.load(state_file)
