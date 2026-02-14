"""Pipeline state tracking for resumable ETL runs.

Tracks step completion in a JSON state file so that a failed pipeline
can be resumed from where it left off.
"""

from __future__ import annotations

import json
from pathlib import Path

VERSION = 2

STEP_NAMES = [
    "create_schema",
    "import_csv",
    "create_indexes",
    "dedup",
    "import_tracks",
    "create_track_indexes",
    "prune",
    "vacuum",
]

# Mapping from v1 step names to v2 equivalents for migration
_V1_STEP_NAMES = ["create_schema", "import_csv", "create_indexes", "dedup", "prune", "vacuum"]


class PipelineState:
    """Track pipeline step completion status."""

    def __init__(self, db_url: str, csv_dir: str) -> None:
        self.db_url = db_url
        self.csv_dir = csv_dir
        self._steps: dict[str, dict] = {name: {"status": "pending"} for name in STEP_NAMES}

    def is_completed(self, step: str) -> bool:
        """Return True if the step has been completed."""
        return self._steps[step]["status"] == "completed"

    def mark_completed(self, step: str) -> None:
        """Mark a step as completed."""
        self._steps[step]["status"] = "completed"

    def mark_failed(self, step: str, error: str) -> None:
        """Mark a step as failed with an error message."""
        self._steps[step]["status"] = "failed"
        self._steps[step]["error"] = error

    def step_status(self, step: str) -> str:
        """Return the status of a step."""
        return self._steps[step]["status"]

    def step_error(self, step: str) -> str | None:
        """Return the error message for a failed step, or None."""
        return self._steps[step].get("error")

    def validate_resume(self, db_url: str, csv_dir: str) -> None:
        """Raise ValueError if db_url or csv_dir don't match this state."""
        if self.db_url != db_url:
            raise ValueError(f"database_url mismatch: state has {self.db_url!r}, got {db_url!r}")
        if self.csv_dir != csv_dir:
            raise ValueError(f"csv_dir mismatch: state has {self.csv_dir!r}, got {csv_dir!r}")

    def save(self, path: Path) -> None:
        """Write state to a JSON file atomically (write .tmp, then rename)."""
        data = {
            "version": VERSION,
            "database_url": self.db_url,
            "csv_dir": self.csv_dir,
            "steps": self._steps,
        }
        tmp_path = path.with_suffix(".tmp")
        tmp_path.write_text(json.dumps(data, indent=2) + "\n")
        tmp_path.rename(path)

    @classmethod
    def load(cls, path: Path) -> PipelineState:
        """Load state from a JSON file.

        Supports v1 state files by migrating them to v2 format.
        """
        data = json.loads(path.read_text())
        version = data.get("version")

        if version == 1:
            return cls._migrate_v1(data)
        if version != VERSION:
            raise ValueError(f"Unsupported state file version {version} (expected {VERSION})")

        state = cls(db_url=data["database_url"], csv_dir=data["csv_dir"])
        state._steps = data["steps"]
        return state

    @classmethod
    def _migrate_v1(cls, data: dict) -> PipelineState:
        """Migrate a v1 state file to v2 format.

        V2 adds import_tracks and create_track_indexes between dedup and prune.

        Migration rules:
        - All v1 steps map directly to their v2 equivalents
        - If import_csv was completed in v1, import_tracks is also completed
          (v1 imported tracks as part of import_csv)
        - If create_indexes or dedup was completed in v1, create_track_indexes
          is also completed (v1 created track indexes during those steps)
        """
        state = cls(db_url=data["database_url"], csv_dir=data["csv_dir"])
        v1_steps = data.get("steps", {})

        # Copy v1 steps that exist in v2
        for step_name in _V1_STEP_NAMES:
            if step_name in v1_steps:
                state._steps[step_name] = v1_steps[step_name]

        # Infer import_tracks from import_csv
        if v1_steps.get("import_csv", {}).get("status") == "completed":
            state._steps["import_tracks"] = {"status": "completed"}

        # Infer create_track_indexes from dedup (v1 created all indexes in dedup)
        if v1_steps.get("dedup", {}).get("status") == "completed":
            state._steps["create_track_indexes"] = {"status": "completed"}
        elif v1_steps.get("create_indexes", {}).get("status") == "completed":
            state._steps["create_track_indexes"] = {"status": "completed"}

        return state
