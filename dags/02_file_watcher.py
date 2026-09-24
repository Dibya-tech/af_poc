"""
## File Watcher Pipeline

Monitors a drop folder for incoming CSV files using a **FileSensor**.
When a new file lands, the DAG picks it up, validates its schema,
runs quality checks, and moves it to a processed folder.

**Airflow features demonstrated:**
- FileSensor with reschedule mode (visible as "up_for_reschedule" in UI)
- Sensor poke interval and timeout configuration
- Conditional logging based on data quality results
- File-based trigger pattern common in data engineering

**How to test:**
Copy any CSV into `include/data/drop/incoming_data.csv` and watch the
sensor detect it on the next poke.
"""

import logging
import os
import shutil
from pathlib import Path

import pandas as pd
from airflow.sdk import dag, task
from airflow.providers.standard.sensors.filesystem import FileSensor
from pendulum import datetime

log = logging.getLogger(__name__)

DATA_DIR = Path(os.environ.get("AIRFLOW_HOME", "/usr/local/airflow")) / "include" / "data"
DROP_DIR = DATA_DIR / "drop"
ARCHIVE_DIR = DATA_DIR / "archive"

WATCHED_FILE = "incoming_data.csv"


@dag(
    start_date=datetime(2024, 1, 1),
    schedule="@hourly",
    catchup=False,
    doc_md=__doc__,
    default_args={"owner": "data-engineering", "retries": 1},
    tags=["sensor", "file-watcher", "demo"],
)
def file_watcher_pipeline():

    # ── SENSE ────────────────────────────────────────────────────────────

    wait_for_file = FileSensor(
        task_id="wait_for_file",
        filepath=str(DROP_DIR / WATCHED_FILE),
        poke_interval=30,          # check every 30 seconds
        timeout=60 * 60,           # give up after 1 hour
        mode="reschedule",         # free up worker slot between pokes (shows in UI)
        soft_fail=True,            # mark as skipped instead of failed on timeout
    )

    # ── VALIDATE ─────────────────────────────────────────────────────────

    @task()
    def validate_file() -> dict:
        """Validate the incoming file schema and basic quality."""
        filepath = DROP_DIR / WATCHED_FILE
        log.info("──── VALIDATE: Incoming File ────")
        log.info("File detected: %s", filepath)
        log.info("File size: %d bytes", filepath.stat().st_size)

        df = pd.read_csv(filepath)
        log.info("Loaded %d rows x %d columns", len(df), len(df.columns))
        log.info("Columns found: %s", list(df.columns))

        # Schema validation
        issues = []

        if df.empty:
            issues.append("File is empty — 0 rows")
            log.error("VALIDATION FAILED: File contains no data rows")

        # Null check
        null_counts = df.isnull().sum()
        cols_with_nulls = null_counts[null_counts > 0]
        if not cols_with_nulls.empty:
            for col, count in cols_with_nulls.items():
                pct = (count / len(df)) * 100
                msg = f"Column '{col}' has {count} null values ({pct:.1f}%)"
                issues.append(msg)
                if pct > 50:
                    log.error("CRITICAL: %s", msg)
                else:
                    log.warning("WARNING: %s", msg)
        else:
            log.info("Null check PASSED — no null values in any column")

        # Duplicate check
        dupes = df.duplicated().sum()
        if dupes:
            msg = f"{dupes} duplicate rows found"
            issues.append(msg)
            log.warning("WARNING: %s", msg)
        else:
            log.info("Duplicate check PASSED — no duplicate rows")

        # Data type summary
        log.info("Column types:")
        for col in df.columns:
            log.info("  %-25s %s", col, df[col].dtype)

        result = {
            "row_count": len(df),
            "col_count": len(df.columns),
            "columns": list(df.columns),
            "issues_count": len(issues),
            "issues": issues,
            "is_valid": len([i for i in issues if "CRITICAL" in i or "empty" in i]) == 0,
        }

        if result["is_valid"]:
            log.info("VALIDATION RESULT: PASSED with %d warnings", len(issues))
        else:
            log.error("VALIDATION RESULT: FAILED with %d issues", len(issues))

        return result

    # ── PROCESS ──────────────────────────────────────────────────────────

    @task()
    def process_file(validation: dict) -> dict:
        """Process the validated file — add metadata and summary stats."""
        log.info("──── PROCESS: Incoming File ────")

        if not validation["is_valid"]:
            log.error("Skipping processing — file failed validation")
            log.error("Issues: %s", validation["issues"])
            return {"status": "skipped", "reason": "validation_failed"}

        filepath = DROP_DIR / WATCHED_FILE
        df = pd.read_csv(filepath)

        log.info("Processing %d rows", len(df))

        # Basic profiling
        log.info("=" * 50)
        log.info("  DATA PROFILE")
        log.info("=" * 50)
        for col in df.columns:
            if df[col].dtype in ["int64", "float64"]:
                log.info(
                    "  %-20s min=%-10s max=%-10s mean=%.2f",
                    col, df[col].min(), df[col].max(), df[col].mean(),
                )
            else:
                unique = df[col].nunique()
                log.info("  %-20s %d unique values", col, unique)
        log.info("=" * 50)

        return {
            "status": "processed",
            "row_count": len(df),
            "columns": list(df.columns),
        }

    # ── ARCHIVE ──────────────────────────────────────────────────────────

    @task()
    def archive_file(process_result: dict) -> str:
        """Move processed file to archive folder with timestamp."""
        log.info("──── ARCHIVE: Move to Archive ────")

        ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)

        source = DROP_DIR / WATCHED_FILE
        if not source.exists():
            log.warning("Source file already removed: %s", source)
            return "already_archived"

        from pendulum import now
        timestamp = now().format("YYYYMMDD_HHmmss")
        dest_name = f"{source.stem}_{timestamp}{source.suffix}"
        dest = ARCHIVE_DIR / dest_name

        shutil.move(str(source), str(dest))

        log.info("Archived: %s -> %s", source.name, dest.name)
        log.info("Archive folder now contains %d files", len(list(ARCHIVE_DIR.iterdir())))

        return str(dest)

    # ── WIRE DEPENDENCIES ────────────────────────────────────────────────

    validation = validate_file()
    processed = process_file(validation)
    archived = archive_file(processed)

    wait_for_file >> validation


file_watcher_pipeline()
