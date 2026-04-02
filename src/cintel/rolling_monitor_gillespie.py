"""
rolling_monitor_gillespie.py - Project script.

Author: Aaron Gillespie
Date: 2026-04

Time-Series System Metrics Data

- Data is taken from a my Oura ring.
- Each row represents one day.
- The CSV file includes these columns:
  - date: when the observation occurred
  - daily_readiness_score: readiness score for the day
  - daily_sleep_score: sleep score for the day
  - daily_activity_score: activity score for the day
  - daily_activity_equivalent_walking_distance: equivalent walking distance for the day

Purpose

- Read time-series system metrics from a CSV file.
- Demonstrate rolling monitoring using a moving window.
- Compute rolling averages to smooth short-term variation.
- Save the resulting monitoring signals as a CSV artifact.
- Log the pipeline process to assist with debugging and transparency.

Questions to Consider

- How does system behavior change over time?
- Why might a rolling average reveal patterns that individual observations hide?
- How can smoothing short-term variation help us understand longer-term trends?

Paths (relative to repo root)

    INPUT FILE: data/oura_march.csv
    OUTPUT FILE: artifacts/rolling_metrics_gillespie.csv

Terminal command to run this file from the root project folder

    uv run python -m cintel.rolling_monitor_gillespie

OBS:
  Don't edit this file - it should remain a working example.
  Use as much of this code as you can when creating your own pipeline script,
  and change the monitoring logic as needed for your project.
"""

# === DECLARE IMPORTS ===

import logging
from pathlib import Path
from typing import Final

import polars as pl
from datafun_toolkit.logger import get_logger, log_header, log_path

# === CONFIGURE LOGGER ===

LOG: logging.Logger = get_logger("P5", level="DEBUG")

# === DEFINE GLOBAL PATHS ===

ROOT_DIR: Final[Path] = Path.cwd()
DATA_DIR: Final[Path] = ROOT_DIR / "data"
ARTIFACTS_DIR: Final[Path] = ROOT_DIR / "artifacts"

DATA_FILE: Final[Path] = DATA_DIR / "oura_march.csv"
OUTPUT_FILE: Final[Path] = ARTIFACTS_DIR / "rolling_metrics_gillespie.csv"

# === DEFINE THE MAIN FUNCTION ===


def main() -> None:
    """Run the pipeline.

    log_header() logs a standard run header.
    log_path() logs repo-relative paths (privacy-safe).
    """
    log_header(LOG, "CINTEL")

    LOG.info("========================")
    LOG.info("START main()")
    LOG.info("========================")

    log_path(LOG, "ROOT_DIR", ROOT_DIR)
    log_path(LOG, "DATA_FILE", DATA_FILE)
    log_path(LOG, "OUTPUT_FILE", OUTPUT_FILE)

    # Ensure artifacts directory exists
    ARTIFACTS_DIR.mkdir(parents=True, exist_ok=True)
    log_path(LOG, "ARTIFACTS_DIR", ARTIFACTS_DIR)

    # ----------------------------------------------------
    # STEP 1: READ CSV DATA FILE INTO A POLARS DATAFRAME (TABLE)
    # ----------------------------------------------------
    df = pl.read_csv(DATA_FILE)

    LOG.info(f"Loaded {df.height} time-series records")

    # ----------------------------------------------------
    # STEP 2: SORT DATA BY TIME
    # ----------------------------------------------------
    # Time-series analysis requires observations to be ordered.
    df = df.sort("date")

    LOG.info("Sorted records by date")

    # ----------------------------------------------------
    # STEP 3: DEFINE ROLLING WINDOW RECIPES
    # ----------------------------------------------------
    # A rolling window computes statistics over the most recent
    # N observations. The window "moves" forward one row at a time.

    # Example: if WINDOW_SIZE = 3
    # row 1 → mean of rows [1]
    # row 2 → mean of rows [1,2]
    # row 3 → mean of rows [1,2,3]
    # row 4 → mean of rows [2,3,4]

    WINDOW_SIZE: int = 3

    # ----------------------------------------------------
    # STEP 3.1: DEFINE ROLLING MEAN FOR DAILY READINESS SCORE
    # ----------------------------------------------------
    # The `daily_readiness_score` column holds the readiness score for each day.
    daily_readiness_score_rolling_mean_recipe: pl.Expr = (
        pl.col("daily_readiness_score").rolling_mean(WINDOW_SIZE).alias("daily_readiness_score_rolling_mean")
    )

    # ----------------------------------------------------
    # STEP 3.2: DEFINE ROLLING MEAN FOR DAILY SLEEP SCORE
    # ----------------------------------------------------
    # The `daily_sleep_score` column holds the sleep score for each day.
    daily_sleep_score_rolling_mean_recipe: pl.Expr = (
        pl.col("daily_sleep_score").rolling_mean(WINDOW_SIZE).alias("daily_sleep_score_rolling_mean")
    )

    # ----------------------------------------------------
    # STEP 3.3: DEFINE ROLLING MEAN FOR DAILY ACTIVITY SCORE
    # ----------------------------------------------------
    # The `daily_activity_score` column holds the activity score for each day.
    daily_activity_score_rolling_mean_recipe: pl.Expr = (
        pl.col("daily_activity_score")
        .rolling_mean(WINDOW_SIZE)
        .alias("daily_activity_score_rolling_mean")
    )

    # ----------------------------------------------------
    # STEP 3.4: APPLY THE ROLLING RECIPES IN A NEW DATAFRAME
    # ----------------------------------------------------
    # with_columns() evaluates the recipes and adds the new columns
    df_with_rolling = df.with_columns(
        [
            daily_readiness_score_rolling_mean_recipe,
            daily_sleep_score_rolling_mean_recipe,
            daily_activity_score_rolling_mean_recipe,
        ]
    )

    LOG.info("Computed rolling mean signals")

    # ----------------------------------------------------
    # STEP 4: SAVE RESULTS AS AN ARTIFACT
    # ----------------------------------------------------
    df_with_rolling.write_csv(OUTPUT_FILE)
    LOG.info(f"Wrote rolling monitoring file: {OUTPUT_FILE}")

    LOG.info("========================")
    LOG.info("Pipeline executed successfully!")
    LOG.info("========================")
    LOG.info("END main()")


# === CONDITIONAL EXECUTION GUARD ===

if __name__ == "__main__":
    main()
