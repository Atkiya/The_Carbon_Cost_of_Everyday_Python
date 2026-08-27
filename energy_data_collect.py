#!/usr/bin/env python3
"""
telemetry_benchmark.py

Benchmark the telemetry preprocessing workload described in the proposal.

Experimental design
-------------------
8 processing implementations:
    pandas_numpy
    pandas_pyarrow
    polars_eager
    polars_lazy
    duckdb
    dask_default
    dask_pyarrow
    python_loops

Crossed with:
    2 file formats            : csv, parquet
    2 numeric data types      : float32, float64
    2 categorical forms       : object, category
    2 threading settings      : single, parallel

Total configurations:
    8 x 2 x 2 x 2 x 2 = 128

The script also supports selecting how many rows of the source dataset are
used. The selected subset is created once, before any measured run, and the
same rows are written to both CSV and Parquet so that every configuration
receives identical input. The expected 24 column names exactly match the
provided collect_telemetry.py FIELDNAMES list.

Examples
--------
Create/refresh the Parquet copy:

    python telemetry_benchmark.py prepare \
        --csv benchmark_data.csv \
        --parquet benchmark_data.parquet

Validate the input files:

    python telemetry_benchmark.py validate \
        --csv benchmark_data.csv \
        --parquet benchmark_data.parquet

Quick test using the first 10,000 rows and one measured run per configuration:

    python telemetry_benchmark.py benchmark \
        --csv benchmark_data.csv \
        --parquet benchmark_data.parquet \
        --rows 10000 \
        --runs 1 \
        --cooldown 1 \
        --output test_results.csv

Use a reproducible random sample of 100,000 rows:

    python telemetry_benchmark.py benchmark \
        --csv benchmark_data.csv \
        --parquet benchmark_data.parquet \
        --rows 100000 \
        --row-selection random \
        --row-seed 407 \
        --runs 30 \
        --cooldown 5 \
        --output benchmark_results.csv

Use the complete dataset:

    python telemetry_benchmark.py benchmark \
        --csv benchmark_data.csv \
        --parquet benchmark_data.parquet \
        --rows 0 \
        --runs 30 \
        --cooldown 5 \
        --output benchmark_results.csv

Notes
-----
* --rows 0 means use every available row.
* The subset is prepared outside the energy/runtime measurement window.
* One warm-up is performed for every configuration and excluded from results.
* Measured runs are organized as randomized blocks. Each configuration appears
  exactly once in each block.
* The benchmark command checks all selected dependencies before any warm-up.
* pyRAPL is required for Intel RAPL energy readings.
* Imports and worker-pool startup occur before the measured workload.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import importlib.util
import json
import math
import os
import random
import subprocess
import sys
import tempfile
import threading
import time
from concurrent.futures import ProcessPoolExecutor
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Iterable, Sequence

import numpy as np



ENGINES = [
    "pandas_numpy",
    "pandas_pyarrow",
    "polars_eager",
    "polars_lazy",
    "duckdb",
    "dask_default",
    "dask_pyarrow",
    "python_loops",
]

FORMATS = ["csv", "parquet"]
NUMERIC_DTYPES = ["float32", "float64"]
CATEGORICAL_DTYPES = ["object", "category"]
THREADING_MODES = ["single", "parallel"]

GROUP_COLUMNS = ["weekday_type", "time_period", "cpu_state"]
IDENTIFIER_COLUMNS = {"sample_index"}

# These columns are required by feature engineering or grouping. They must not
# be dropped as "constant" merely because a tiny smoke-test subset (for
# example --rows 1) contains only one distinct value.
PROTECTED_COLUMNS = IDENTIFIER_COLUMNS | {
    "timestamp",
    "cpu_percent",
    "disk_read_mb",
    "disk_write_mb",
    "net_sent_kb",
    "net_recv_kb",
    "weekday_type",
    "time_period",
    "cpu_state",
}

EXPECTED_COLUMNS = [
    "timestamp",
    "cpu_freq_mhz",
    "cpu_temp_celsius",
    "cpu_percent",
    "ram_used_mb",
    "ram_percent",
    "disk_read_mb",
    "disk_write_mb",
    "net_sent_kb",
    "net_recv_kb",
    "swap_used_mb",
    "process_count",
    "core_id",
    "cpu_state",
    "ram_state",
    "disk_activity",
    "swap_state",
    "process_load_tier",
    "net_activity",
    "freq_tier",
    "temp_state",
    "time_period",
    "weekday_type",
    "sample_index",
]

RAW_NUMERIC_COLUMNS = [
    "cpu_freq_mhz",
    "cpu_temp_celsius",
    "cpu_percent",
    "ram_used_mb",
    "ram_percent",
    "disk_read_mb",
    "disk_write_mb",
    "net_sent_kb",
    "net_recv_kb",
    "swap_used_mb",
    "process_count",
]

RAW_CATEGORICAL_COLUMNS = [
    "core_id",
    "cpu_state",
    "ram_state",
    "disk_activity",
    "swap_state",
    "process_load_tier",
    "net_activity",
    "freq_tier",
    "temp_state",
    "time_period",
    "weekday_type",
]


def read_csv_header(path: Path) -> list[str]:
    """Read only the CSV header, without loading the dataset."""
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.reader(handle)
        try:
            return next(reader)
        except StopIteration as exc:
            raise ValueError(f"{path} is empty.") from exc


def validate_source_columns(path: Path) -> None:
    """Fail early when the source schema does not match the telemetry logger."""
    actual = read_csv_header(path)
    missing = [column for column in EXPECTED_COLUMNS if column not in actual]
    extra = [column for column in actual if column not in EXPECTED_COLUMNS]

    if missing:
        raise ValueError(
            "Telemetry CSV schema mismatch. Missing columns: "
            + ", ".join(missing)
            + ". Actual columns: "
            + ", ".join(actual)
        )

    if extra:
        print(
            "Warning: extra dataset columns will be preserved where supported: "
            + ", ".join(extra),
            file=sys.stderr,
        )


def required_modules_for(configs: Sequence["Config"]) -> dict[str, str]:
    """Return import-module -> install-name for the selected configurations."""
    required = {
        "numpy": "numpy",
        "pandas": "pandas",
        "pyarrow": "pyarrow",
        "psutil": "psutil",
        "pyRAPL": "pyRAPL",
    }

    selected_engines = {config.engine for config in configs}

    if selected_engines & {"polars_eager", "polars_lazy"}:
        required["polars"] = "polars"
    if "duckdb" in selected_engines:
        required["duckdb"] = "duckdb"
    if selected_engines & {"dask_default", "dask_pyarrow"}:
        required["dask"] = "dask[dataframe]"

    return required


def missing_modules(configs: Sequence["Config"]) -> list[str]:
    missing: list[str] = []
    for module_name, install_name in required_modules_for(configs).items():
        try:
            available = importlib.util.find_spec(module_name) is not None
        except (ImportError, ModuleNotFoundError, ValueError):
            available = False
        if not available:
            missing.append(install_name)
    return missing


def ensure_dependencies(configs: Sequence["Config"]) -> None:
    """Stop once with a useful message instead of failing 128 warm-ups."""
    missing = missing_modules(configs)
    if not missing:
        return

    packages = " ".join(missing)
    raise RuntimeError(
        "Missing required Python packages: "
        + ", ".join(missing)
        + ". On Arch Linux, use a virtual environment, then install them with: "
        + f"python -m pip install {packages}"
    )


@dataclass(frozen=True)
class Config:
    engine: str
    data_format: str
    numeric_dtype: str
    categorical_dtype: str
    threading_mode: str

    @property
    def config_id(self) -> str:
        return "__".join(
            (
                self.engine,
                self.data_format,
                self.numeric_dtype,
                self.categorical_dtype,
                self.threading_mode,
            )
        )


def build_all_configs() -> list[Config]:
    return [
        Config(engine, data_format, numeric_dtype, categorical_dtype, threading_mode)
        for engine in ENGINES
        for data_format in FORMATS
        for numeric_dtype in NUMERIC_DTYPES
        for categorical_dtype in CATEGORICAL_DTYPES
        for threading_mode in THREADING_MODES
    ]


def parse_filter(value: str | None) -> set[str] | None:
    if not value:
        return None
    return {item.strip() for item in value.split(",") if item.strip()}


def filter_configs(args: argparse.Namespace) -> list[Config]:
    engines = parse_filter(args.engines)
    formats = parse_filter(args.formats)
    numeric = parse_filter(args.numeric_dtypes)
    categorical = parse_filter(args.categorical_dtypes)
    threading_modes = parse_filter(args.threading_modes)

    return [
        config
        for config in build_all_configs()
        if (not engines or config.engine in engines)
        and (not formats or config.data_format in formats)
        and (not numeric or config.numeric_dtype in numeric)
        and (not categorical or config.categorical_dtype in categorical)
        and (not threading_modes or config.threading_mode in threading_modes)
    ]




def prepare_parquet(csv_path: Path, parquet_path: Path) -> None:
    import pandas as pd

    frame = pd.read_csv(csv_path)
    parquet_path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(parquet_path, index=False, compression="snappy")


def subset_signature(
    csv_path: Path,
    rows: int,
    row_selection: str,
    row_seed: int,
) -> str:
    payload = {
        "source": str(csv_path.resolve()),
        "source_size": csv_path.stat().st_size,
        "source_mtime_ns": csv_path.stat().st_mtime_ns,
        "rows": rows,
        "row_selection": row_selection,
        "row_seed": row_seed,
    }
    encoded = json.dumps(payload, sort_keys=True).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()[:16]


def prepare_selected_input(
    source_csv: Path,
    source_parquet: Path,
    rows: int,
    row_selection: str,
    row_seed: int,
    cache_dir: Path,
) -> tuple[Path, Path, int]:
    """
    Return CSV and Parquet files containing exactly the same selected rows.

    This function runs once before benchmarking, so row selection and subset
    creation are excluded from energy and runtime measurements.
    """
    if rows < 0:
        raise ValueError("--rows must be 0 or a positive integer.")

    if rows == 0:
        if not source_csv.exists():
            raise FileNotFoundError(source_csv)
        if not source_parquet.exists():
            raise FileNotFoundError(
                f"{source_parquet} does not exist. Run the prepare command first."
            )

        # Determine the number of rows without changing either source file.
        import pandas as pd

        row_count = len(pd.read_csv(source_csv, usecols=["sample_index"]))
        return source_csv, source_parquet, row_count

    import pandas as pd

    signature = subset_signature(source_csv, rows, row_selection, row_seed)
    subset_dir = cache_dir / f"rows_{rows}_{row_selection}_{signature}"
    subset_csv = subset_dir / "benchmark_subset.csv"
    subset_parquet = subset_dir / "benchmark_subset.parquet"
    metadata_path = subset_dir / "subset_metadata.json"

    if subset_csv.exists() and subset_parquet.exists() and metadata_path.exists():
        metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
        return subset_csv, subset_parquet, int(metadata["rows_used"])

    subset_dir.mkdir(parents=True, exist_ok=True)

    if row_selection == "first":
        selected = pd.read_csv(source_csv, nrows=rows)
    elif row_selection == "random":
        full = pd.read_csv(source_csv)
        use_rows = min(rows, len(full))
        selected = (
            full.sample(n=use_rows, random_state=row_seed, replace=False)
            .sort_index()
            .reset_index(drop=True)
        )
    else:
        raise ValueError(f"Unsupported row-selection mode: {row_selection}")

    if selected.empty:
        raise ValueError("The selected dataset contains no rows.")

    selected.to_csv(subset_csv, index=False)
    selected.to_parquet(subset_parquet, index=False, compression="snappy")

    metadata = {
        "source_csv": str(source_csv.resolve()),
        "requested_rows": rows,
        "rows_used": len(selected),
        "row_selection": row_selection,
        "row_seed": row_seed,
        "csv_path": str(subset_csv.resolve()),
        "parquet_path": str(subset_parquet.resolve()),
    }
    metadata_path.write_text(json.dumps(metadata, indent=2), encoding="utf-8")

    return subset_csv, subset_parquet, len(selected)




def worker_count(mode: str, requested_workers: int | None) -> int:
    if mode == "single":
        return 1
    available = os.cpu_count() or 2
    requested = requested_workers or available
    return max(2, min(requested, available))


def set_thread_environment(mode: str, workers: int) -> None:
    value = "1" if mode == "single" else str(workers)
    for variable in (
        "OMP_NUM_THREADS",
        "MKL_NUM_THREADS",
        "OPENBLAS_NUM_THREADS",
        "NUMEXPR_NUM_THREADS",
        "VECLIB_MAXIMUM_THREADS",
        "BLIS_NUM_THREADS",
        "POLARS_MAX_THREADS",
        "ARROW_NUM_THREADS",
    ):
        os.environ[variable] = value


class RaplMeter:
    def __init__(self) -> None:
        self.pyrapl = None
        self.measurement = None
        self.energy_j: float | None = None

        try:
            # pyRAPL may log a harmless warning about its optional MongoOutput
            # dependency. MongoDB output is not used by this benchmark, so silence
            # only that import-time root-logger warning and restore the old level.
            import logging

            root_logger = logging.getLogger()
            previous_level = root_logger.level
            root_logger.setLevel(logging.ERROR)
            try:
                import pyRAPL
                pyRAPL.setup()
            finally:
                root_logger.setLevel(previous_level)

            self.pyrapl = pyRAPL
        except Exception:
            self.pyrapl = None

    def __enter__(self) -> "RaplMeter":
        if self.pyrapl is not None:
            self.measurement = self.pyrapl.Measurement("telemetry_benchmark")
            self.measurement.begin()
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:
        if self.measurement is None:
            return

        try:
            self.measurement.end()
            result = self.measurement.result
            total_microjoules = 0.0

            for domain in ("pkg", "dram"):
                samples = getattr(result, domain, []) or []
                for sample in samples:
                    if sample is not None:
                        total_microjoules += float(sample)

            self.energy_j = total_microjoules / 1_000_000.0
        except Exception:
            self.energy_j = None


class PeakMemoryMonitor:
    """Track total RSS of the worker process and its child processes."""

    def __init__(self, interval_seconds: float = 0.02) -> None:
        self.interval_seconds = interval_seconds
        self.peak_mb = 0.0
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def _run(self) -> None:
        try:
            import psutil

            parent = psutil.Process(os.getpid())

            while not self._stop.is_set():
                processes = [parent]
                try:
                    processes.extend(parent.children(recursive=True))
                except Exception:
                    pass

                total_bytes = 0
                for process in processes:
                    try:
                        total_bytes += process.memory_info().rss
                    except Exception:
                        pass

                self.peak_mb = max(self.peak_mb, total_bytes / (1024 * 1024))
                self._stop.wait(self.interval_seconds)
        except Exception:
            self.peak_mb = float("nan")

    def __enter__(self) -> "PeakMemoryMonitor":
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=2)


def preload_engine(engine: str) -> None:
    """Import the selected engine before the measured workload."""
    if engine in {"pandas_numpy", "pandas_pyarrow"}:
        import pandas  # noqa: F401
    elif engine in {"polars_eager", "polars_lazy"}:
        import polars  # noqa: F401
    elif engine == "duckdb":
        import duckdb  # noqa: F401
    elif engine in {"dask_default", "dask_pyarrow"}:
        import dask  # noqa: F401
        import dask.dataframe  # noqa: F401
    elif engine == "python_loops":
        import pyarrow.parquet  # noqa: F401


class ExecutionContext:
    """
    Pre-create process workers before measurement for parallel Pandas and
    Python-loop configurations.
    """

    def __init__(self, config: Config, workers: int) -> None:
        self.config = config
        self.workers = workers
        self.pool: ProcessPoolExecutor | None = None

    def __enter__(self) -> "ExecutionContext":
        if (
            self.config.threading_mode == "parallel"
            and self.config.engine
            in {"pandas_numpy", "pandas_pyarrow", "python_loops"}
        ):
            self.pool = ProcessPoolExecutor(max_workers=self.workers)
            # Start processes before the measurement window.
            list(self.pool.map(_identity_worker, range(self.workers)))
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:
        if self.pool is not None:
            self.pool.shutdown(wait=True, cancel_futures=True)


def _identity_worker(value: int) -> int:
    return value



def flatten_pandas_columns(frame):
    frame = frame.copy()
    frame.columns = [
        "_".join(str(part) for part in column if str(part))
        if isinstance(column, tuple)
        else str(column)
        for column in frame.columns
    ]
    return frame


def canonical_records(result: Any) -> list[dict[str, Any]]:
    if hasattr(result, "to_pandas"):
        result = result.to_pandas()

    ordered = result.sort_values(GROUP_COLUMNS).reset_index(drop=True)
    ordered = ordered.reindex(sorted(ordered.columns), axis=1)
    ordered = ordered.replace({np.nan: None})

    records = ordered.to_dict(orient="records")

    for record in records:
        for key, value in list(record.items()):
            if isinstance(value, (np.integer,)):
                record[key] = int(value)
            elif isinstance(value, (float, np.floating)):
                number = float(value)
                record[key] = round(number, 7) if math.isfinite(number) else None
            elif value is not None:
                record[key] = str(value)

    return records


def result_checksum(result: Any) -> str:
    encoded = json.dumps(
        canonical_records(result),
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()




def pandas_read(path: Path, pyarrow_backend: bool):
    import pandas as pd

    kwargs: dict[str, Any] = {}
    if pyarrow_backend:
        kwargs["dtype_backend"] = "pyarrow"

    if path.suffix.lower() == ".parquet":
        return pd.read_parquet(path, **kwargs)
    return pd.read_csv(path, **kwargs)


def pandas_prepare_base(frame):
    frame = frame.drop_duplicates().copy()

    constant_columns = [
        column
        for column in frame.columns
        if column not in PROTECTED_COLUMNS
        and frame[column].nunique(dropna=False) <= 1
    ]

    if constant_columns:
        frame = frame.drop(columns=constant_columns)

    return frame


def pandas_transform_partition(
    payload: tuple[Any, str, str],
):
    import pandas as pd

    frame, numeric_dtype, categorical_dtype = payload
    frame = frame.copy()

    target_numeric = np.float32 if numeric_dtype == "float32" else np.float64

    for column in RAW_NUMERIC_COLUMNS:
        if column in frame.columns:
            frame[column] = pd.to_numeric(frame[column], errors="coerce").astype(
                target_numeric
            )

    for column in RAW_CATEGORICAL_COLUMNS:
        if column in frame.columns:
            if categorical_dtype == "category":
                frame[column] = frame[column].astype("category")
            else:
                frame[column] = frame[column].astype("object")

    frame["timestamp"] = pd.to_datetime(frame["timestamp"], errors="coerce")

    epsilon = np.array(1e-6, dtype=target_numeric).item()

    frame["total_disk_throughput"] = (
        frame["disk_read_mb"] + frame["disk_write_mb"]
    ).astype(target_numeric)

    frame["total_network_throughput"] = (
        frame["net_sent_kb"] + frame["net_recv_kb"]
    ).astype(target_numeric)

    frame["cpu_headroom"] = (
        np.array(100.0, dtype=target_numeric) - frame["cpu_percent"]
    ).astype(target_numeric)

    frame["disk_rw_balance"] = (
        (frame["disk_read_mb"] - frame["disk_write_mb"])
        / (frame["disk_read_mb"] + frame["disk_write_mb"] + epsilon)
    ).astype(target_numeric)

    frame["net_sr_balance"] = (
        (frame["net_sent_kb"] - frame["net_recv_kb"])
        / (frame["net_sent_kb"] + frame["net_recv_kb"] + epsilon)
    ).astype(target_numeric)

    frame["hour"] = frame["timestamp"].dt.hour.astype(target_numeric)

    return frame


def pandas_aggregate(frame):
    numeric_columns = [
        column
        for column in frame.select_dtypes(include=[np.number]).columns
        if column != "sample_index"
    ]

    result = (
        frame.groupby(GROUP_COLUMNS, observed=True, dropna=False)[numeric_columns]
        .agg(["count", "mean", "std", "min", "max"])
        .reset_index()
    )

    return flatten_pandas_columns(result)


def pandas_pipeline(
    path: Path,
    numeric_dtype: str,
    categorical_dtype: str,
    threading_mode: str,
    pyarrow_backend: bool,
    workers: int,
    pool: ProcessPoolExecutor | None,
):
    import pandas as pd

    frame = pandas_read(path, pyarrow_backend=pyarrow_backend)
    frame = pandas_prepare_base(frame)

    if threading_mode == "single":
        transformed = pandas_transform_partition(
            (frame, numeric_dtype, categorical_dtype)
        )
    else:
        if pool is None:
            raise RuntimeError("Parallel Pandas requires a prepared process pool.")

        boundaries = np.linspace(0, len(frame), workers + 1, dtype=int)
        partitions = [
            frame.iloc[boundaries[index] : boundaries[index + 1]].copy()
            for index in range(workers)
            if boundaries[index] < boundaries[index + 1]
        ]

        transformed_parts = list(
            pool.map(
                pandas_transform_partition,
                [
                    (partition, numeric_dtype, categorical_dtype)
                    for partition in partitions
                ],
            )
        )

        transformed = pd.concat(transformed_parts, ignore_index=True)

        # Reapply categorical dtype because concatenating partitions can sometimes
        # fall back to object when category sets differ.
        if categorical_dtype == "category":
            for column in RAW_CATEGORICAL_COLUMNS:
                if column in transformed.columns:
                    transformed[column] = transformed[column].astype("category")

    return pandas_aggregate(transformed)




def polars_pipeline(
    path: Path,
    numeric_dtype: str,
    categorical_dtype: str,
    lazy: bool,
):
    import polars as pl

    float_type = pl.Float32 if numeric_dtype == "float32" else pl.Float64
    category_type = pl.Categorical if categorical_dtype == "category" else pl.String

    if lazy:
        frame = (
            pl.scan_parquet(path)
            if path.suffix.lower() == ".parquet"
            else pl.scan_csv(path, try_parse_dates=True)
        )
    else:
        frame = (
            pl.read_parquet(path)
            if path.suffix.lower() == ".parquet"
            else pl.read_csv(path, try_parse_dates=True)
        )

    frame = frame.unique()

    schema = frame.collect_schema() if lazy else frame.schema
    columns = list(schema.keys())

    # Only test removable columns for constancy. Columns needed by feature
    # engineering, grouping, timestamp parsing, and sample identity are protected,
    # especially for tiny smoke-test subsets such as --rows 1.
    unique_expression = [
        pl.col(column).n_unique().alias(column)
        for column in columns
        if column not in PROTECTED_COLUMNS
    ]

    unique_counts = (
        frame.select(unique_expression).collect()
        if lazy
        else frame.select(unique_expression)
    )

    constant_columns = [
        column
        for column in unique_counts.columns
        if unique_counts[column][0] <= 1
    ]

    if constant_columns:
        frame = frame.drop(constant_columns)

    remaining_columns = [column for column in columns if column not in constant_columns]

    casts = []

    for column in RAW_NUMERIC_COLUMNS:
        if column in remaining_columns:
            casts.append(pl.col(column).cast(float_type, strict=False))

    for column in RAW_CATEGORICAL_COLUMNS:
        if column in remaining_columns:
            casts.append(pl.col(column).cast(category_type, strict=False))

    casts.append(
        pl.col("timestamp")
        .cast(pl.String)
        .str.to_datetime(strict=False)
        .alias("timestamp")
    )

    frame = frame.with_columns(casts)

    epsilon = pl.lit(1e-6, dtype=float_type)

    frame = frame.with_columns(
        [
            (pl.col("disk_read_mb") + pl.col("disk_write_mb"))
            .cast(float_type)
            .alias("total_disk_throughput"),
            (pl.col("net_sent_kb") + pl.col("net_recv_kb"))
            .cast(float_type)
            .alias("total_network_throughput"),
            (pl.lit(100.0, dtype=float_type) - pl.col("cpu_percent"))
            .cast(float_type)
            .alias("cpu_headroom"),
            (
                (pl.col("disk_read_mb") - pl.col("disk_write_mb"))
                / (pl.col("disk_read_mb") + pl.col("disk_write_mb") + epsilon)
            )
            .cast(float_type)
            .alias("disk_rw_balance"),
            (
                (pl.col("net_sent_kb") - pl.col("net_recv_kb"))
                / (pl.col("net_sent_kb") + pl.col("net_recv_kb") + epsilon)
            )
            .cast(float_type)
            .alias("net_sr_balance"),
            pl.col("timestamp").dt.hour().cast(float_type).alias("hour"),
        ]
    )

    result_schema = frame.collect_schema() if lazy else frame.schema

    numeric_columns = [
        column
        for column, dtype in result_schema.items()
        if column != "sample_index" and dtype.is_numeric()
    ]

    aggregations = []

    for column in numeric_columns:
        aggregations.extend(
            [
                pl.col(column).count().alias(f"{column}_count"),
                pl.col(column).mean().alias(f"{column}_mean"),
                pl.col(column).std().alias(f"{column}_std"),
                pl.col(column).min().alias(f"{column}_min"),
                pl.col(column).max().alias(f"{column}_max"),
            ]
        )

    result = (
        frame.group_by(GROUP_COLUMNS)
        .agg(aggregations)
        .sort(GROUP_COLUMNS)
    )

    return result.collect() if lazy else result




def quote_identifier(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def duckdb_pipeline(
    path: Path,
    numeric_dtype: str,
    categorical_dtype: str,
    workers: int,
):
    import duckdb

    connection = duckdb.connect(database=":memory:")
    connection.execute(f"SET threads={workers}")

    source = (
        f"read_parquet('{path.as_posix()}')"
        if path.suffix.lower() == ".parquet"
        else f"read_csv_auto('{path.as_posix()}', header=true)"
    )

    connection.execute(f"CREATE TEMP TABLE source_data AS SELECT * FROM {source}")
    connection.execute(
        "CREATE TEMP TABLE deduplicated AS SELECT DISTINCT * FROM source_data"
    )

    retained_columns: list[str] = []

    for column in EXPECTED_COLUMNS:
        if column in PROTECTED_COLUMNS:
            retained_columns.append(column)
            continue

        distinct_count = connection.execute(
            f"SELECT COUNT(DISTINCT {quote_identifier(column)}) FROM deduplicated"
        ).fetchone()[0]

        if distinct_count > 1:
            retained_columns.append(column)

    float_type = "FLOAT" if numeric_dtype == "float32" else "DOUBLE"

    select_terms: list[str] = []

    for column in retained_columns:
        quoted = quote_identifier(column)

        if column in RAW_NUMERIC_COLUMNS:
            select_terms.append(f"CAST({quoted} AS {float_type}) AS {quoted}")
        elif column == "timestamp":
            select_terms.append(
                f"TRY_CAST({quoted} AS TIMESTAMP) AS {quoted}"
            )
        elif column in RAW_CATEGORICAL_COLUMNS:
            # DuckDB ENUM provides dictionary-encoded categorical storage.
            if categorical_dtype == "category":
                enum_name = f"enum_{column}"
                values = [
                    row[0]
                    for row in connection.execute(
                        f"SELECT DISTINCT {quoted} "
                        f"FROM deduplicated WHERE {quoted} IS NOT NULL"
                    ).fetchall()
                ]

                if values:
                    escaped_values = ", ".join(
                        "'" + str(value).replace("'", "''") + "'"
                        for value in values
                    )
                    connection.execute(
                        f"CREATE TYPE {quote_identifier(enum_name)} "
                        f"AS ENUM ({escaped_values})"
                    )
                    select_terms.append(
                        f"CAST({quoted} AS {quote_identifier(enum_name)}) AS {quoted}"
                    )
                else:
                    select_terms.append(f"CAST({quoted} AS VARCHAR) AS {quoted}")
            else:
                select_terms.append(f"CAST({quoted} AS VARCHAR) AS {quoted}")
        else:
            select_terms.append(quoted)

    connection.execute(
        "CREATE TEMP TABLE prepared AS SELECT "
        + ", ".join(select_terms)
        + " FROM deduplicated"
    )

    connection.execute(
        f"""
        CREATE TEMP VIEW engineered AS
        SELECT
            *,
            CAST(disk_read_mb + disk_write_mb AS {float_type})
                AS total_disk_throughput,
            CAST(net_sent_kb + net_recv_kb AS {float_type})
                AS total_network_throughput,
            CAST(100.0 - cpu_percent AS {float_type})
                AS cpu_headroom,
            CAST(
                (disk_read_mb - disk_write_mb) /
                (disk_read_mb + disk_write_mb + 1e-6)
                AS {float_type}
            ) AS disk_rw_balance,
            CAST(
                (net_sent_kb - net_recv_kb) /
                (net_sent_kb + net_recv_kb + 1e-6)
                AS {float_type}
            ) AS net_sr_balance,
            CAST(EXTRACT(HOUR FROM timestamp) AS {float_type}) AS hour
        FROM prepared
        """
    )

    numeric_columns = [
        row[0]
        for row in connection.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'engineered'
              AND data_type IN (
                  'FLOAT', 'DOUBLE', 'REAL', 'TINYINT', 'SMALLINT',
                  'INTEGER', 'BIGINT', 'HUGEINT', 'DECIMAL'
              )
            """
        ).fetchall()
        if row[0] != "sample_index"
    ]

    aggregation_terms: list[str] = []

    for column in numeric_columns:
        quoted = quote_identifier(column)
        aggregation_terms.extend(
            [
                f"COUNT({quoted}) AS {quote_identifier(column + '_count')}",
                f"AVG({quoted}) AS {quote_identifier(column + '_mean')}",
                f"STDDEV_SAMP({quoted}) AS {quote_identifier(column + '_std')}",
                f"MIN({quoted}) AS {quote_identifier(column + '_min')}",
                f"MAX({quoted}) AS {quote_identifier(column + '_max')}",
            ]
        )

    result = connection.execute(
        f"""
        SELECT
            weekday_type,
            time_period,
            cpu_state,
            {", ".join(aggregation_terms)}
        FROM engineered
        GROUP BY weekday_type, time_period, cpu_state
        ORDER BY weekday_type, time_period, cpu_state
        """
    ).df()

    connection.close()
    return result




def dask_read(path: Path, pyarrow_backend: bool):
    import dask.dataframe as dd

    kwargs: dict[str, Any] = {}

    if pyarrow_backend:
        kwargs["dtype_backend"] = "pyarrow"

    try:
        if path.suffix.lower() == ".parquet":
            return dd.read_parquet(path, **kwargs)
        return dd.read_csv(path, assume_missing=True, **kwargs)
    except TypeError:
        # Compatibility fallback for older Dask versions.
        if path.suffix.lower() == ".parquet":
            frame = dd.read_parquet(path)
        else:
            frame = dd.read_csv(path, assume_missing=True)

        if pyarrow_backend:
            frame = frame.map_partitions(
                lambda partition: partition.convert_dtypes(
                    dtype_backend="pyarrow"
                )
            )
        return frame


def dask_pipeline(
    path: Path,
    numeric_dtype: str,
    categorical_dtype: str,
    threading_mode: str,
    pyarrow_backend: bool,
    workers: int,
):
    import dask
    import dask.dataframe as dd
    import pandas as pd

    scheduler = "single-threaded" if threading_mode == "single" else "threads"

    frame = dask_read(path, pyarrow_backend=pyarrow_backend)
    frame = frame.drop_duplicates()

    with dask.config.set(scheduler=scheduler, num_workers=workers):
        unique_tasks = {
            column: frame[column].nunique(dropna=False)
            for column in frame.columns
            if column not in PROTECTED_COLUMNS
        }
        computed_unique = dask.compute(unique_tasks)[0]

    constant_columns = [
        column
        for column, count in computed_unique.items()
        if int(count) <= 1
    ]

    if constant_columns:
        frame = frame.drop(columns=constant_columns)

    target_numeric = "float32" if numeric_dtype == "float32" else "float64"

    for column in RAW_NUMERIC_COLUMNS:
        if column in frame.columns:
            frame[column] = dd.to_numeric(
                frame[column], errors="coerce"
            ).astype(target_numeric)

    for column in RAW_CATEGORICAL_COLUMNS:
        if column in frame.columns:
            if categorical_dtype == "category":
                frame[column] = frame[column].astype("category")
            else:
                frame[column] = frame[column].map_partitions(
                    lambda partition: partition.astype("object"),
                    meta=(column, "object"),
                )

    if categorical_dtype == "category":
        with dask.config.set(scheduler=scheduler, num_workers=workers):
            for column in RAW_CATEGORICAL_COLUMNS:
                if column in frame.columns:
                    frame[column] = frame[column].cat.as_known()

    frame["timestamp"] = dd.to_datetime(frame["timestamp"], errors="coerce")

    frame["total_disk_throughput"] = (
        frame["disk_read_mb"] + frame["disk_write_mb"]
    ).astype(target_numeric)

    frame["total_network_throughput"] = (
        frame["net_sent_kb"] + frame["net_recv_kb"]
    ).astype(target_numeric)

    frame["cpu_headroom"] = (
        100.0 - frame["cpu_percent"]
    ).astype(target_numeric)

    frame["disk_rw_balance"] = (
        (frame["disk_read_mb"] - frame["disk_write_mb"])
        / (frame["disk_read_mb"] + frame["disk_write_mb"] + 1e-6)
    ).astype(target_numeric)

    frame["net_sr_balance"] = (
        (frame["net_sent_kb"] - frame["net_recv_kb"])
        / (frame["net_sent_kb"] + frame["net_recv_kb"] + 1e-6)
    ).astype(target_numeric)

    frame["hour"] = frame["timestamp"].dt.hour.astype(target_numeric)

    numeric_columns = [
        column
        for column, dtype in frame.dtypes.items()
        if column != "sample_index" and pd.api.types.is_numeric_dtype(dtype)
    ]

    aggregation = {
        column: ["count", "mean", "std", "min", "max"]
        for column in numeric_columns
    }

    with dask.config.set(scheduler=scheduler, num_workers=workers):
        result = (
            frame.groupby(GROUP_COLUMNS, observed=True)[numeric_columns]
            .agg(aggregation)
            .compute()
            .reset_index()
        )

    return flatten_pandas_columns(result)



def read_records(path: Path) -> list[dict[str, Any]]:
    if path.suffix.lower() == ".parquet":
        import pyarrow.parquet as parquet

        return parquet.read_table(path).to_pylist()

    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def remove_duplicate_records(
    records: Sequence[dict[str, Any]],
) -> list[dict[str, Any]]:
    seen: set[tuple[tuple[str, str], ...]] = set()
    output: list[dict[str, Any]] = []

    for record in records:
        identity = tuple(
            (key, "" if value is None else str(value))
            for key, value in sorted(record.items())
        )

        if identity not in seen:
            seen.add(identity)
            output.append(record)

    return output


def find_constant_record_columns(
    records: Sequence[dict[str, Any]],
) -> set[str]:
    constant_columns: set[str] = set()

    if not records:
        return constant_columns

    for column in records[0]:
        if column in PROTECTED_COLUMNS:
            continue

        first = records[0].get(column)
        is_constant = True

        for record in records[1:]:
            if record.get(column) != first:
                is_constant = False
                break

        if is_constant:
            constant_columns.add(column)

    return constant_columns


def build_category_maps(
    records: Sequence[dict[str, Any]],
    retained_columns: set[str],
) -> dict[str, dict[str, int]]:
    maps: dict[str, dict[str, int]] = {}

    for column in RAW_CATEGORICAL_COLUMNS:
        if column not in retained_columns:
            continue

        values = sorted(
            {
                "missing" if record.get(column) is None else str(record.get(column))
                for record in records
            }
        )
        maps[column] = {value: index for index, value in enumerate(values)}

    return maps


def loops_partition_aggregate(
    payload: tuple[
        Sequence[dict[str, Any]],
        str,
        str,
        set[str],
        dict[str, dict[str, int]],
    ]
) -> dict[tuple[Any, Any, Any], dict[str, list[float]]]:
    (
        records,
        numeric_dtype,
        categorical_dtype,
        constant_columns,
        category_maps,
    ) = payload

    cast = np.float32 if numeric_dtype == "float32" else np.float64

    numeric_columns = [
        column
        for column in RAW_NUMERIC_COLUMNS
        if column not in constant_columns
    ] + [
        "total_disk_throughput",
        "total_network_throughput",
        "cpu_headroom",
        "disk_rw_balance",
        "net_sr_balance",
        "hour",
    ]

    groups: dict[tuple[Any, Any, Any], dict[str, list[float]]] = {}

    for record in records:
        converted_numeric: dict[str, float] = {}

        for column in RAW_NUMERIC_COLUMNS:
            if column in constant_columns:
                continue

            try:
                value = float(record.get(column))
                converted_numeric[column] = cast(value).item()
            except (TypeError, ValueError):
                converted_numeric[column] = float("nan")

        disk_total = (
            converted_numeric["disk_read_mb"]
            + converted_numeric["disk_write_mb"]
        )
        network_total = (
            converted_numeric["net_sent_kb"]
            + converted_numeric["net_recv_kb"]
        )

        converted_numeric["total_disk_throughput"] = cast(disk_total).item()
        converted_numeric["total_network_throughput"] = cast(network_total).item()
        converted_numeric["cpu_headroom"] = cast(
            100.0 - converted_numeric["cpu_percent"]
        ).item()
        converted_numeric["disk_rw_balance"] = cast(
            (
                converted_numeric["disk_read_mb"]
                - converted_numeric["disk_write_mb"]
            )
            / (disk_total + 1e-6)
        ).item()
        converted_numeric["net_sr_balance"] = cast(
            (
                converted_numeric["net_sent_kb"]
                - converted_numeric["net_recv_kb"]
            )
            / (network_total + 1e-6)
        ).item()

        timestamp = "" if record.get("timestamp") is None else str(record["timestamp"])

        try:
            converted_numeric["hour"] = cast(float(timestamp[11:13])).item()
        except (ValueError, IndexError):
            converted_numeric["hour"] = float("nan")

        group_values: list[Any] = []

        for column in GROUP_COLUMNS:
            text = (
                "missing"
                if record.get(column) is None
                else str(record.get(column))
            )

            if categorical_dtype == "category":
                group_values.append(category_maps[column][text])
            else:
                group_values.append(text)

        group_key = tuple(group_values)

        if group_key not in groups:
            groups[group_key] = {
                column: [0.0, 0.0, 0.0, math.inf, -math.inf]
                for column in numeric_columns
            }

        for column in numeric_columns:
            value = converted_numeric[column]

            if math.isnan(value):
                continue

            stats = groups[group_key][column]
            stats[0] += 1.0
            stats[1] += value
            stats[2] += value * value
            stats[3] = min(stats[3], value)
            stats[4] = max(stats[4], value)

    return groups


def merge_loop_aggregates(
    partials: Sequence[
        dict[tuple[Any, Any, Any], dict[str, list[float]]]
    ]
) -> dict[tuple[Any, Any, Any], dict[str, list[float]]]:
    merged: dict[tuple[Any, Any, Any], dict[str, list[float]]] = {}

    for partial in partials:
        for group_key, columns in partial.items():
            if group_key not in merged:
                merged[group_key] = {
                    column: values.copy()
                    for column, values in columns.items()
                }
                continue

            for column, values in columns.items():
                target = merged[group_key][column]
                target[0] += values[0]
                target[1] += values[1]
                target[2] += values[2]
                target[3] = min(target[3], values[3])
                target[4] = max(target[4], values[4])

    return merged


def loop_aggregate_to_frame(
    groups: dict[tuple[Any, Any, Any], dict[str, list[float]]],
    categorical_dtype: str,
    category_maps: dict[str, dict[str, int]],
):
    import pandas as pd

    reverse_maps = {
        column: {code: value for value, code in mapping.items()}
        for column, mapping in category_maps.items()
    }

    rows: list[dict[str, Any]] = []

    for group_key, columns in groups.items():
        row: dict[str, Any] = {}

        for index, column in enumerate(GROUP_COLUMNS):
            value = group_key[index]

            if categorical_dtype == "category":
                row[column] = reverse_maps[column][value]
            else:
                row[column] = value

        for column, values in columns.items():
            count, total, total_squares, minimum, maximum = values

            mean = total / count if count else float("nan")

            if count > 1:
                variance = max(
                    0.0,
                    (total_squares - (total * total) / count)
                    / (count - 1.0),
                )
                standard_deviation = math.sqrt(variance)
            else:
                standard_deviation = float("nan")

            row[f"{column}_count"] = int(count)
            row[f"{column}_mean"] = mean
            row[f"{column}_std"] = standard_deviation
            row[f"{column}_min"] = minimum if count else float("nan")
            row[f"{column}_max"] = maximum if count else float("nan")

        rows.append(row)

    return pd.DataFrame(rows)


def python_loops_pipeline(
    path: Path,
    numeric_dtype: str,
    categorical_dtype: str,
    threading_mode: str,
    workers: int,
    pool: ProcessPoolExecutor | None,
):
    records = read_records(path)
    records = remove_duplicate_records(records)

    constant_columns = find_constant_record_columns(records)
    retained_columns = set(records[0]) - constant_columns if records else set()

    category_maps = (
        build_category_maps(records, retained_columns)
        if categorical_dtype == "category"
        else {}
    )

    if threading_mode == "single":
        partials = [
            loops_partition_aggregate(
                (
                    records,
                    numeric_dtype,
                    categorical_dtype,
                    constant_columns,
                    category_maps,
                )
            )
        ]
    else:
        if pool is None:
            raise RuntimeError(
                "Parallel Python loops require a prepared process pool."
            )

        partitions = [
            partition.tolist()
            for partition in np.array_split(
                np.array(records, dtype=object),
                workers,
            )
            if len(partition) > 0
        ]

        payloads = [
            (
                partition,
                numeric_dtype,
                categorical_dtype,
                constant_columns,
                category_maps,
            )
            for partition in partitions
        ]

        partials = list(pool.map(loops_partition_aggregate, payloads))

    merged = merge_loop_aggregates(partials)

    return loop_aggregate_to_frame(
        merged,
        categorical_dtype=categorical_dtype,
        category_maps=category_maps,
    )




def run_pipeline(
    config: Config,
    input_path: Path,
    workers: int,
    context: ExecutionContext,
):
    if config.engine == "pandas_numpy":
        return pandas_pipeline(
            input_path,
            config.numeric_dtype,
            config.categorical_dtype,
            config.threading_mode,
            pyarrow_backend=False,
            workers=workers,
            pool=context.pool,
        )

    if config.engine == "pandas_pyarrow":
        return pandas_pipeline(
            input_path,
            config.numeric_dtype,
            config.categorical_dtype,
            config.threading_mode,
            pyarrow_backend=True,
            workers=workers,
            pool=context.pool,
        )

    if config.engine == "polars_eager":
        return polars_pipeline(
            input_path,
            config.numeric_dtype,
            config.categorical_dtype,
            lazy=False,
        )

    if config.engine == "polars_lazy":
        return polars_pipeline(
            input_path,
            config.numeric_dtype,
            config.categorical_dtype,
            lazy=True,
        )

    if config.engine == "duckdb":
        return duckdb_pipeline(
            input_path,
            config.numeric_dtype,
            config.categorical_dtype,
            workers=workers,
        )

    if config.engine == "dask_default":
        return dask_pipeline(
            input_path,
            config.numeric_dtype,
            config.categorical_dtype,
            config.threading_mode,
            pyarrow_backend=False,
            workers=workers,
        )

    if config.engine == "dask_pyarrow":
        return dask_pipeline(
            input_path,
            config.numeric_dtype,
            config.categorical_dtype,
            config.threading_mode,
            pyarrow_backend=True,
            workers=workers,
        )

    if config.engine == "python_loops":
        return python_loops_pipeline(
            input_path,
            config.numeric_dtype,
            config.categorical_dtype,
            config.threading_mode,
            workers=workers,
            pool=context.pool,
        )

    raise ValueError(f"Unsupported engine: {config.engine}")




def internal_worker(args: argparse.Namespace) -> int:
    config = Config(
        engine=args.engine,
        data_format=args.data_format,
        numeric_dtype=args.numeric_dtype,
        categorical_dtype=args.categorical_dtype,
        threading_mode=args.threading_mode,
    )

    workers = worker_count(config.threading_mode, args.workers)
    set_thread_environment(config.threading_mode, workers)
    preload_engine(config.engine)

    input_path = Path(args.input)

    with ExecutionContext(config, workers) as context:
        with PeakMemoryMonitor() as memory_monitor:
            start = time.perf_counter()

            with RaplMeter() as energy_meter:
                result = run_pipeline(
                    config,
                    input_path,
                    workers=workers,
                    context=context,
                )

            runtime_seconds = time.perf_counter() - start

    records = canonical_records(result)

    output = {
        **asdict(config),
        "config_id": config.config_id,
        "runtime_s": runtime_seconds,
        "energy_j": energy_meter.energy_j,
        "peak_rss_mb": memory_monitor.peak_mb,
        "rows_out": len(records),
        "result_hash": result_checksum(result),
        "workers": workers,
        "status": "ok",
        "error": "",
    }

    print(json.dumps(output, default=str))
    return 0


def invoke_worker(
    script_path: Path,
    config: Config,
    input_path: Path,
    requested_workers: int | None,
) -> dict[str, Any]:
    command = [
        sys.executable,
        str(script_path),
        "_worker",
        "--engine",
        config.engine,
        "--data-format",
        config.data_format,
        "--numeric-dtype",
        config.numeric_dtype,
        "--categorical-dtype",
        config.categorical_dtype,
        "--threading-mode",
        config.threading_mode,
        "--input",
        str(input_path),
    ]

    if requested_workers is not None:
        command.extend(["--workers", str(requested_workers)])

    completed = subprocess.run(
        command,
        capture_output=True,
        text=True,
    )

    if completed.returncode != 0:
        return {
            **asdict(config),
            "config_id": config.config_id,
            "runtime_s": None,
            "energy_j": None,
            "peak_rss_mb": None,
            "rows_out": None,
            "result_hash": "",
            "workers": worker_count(
                config.threading_mode,
                requested_workers,
            ),
            "status": "error",
            "error": completed.stderr.strip() or completed.stdout.strip(),
        }

    output_lines = [
        line for line in completed.stdout.splitlines() if line.strip()
    ]

    try:
        return json.loads(output_lines[-1])
    except (IndexError, json.JSONDecodeError):
        return {
            **asdict(config),
            "config_id": config.config_id,
            "runtime_s": None,
            "energy_j": None,
            "peak_rss_mb": None,
            "rows_out": None,
            "result_hash": "",
            "workers": worker_count(
                config.threading_mode,
                requested_workers,
            ),
            "status": "error",
            "error": "Could not parse worker output:\n" + completed.stdout[-2000:],
        }




RESULT_FIELDS = [
    "block",
    "position_in_block",
    "engine",
    "data_format",
    "numeric_dtype",
    "categorical_dtype",
    "threading_mode",
    "config_id",
    "runtime_s",
    "energy_j",
    "peak_rss_mb",
    "rows_in",
    "rows_out",
    "result_hash",
    "workers",
    "status",
    "error",
    "timestamp",
]


def append_result(path: Path, result: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    file_exists = path.exists()

    with path.open("a", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=RESULT_FIELDS)

        if not file_exists:
            writer.writeheader()

        writer.writerow(
            {field: result.get(field) for field in RESULT_FIELDS}
        )


def load_completed_runs(path: Path) -> set[tuple[int, str]]:
    if not path.exists():
        return set()

    completed: set[tuple[int, str]] = set()

    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)

        for row in reader:
            if row.get("status") == "ok":
                completed.add(
                    (int(row["block"]), row["config_id"])
                )

    return completed


def benchmark(args: argparse.Namespace) -> int:
    source_csv = Path(args.csv)
    source_parquet = Path(args.parquet)
    output_path = Path(args.output)
    cache_dir = Path(args.subset_cache)

    if not source_csv.exists():
        raise FileNotFoundError(source_csv)

    if not source_parquet.exists():
        raise FileNotFoundError(
            f"{source_parquet} is missing. Run the prepare command first."
        )

    validate_source_columns(source_csv)

    configs = filter_configs(args)

    if not configs:
        raise ValueError("No configurations match the selected filters.")

    ensure_dependencies(configs)

    selected_csv, selected_parquet, rows_used = prepare_selected_input(
        source_csv=source_csv,
        source_parquet=source_parquet,
        rows=args.rows,
        row_selection=args.row_selection,
        row_seed=args.row_seed,
        cache_dir=cache_dir,
    )

    if rows_used < 2:
        print(
            "Warning: fewer than 2 rows were selected. The script will run as a "
            "smoke test, but standard deviations will be undefined and the "
            "results are not suitable for analysis.",
            file=sys.stderr,
        )

    if output_path.exists() and not args.resume:
        output_path.unlink()

    completed_runs = (
        load_completed_runs(output_path)
        if args.resume
        else set()
    )

    metadata = {
        "created_at": time.strftime("%Y-%m-%d %H:%M:%S %z"),
        "source_csv": str(source_csv.resolve()),
        "source_parquet": str(source_parquet.resolve()),
        "selected_csv": str(selected_csv.resolve()),
        "selected_parquet": str(selected_parquet.resolve()),
        "requested_rows": args.rows,
        "rows_used": rows_used,
        "row_selection": args.row_selection,
        "row_seed": args.row_seed,
        "configuration_count": len(configs),
        "runs_per_configuration": args.runs,
        "measured_run_count": len(configs) * args.runs,
        "warmup_count": 0 if args.skip_warmups else len(configs),
        "cooldown_seconds": args.cooldown,
        "randomization_seed": args.randomization_seed,
        "parallel_workers": worker_count("parallel", args.workers),
        "filters": {
            "engines": args.engines,
            "formats": args.formats,
            "numeric_dtypes": args.numeric_dtypes,
            "categorical_dtypes": args.categorical_dtypes,
            "threading_modes": args.threading_modes,
        },
    }

    metadata_path = output_path.with_name(
        output_path.stem + "_metadata.json"
    )
    metadata_path.parent.mkdir(parents=True, exist_ok=True)
    metadata_path.write_text(
        json.dumps(metadata, indent=2),
        encoding="utf-8",
    )

    script_path = Path(__file__).resolve()

    print(f"Rows used: {rows_used:,}")
    print(f"Configurations: {len(configs)}")
    print(f"Measured runs per configuration: {args.runs}")
    print(f"Total measured runs: {len(configs) * args.runs}")
    print(f"Results file: {output_path}")

    if not args.skip_warmups:
        print("\nRunning one excluded warm-up per configuration...")

        for index, config in enumerate(configs, start=1):
            input_path = (
                selected_csv
                if config.data_format == "csv"
                else selected_parquet
            )

            print(
                f"[warm-up {index}/{len(configs)}] {config.config_id}",
                flush=True,
            )

            warmup_result = invoke_worker(
                script_path=script_path,
                config=config,
                input_path=input_path,
                requested_workers=args.workers,
            )

            if warmup_result["status"] != "ok":
                print(
                    f"  Warm-up failed: {warmup_result['error']}",
                    file=sys.stderr,
                )

            if args.cooldown > 0:
                time.sleep(args.cooldown)

    for block in range(1, args.runs + 1):
        randomized_order = list(configs)

        random.Random(
            args.randomization_seed + block
        ).shuffle(randomized_order)

        print(f"\nRandomized block {block}/{args.runs}")

        for position, config in enumerate(randomized_order, start=1):
            run_key = (block, config.config_id)

            if run_key in completed_runs:
                print(
                    f"[skip] block={block} {config.config_id}",
                    flush=True,
                )
                continue

            input_path = (
                selected_csv
                if config.data_format == "csv"
                else selected_parquet
            )

            print(
                f"[{position}/{len(randomized_order)}] "
                f"block={block} {config.config_id}",
                flush=True,
            )

            result = invoke_worker(
                script_path=script_path,
                config=config,
                input_path=input_path,
                requested_workers=args.workers,
            )

            result.update(
                {
                    "block": block,
                    "position_in_block": position,
                    "rows_in": rows_used,
                    "timestamp": time.strftime(
                        "%Y-%m-%d %H:%M:%S %z"
                    ),
                }
            )

            append_result(output_path, result)

            if args.cooldown > 0:
                time.sleep(args.cooldown)

    print("\nBenchmark finished.")
    print(f"Results: {output_path}")
    print(f"Metadata: {metadata_path}")
    return 0




def doctor(args: argparse.Namespace) -> int:
    """Check dataset columns and dependencies without running a benchmark."""
    csv_path = Path(args.csv)
    parquet_path = Path(args.parquet)

    if not csv_path.exists():
        print(f"Missing CSV file: {csv_path}", file=sys.stderr)
        return 1

    try:
        validate_source_columns(csv_path)
    except Exception as error:
        print(f"Dataset check failed: {error}", file=sys.stderr)
        return 1

    configs = build_all_configs()
    missing = missing_modules(configs)

    print("Dataset schema: OK")
    print("CSV columns: " + ", ".join(read_csv_header(csv_path)))
    print(f"Parquet file: {'OK' if parquet_path.exists() else 'MISSING'}")

    if missing:
        print("Missing packages: " + ", ".join(missing))
        print(
            "Create/activate a virtual environment, then run:\n"
            + "python -m pip install "
            + " ".join(missing)
        )
        return 1

    print("Python dependencies: OK")
    return 0




def validate(args: argparse.Namespace) -> int:
    import pandas as pd

    csv_path = Path(args.csv)
    parquet_path = Path(args.parquet)

    csv_frame = pd.read_csv(csv_path)

    report: dict[str, Any] = {
        "csv_rows": len(csv_frame),
        "csv_columns": len(csv_frame.columns),
        "missing_expected_columns": sorted(
            set(EXPECTED_COLUMNS) - set(csv_frame.columns)
        ),
        "extra_columns": sorted(
            set(csv_frame.columns) - set(EXPECTED_COLUMNS)
        ),
        "duplicate_rows": int(csv_frame.duplicated().sum()),
        "constant_columns_excluding_sample_index": [
            column
            for column in csv_frame.columns
            if column not in PROTECTED_COLUMNS
            and csv_frame[column].nunique(dropna=False) <= 1
        ],
        "null_counts": {
            column: int(count)
            for column, count in csv_frame.isna().sum().items()
            if count > 0
        },
    }

    if parquet_path.exists():
        parquet_frame = pd.read_parquet(parquet_path)

        report.update(
            {
                "parquet_rows": len(parquet_frame),
                "parquet_columns": len(parquet_frame.columns),
                "same_column_order": (
                    list(csv_frame.columns)
                    == list(parquet_frame.columns)
                ),
                "same_row_count": len(csv_frame) == len(parquet_frame),
            }
        )
    else:
        report["parquet_file_missing"] = True

    print(json.dumps(report, indent=2))

    return 1 if report["missing_expected_columns"] else 0


def summarize(args: argparse.Namespace) -> int:
    import pandas as pd

    input_path = Path(args.results)
    output_path = Path(args.output)

    frame = pd.read_csv(input_path)
    frame = frame[frame["status"] == "ok"].copy()

    group_columns = [
        "engine",
        "data_format",
        "numeric_dtype",
        "categorical_dtype",
        "threading_mode",
        "config_id",
    ]

    summary = (
        frame.groupby(group_columns, dropna=False)
        .agg(
            energy_j_mean=("energy_j", "mean"),
            energy_j_median=("energy_j", "median"),
            energy_j_std=("energy_j", "std"),
            runtime_s_mean=("runtime_s", "mean"),
            runtime_s_median=("runtime_s", "median"),
            runtime_s_std=("runtime_s", "std"),
            peak_rss_mb_mean=("peak_rss_mb", "mean"),
            peak_rss_mb_median=("peak_rss_mb", "median"),
            peak_rss_mb_std=("peak_rss_mb", "std"),
            measured_runs=("block", "count"),
        )
        .reset_index()
        .sort_values(
            ["energy_j_mean", "runtime_s_mean"],
            na_position="last",
        )
    )

    summary.insert(
        0,
        "energy_rank",
        range(1, len(summary) + 1),
    )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    summary.to_csv(output_path, index=False)

    print(f"Summary written to: {output_path}")
    return 0




def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Run the 128-configuration telemetry energy benchmark "
            "with selectable dataset row counts."
        )
    )

    subparsers = parser.add_subparsers(
        dest="command",
        required=True,
    )

    prepare_parser = subparsers.add_parser(
        "prepare",
        help="Create the Parquet copy from the telemetry CSV.",
    )
    prepare_parser.add_argument("--csv", required=True)
    prepare_parser.add_argument("--parquet", required=True)
    prepare_parser.set_defaults(
        func=lambda args: (
            prepare_parquet(Path(args.csv), Path(args.parquet)) or 0
        )
    )

    doctor_parser = subparsers.add_parser(
        "doctor",
        help="Check telemetry columns and required packages without benchmarking.",
    )
    doctor_parser.add_argument("--csv", required=True)
    doctor_parser.add_argument("--parquet", required=True)
    doctor_parser.set_defaults(func=doctor)

    validate_parser = subparsers.add_parser(
        "validate",
        help="Validate the CSV and Parquet dataset structure.",
    )
    validate_parser.add_argument("--csv", required=True)
    validate_parser.add_argument("--parquet", required=True)
    validate_parser.set_defaults(func=validate)

    benchmark_parser = subparsers.add_parser(
        "benchmark",
        help="Run randomized-block benchmark measurements.",
    )
    benchmark_parser.add_argument("--csv", required=True)
    benchmark_parser.add_argument("--parquet", required=True)
    benchmark_parser.add_argument(
        "--output",
        default="benchmark_results.csv",
    )
    benchmark_parser.add_argument(
        "--rows",
        type=int,
        default=0,
        help=(
            "Number of dataset rows to use. "
            "Use 0 for the complete dataset."
        ),
    )
    benchmark_parser.add_argument(
        "--row-selection",
        choices=["first", "random"],
        default="first",
        help=(
            "How rows are selected when --rows is greater than 0. "
            "The same selected rows are used for CSV and Parquet."
        ),
    )
    benchmark_parser.add_argument(
        "--row-seed",
        type=int,
        default=407,
        help="Random seed used by --row-selection random.",
    )
    benchmark_parser.add_argument(
        "--subset-cache",
        default=".benchmark_subsets",
        help="Directory used to store prepared CSV/Parquet subsets.",
    )
    benchmark_parser.add_argument(
        "--runs",
        type=int,
        default=30,
        help=(
            "Measured runs per configuration. "
            "Each run is one randomized block."
        ),
    )
    benchmark_parser.add_argument(
        "--cooldown",
        type=float,
        default=5.0,
        help="Seconds to wait between executions.",
    )
    benchmark_parser.add_argument(
        "--randomization-seed",
        type=int,
        default=407,
    )
    benchmark_parser.add_argument(
        "--workers",
        type=int,
        default=None,
        help=(
            "Worker/thread count for parallel mode. "
            "Default: all available CPU cores."
        ),
    )
    benchmark_parser.add_argument(
        "--engines",
        default=None,
        help="Optional comma-separated engine filter.",
    )
    benchmark_parser.add_argument(
        "--formats",
        default=None,
        help="Optional comma-separated format filter.",
    )
    benchmark_parser.add_argument(
        "--numeric-dtypes",
        default=None,
        help="Optional comma-separated numeric dtype filter.",
    )
    benchmark_parser.add_argument(
        "--categorical-dtypes",
        default=None,
        help="Optional comma-separated categorical dtype filter.",
    )
    benchmark_parser.add_argument(
        "--threading-modes",
        default=None,
        help="Optional comma-separated threading filter.",
    )
    benchmark_parser.add_argument(
        "--skip-warmups",
        action="store_true",
    )
    benchmark_parser.add_argument(
        "--resume",
        action="store_true",
    )
    benchmark_parser.set_defaults(func=benchmark)

    summary_parser = subparsers.add_parser(
        "summarize",
        help="Create descriptive statistics and rank configurations.",
    )
    summary_parser.add_argument("--results", required=True)
    summary_parser.add_argument(
        "--output",
        default="benchmark_summary.csv",
    )
    summary_parser.set_defaults(func=summarize)

    worker_parser = subparsers.add_parser(
        "_worker",
        help=argparse.SUPPRESS,
    )
    worker_parser.add_argument(
        "--engine",
        required=True,
        choices=ENGINES,
    )
    worker_parser.add_argument(
        "--data-format",
        required=True,
        choices=FORMATS,
    )
    worker_parser.add_argument(
        "--numeric-dtype",
        required=True,
        choices=NUMERIC_DTYPES,
    )
    worker_parser.add_argument(
        "--categorical-dtype",
        required=True,
        choices=CATEGORICAL_DTYPES,
    )
    worker_parser.add_argument(
        "--threading-mode",
        required=True,
        choices=THREADING_MODES,
    )
    worker_parser.add_argument("--input", required=True)
    worker_parser.add_argument(
        "--workers",
        type=int,
        default=None,
    )
    worker_parser.set_defaults(func=internal_worker)

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    try:
        return int(args.func(args))
    except KeyboardInterrupt:
        print("\nInterrupted.", file=sys.stderr)
        return 130
    except Exception as error:
        print(
            f"ERROR: {type(error).__name__}: {error}",
            file=sys.stderr,
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
