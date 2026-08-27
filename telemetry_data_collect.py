import argparse
import csv
import datetime
import os
import sys
import time

try:
    import psutil
except ImportError:
    sys.exit(
        "psutil is required. Install it with:  pip install psutil"
    )

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
    import pyarrow.csv as pa_csv
    HAS_PYARROW = True
except ImportError:
    HAS_PYARROW = False
    print(
        "Warning: pyarrow not found. Only CSV will be written.\n"
        "Install with:  pip install pyarrow"
    )


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

CSV_PATH     = "benchmark_data.csv"
PARQUET_PATH = "benchmark_data.parquet"
LOG_PATH     = "collection_log.txt"

FIELDNAMES = [
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




def get_cpu_temp() -> float | None:
    """
    Return CPU package temperature in °C, or None if unavailable.
    Works on Linux with lm-sensors; gracefully returns None elsewhere.
    """
    try:
        temps = psutil.sensors_temperatures()
        if not temps:
            return None
        # Try common sensor names in order of preference
        for key in ("coretemp", "k10temp", "acpitz", "cpu_thermal"):
            if key in temps:
                entries = temps[key]
                # Use the first 'Package' entry if present, else average all
                package = [e for e in entries if "package" in e.label.lower()]
                if package:
                    return round(package[0].current, 2)
                return round(sum(e.current for e in entries) / len(entries), 2)
        # Fallback: use whatever sensor is available
        first_key = next(iter(temps))
        entries = temps[first_key]
        if entries:
            return round(entries[0].current, 2)
    except (AttributeError, StopIteration):
        pass
    return None


def bin_cpu_state(pct: float) -> str:
    if pct < 20:
        return "idle"
    elif pct < 50:
        return "low"
    elif pct < 80:
        return "medium"
    return "high"


def bin_ram_state(pct: float) -> str:
    if pct < 40:
        return "low"
    elif pct < 75:
        return "medium"
    return "high"


def bin_disk_activity(read_mb: float, write_mb: float) -> str:
    total = read_mb + write_mb
    if total < 0.1:
        return "idle"
    elif total < 10.0:
        return "light"
    return "heavy"


def bin_swap_state(swap_mb: float) -> str:
    if swap_mb < 1.0:
        return "none"
    elif swap_mb < 100.0:
        return "low"
    return "high"


def bin_process_load_tier(proc_count: int) -> str:
    if proc_count < 100:
        return "light"
    elif proc_count < 300:
        return "normal"
    return "heavy"


def bin_net_activity(sent_kb: float, recv_kb: float) -> str:
    total = sent_kb + recv_kb
    if total < 1.0:
        return "idle"
    elif total < 100.0:
        return "light"
    return "heavy"


def bin_freq_tier(freq_mhz: float | None, session_min: float, session_max: float) -> str:
    """
    Bin CPU frequency relative to the min/max observed so far THIS SESSION,
    since absolute frequency ranges vary by CPU model. Falls back to "mid"
    if freq is unavailable or the session range is degenerate (min==max).
    """
    if freq_mhz is None:
        return "unknown"
    spread = session_max - session_min
    if spread <= 0:
        return "mid"
    position = (freq_mhz - session_min) / spread
    if position < 0.33:
        return "low"
    elif position < 0.66:
        return "mid"
    return "high"


def bin_temp_state(temp_c: float | None) -> str:
    if temp_c is None:
        return "unknown"
    if temp_c < 40:
        return "cool"
    elif temp_c < 60:
        return "warm"
    return "hot"


def bin_time_period(dt: datetime.datetime) -> str:
    hour = dt.hour
    if 5 <= hour < 12:
        return "morning"
    elif 12 <= hour < 17:
        return "afternoon"
    elif 17 <= hour < 21:
        return "evening"
    return "night"


def bin_weekday_type(dt: datetime.datetime) -> str:
    return "weekend" if dt.weekday() >= 5 else "weekday"


def get_busiest_core() -> str:
    """Return the label of the CPU core with the highest per-core utilisation."""
    try:
        per_core = psutil.cpu_percent(percpu=True)
        idx = per_core.index(max(per_core))
        return f"core_{idx}"
    except Exception:
        return "core_0"


def count_rows_in_csv(path: str) -> int:
    """Count data rows (excluding header) in an existing CSV."""
    if not os.path.exists(path):
        return 0
    with open(path, "r") as f:
        # Subtract 1 for the header line
        return max(0, sum(1 for _ in f) - 1)


def write_log(start: datetime.datetime, end: datetime.datetime,
              new_rows: int, total_rows: int) -> None:
    duration = end - start
    with open(LOG_PATH, "w") as f:
        f.write("=== Telemetry Collection Log (most recent session) ===\n")
        f.write(f"Session start    : {start.isoformat()}\n")
        f.write(f"Session end      : {end.isoformat()}\n")
        f.write(f"Session duration : {duration}\n")
        f.write(f"New rows (this run)   : {new_rows:,}\n")
        f.write(f"Total rows in file    : {total_rows:,}\n")
        f.write(f"CSV path     : {os.path.abspath(CSV_PATH)}\n")
        f.write(f"Parquet path : {os.path.abspath(PARQUET_PATH)}\n")
        f.write("\nSchema\n------\n")
        for name in FIELDNAMES:
            f.write(f"  {name}\n")
    print(f"\nLog written to {LOG_PATH}")


def convert_csv_to_parquet(csv_path: str, parquet_path: str) -> None:
    """Convert the finished CSV to Parquet using PyArrow."""
    if not HAS_PYARROW:
        print("Skipping Parquet conversion (pyarrow not installed).")
        return
    print(f"Converting {csv_path} → {parquet_path} ...")
    table = pa_csv.read_csv(csv_path)
    pq.write_table(table, parquet_path, compression="snappy")
    print(f"Parquet written: {parquet_path}")



def collect(interval: float, rows_to_collect: int, overwrite: bool) -> None:

    file_exists   = os.path.exists(CSV_PATH)
    existing_rows = count_rows_in_csv(CSV_PATH) if (file_exists and not overwrite) else 0

    if overwrite and file_exists:
        print(f"{CSV_PATH} exists but --overwrite was passed — starting fresh.")
    elif file_exists:
        print(f"{CSV_PATH} exists with {existing_rows:,} rows — "
              f"appending {rows_to_collect:,} new rows (pass --overwrite to start fresh instead).")
    else:
        print(f"{CSV_PATH} does not exist yet — creating it.")

    # Append if the file exists and we're not overwriting; write fresh otherwise.
    file_mode    = "a" if (file_exists and not overwrite) else "w"
    write_header = file_mode == "w"

    # Prime disk and network counters (first delta will be 0)
    prev_disk  = psutil.disk_io_counters()
    prev_net   = psutil.net_io_counters()

    new_rows_written = 0
    start_time        = datetime.datetime.now()

    # Track session-observed CPU frequency range for freq_tier binning
    # (relative binning since absolute MHz ranges vary by CPU model)
    session_freq_min = None
    session_freq_max = None

    print(f"Collecting telemetry → {CSV_PATH}")
    print(f"Collecting {rows_to_collect:,} new rows this session  |  "
          f"Interval: {interval}s  |  "
          f"Estimated duration: {datetime.timedelta(seconds=int(rows_to_collect * interval))}")
    print("Press Ctrl+C to stop early (rows collected so far will be saved).\n")

    try:
        with open(CSV_PATH, file_mode, newline="", buffering=1) as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=FIELDNAMES)
            if write_header:
                writer.writeheader()

            while new_rows_written < rows_to_collect:
                loop_start = time.perf_counter()

                # --- Snapshot all metrics ---
                ts          = datetime.datetime.now().isoformat(timespec="milliseconds")
                cpu_pct     = psutil.cpu_percent(interval=None)
                cpu_freq    = psutil.cpu_freq()
                freq_mhz    = round(cpu_freq.current, 2) if cpu_freq else None
                cpu_temp    = get_cpu_temp()          # None on unsupported systems → NaN
                mem         = psutil.virtual_memory()
                swap        = psutil.swap_memory()
                disk        = psutil.disk_io_counters()
                net         = psutil.net_io_counters()
                proc_count  = len(psutil.pids())
                core_id     = get_busiest_core()

                # Deltas (handle None if counters unavailable)
                disk_read_mb  = 0.0
                disk_write_mb = 0.0
                net_sent_kb   = 0.0
                net_recv_kb   = 0.0

                if disk and prev_disk:
                    disk_read_mb  = round((disk.read_bytes  - prev_disk.read_bytes)  / 1e6, 4)
                    disk_write_mb = round((disk.write_bytes - prev_disk.write_bytes) / 1e6, 4)
                if net and prev_net:
                    net_sent_kb = round((net.bytes_sent - prev_net.bytes_sent) / 1e3, 4)
                    net_recv_kb = round((net.bytes_recv - prev_net.bytes_recv) / 1e3, 4)

                prev_disk = disk
                prev_net  = net

                # Update session frequency range, then bin relative to it
                if freq_mhz is not None:
                    if session_freq_min is None or freq_mhz < session_freq_min:
                        session_freq_min = freq_mhz
                    if session_freq_max is None or freq_mhz > session_freq_max:
                        session_freq_max = freq_mhz

                now_dt = datetime.datetime.now()

                # Derived categorical columns
                cpu_state          = bin_cpu_state(cpu_pct)
                ram_state          = bin_ram_state(mem.percent)
                disk_activity      = bin_disk_activity(disk_read_mb, disk_write_mb)
                swap_state         = bin_swap_state(round(swap.used / 1e6, 2))
                process_load_tier  = bin_process_load_tier(proc_count)
                net_activity       = bin_net_activity(net_sent_kb, net_recv_kb)
                freq_tier          = bin_freq_tier(
                    freq_mhz,
                    session_freq_min if session_freq_min is not None else 0.0,
                    session_freq_max if session_freq_max is not None else 0.0,
                )
                temp_state         = bin_temp_state(cpu_temp)
                time_period        = bin_time_period(now_dt)
                weekday_type       = bin_weekday_type(now_dt)

                row = {
                    "timestamp"         : ts,
                    "cpu_freq_mhz"      : freq_mhz,
                    "cpu_temp_celsius"  : cpu_temp,   # NaN where sensor unavailable
                    "cpu_percent"       : round(cpu_pct, 2),
                    "ram_used_mb"       : round(mem.used / 1e6, 2),
                    "ram_percent"       : round(mem.percent, 2),
                    "disk_read_mb"      : disk_read_mb,
                    "disk_write_mb"     : disk_write_mb,
                    "net_sent_kb"       : net_sent_kb,
                    "net_recv_kb"       : net_recv_kb,
                    "swap_used_mb"      : round(swap.used / 1e6, 2),
                    "process_count"     : proc_count,
                    "core_id"           : core_id,
                    "cpu_state"         : cpu_state,
                    "ram_state"         : ram_state,
                    "disk_activity"     : disk_activity,
                    "swap_state"        : swap_state,
                    "process_load_tier" : process_load_tier,
                    "net_activity"      : net_activity,
                    "freq_tier"         : freq_tier,
                    "temp_state"        : temp_state,
                    "time_period"       : time_period,
                    "weekday_type"      : weekday_type,
                    "sample_index"      : existing_rows + new_rows_written,
                }

                writer.writerow(row)
                new_rows_written += 1

                # Progress report every 10,000 new rows
                if new_rows_written % 10_000 == 0:
                    elapsed   = datetime.datetime.now() - start_time
                    remaining = rows_to_collect - new_rows_written
                    eta_secs  = remaining * interval
                    print(
                        f"  {new_rows_written:>10,} / {rows_to_collect:,} new rows  |  "
                        f"Total in file: {existing_rows + new_rows_written:,}  |  "
                        f"Elapsed: {elapsed}  |  "
                        f"ETA: {datetime.timedelta(seconds=int(eta_secs))}"
                    )

                # Sleep for remainder of interval
                elapsed_loop = time.perf_counter() - loop_start
                sleep_time   = max(0.0, interval - elapsed_loop)
                if sleep_time > 0:
                    time.sleep(sleep_time)

    except KeyboardInterrupt:
        print(f"\nInterrupted. {new_rows_written:,} new rows written this session.")

    total_rows = existing_rows + new_rows_written
    end_time   = datetime.datetime.now()
    print(f"\nCollection complete: {new_rows_written:,} new rows written  |  "
          f"{total_rows:,} total rows now in {CSV_PATH}")

    # Convert to Parquet
    convert_csv_to_parquet(CSV_PATH, PARQUET_PATH)

    # Write log
    write_log(start_time, end_time, new_rows_written, total_rows)




def parse_args():
    parser = argparse.ArgumentParser(
        description="Collect system telemetry for energy benchmarking dataset. "
                     "By default, appends new rows to any existing CSV."
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=1.0,
        help="Sampling interval in seconds (default: 1.0). Sub-second "
             "intervals are not recommended — see module docstring."
    )
    parser.add_argument(
        "--target-rows",
        type=int,
        default=4797,
        dest="rows_to_collect",
        help="Number of NEW rows to collect THIS SESSION (default: 100). "
             "If the CSV already has rows, this many are appended on top "
             "of whatever is already there."
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Discard any existing benchmark_data.csv and start fresh. "
             "Without this flag, new rows are always appended to the "
             "existing file if one is present."
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    collect(
        interval        = args.interval,
        rows_to_collect = args.rows_to_collect,
        overwrite       = args.overwrite,
    )
