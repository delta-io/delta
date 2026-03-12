#!/usr/bin/env python3
# ============================================================================
# AddFile Limit Benchmark for V2 Streaming DataFrame-based Initial Snapshot
# ============================================================================
# Run from a Databricks notebook (Python).
#
# Tests the maximum number of AddFiles the DataFrame-based path can handle
# by creating Delta tables of increasing size and streaming from them.
#
# Adjust CATALOG / SCHEMA / CHECKPOINT_ROOT before running.
# ============================================================================

import time
import traceback
import shutil
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

# ============================================================================
# Configuration — EDIT THESE
# ============================================================================

CATALOG = "migration_bugbash"
SCHEMA = "addfile_benchmark"
CHECKPOINT_ROOT = "/tmp/addfile_benchmark_checkpoints"

SCALE_LEVELS = [
    # (label, target_file_count, rows_per_file)
    ("1K",    1_000,  1),
    ("5K",    5_000,  1),
    ("10K",  10_000,  1),
    ("50K",  50_000,  1),
    ("100K", 100_000, 1),
    ("200K", 200_000, 1),
    # Uncomment for extreme tests (table setup is slow):
    # ("500K", 500_000, 1),
    # ("1M", 1_000_000, 1),
]

DRIVER_MEMORY = spark.conf.get("spark.driver.memory", "unknown")
EXECUTOR_MEMORY = spark.conf.get("spark.executor.memory", "unknown")

# ============================================================================
# Setup catalog/schema
# ============================================================================

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(
    f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA} "
    f"COMMENT 'AddFile limit benchmark for V2 streaming'"
)
spark.sql(f"USE {CATALOG}.{SCHEMA}")

# ============================================================================
# Helpers
# ============================================================================

def fqn(table_name):
    return f"{CATALOG}.{SCHEMA}.{table_name}"


def create_table_with_n_files(table_name, num_files, rows_per_file=1):
    """
    Creates a managed Delta table with exactly `num_files` data files.
    Uses batch append with repartition() to control file count.
    """
    spark.sql(f"DROP TABLE IF EXISTS {fqn(table_name)}")

    spark.sql(f"""
        CREATE TABLE {fqn(table_name)} (
            id LONG,
            name STRING,
            value DOUBLE,
            ts TIMESTAMP
        ) USING DELTA
    """)

    batch_size = min(1000, num_files)
    files_created = 0

    while files_created < num_files:
        remaining = num_files - files_created
        current_batch = min(batch_size, remaining)

        df = spark.range(
            files_created * rows_per_file,
            (files_created + current_batch) * rows_per_file
        ).select(
            F.col("id"),
            F.concat(F.lit("name_"), F.col("id")).alias("name"),
            (F.col("id") * 1.1).alias("value"),
            F.current_timestamp().alias("ts")
        ).repartition(current_batch)

        df.write.format("delta").mode("append").saveAsTable(fqn(table_name))
        files_created += current_batch

        if files_created % 10_000 == 0 or files_created == num_files:
            print(f"  Created {files_created}/{num_files} files...")

    return files_created


def count_addfiles(table_name):
    detail = spark.sql(f"DESCRIBE DETAIL {fqn(table_name)}")
    return detail.select("numFiles").collect()[0][0]


def get_table_location(table_name):
    detail = spark.sql(f"DESCRIBE DETAIL {fqn(table_name)}")
    return detail.select("location").collect()[0][0]


def run_streaming_test(table_name, label, use_df_path, max_files_override=None):
    """
    Run a streaming read from the given table and return result dict.

    Args:
        table_name: managed table name (no catalog/schema prefix)
        label: unique label for this test run (used for query name / checkpoint)
        use_df_path: True = distributedInitialSnapshot, False = original path
        max_files_override: if set, override maxInitialSnapshotFiles config
    """
    conf_flag = "spark.databricks.delta.streaming.distributedInitialSnapshot"
    conf_max = "spark.databricks.delta.streaming.initialSnapshot.maxFiles"
    query_name = f"bench_{label}_{int(time.time())}"
    checkpoint = f"{CHECKPOINT_ROOT}/{query_name}"

    spark.conf.set(conf_flag, str(use_df_path).lower())
    if max_files_override is not None:
        spark.conf.set(conf_max, str(max_files_override))

    table_loc = get_table_location(table_name)

    try:
        start_time = time.time()
        query = (
            spark.readStream
            .format("delta")
            .load(table_loc)
            .writeStream
            .format("noop")
            .queryName(query_name)
            .option("checkpointLocation", checkpoint)
            .trigger(availableNow=True)
            .start()
        )
        query.awaitTermination(timeout=600)
        elapsed = time.time() - start_time

        metrics = query.recentProgress
        total_rows = sum(p.get("numInputRows", 0) for p in metrics) if metrics else -1
        query.stop()
        return {"status": "OK", "rows": total_rows, "elapsed_s": round(elapsed, 1)}
    except Exception as e:
        elapsed = time.time() - start_time
        msg = str(e)
        if "DELTA_STREAMING_INITIAL_SNAPSHOT_TOO_LARGE" in msg:
            return {"status": "MAX_FILES_LIMIT", "elapsed_s": round(elapsed, 1)}
        elif "OutOfMemoryError" in msg or "OOM" in msg:
            return {"status": "OOM", "error": msg[:300], "elapsed_s": round(elapsed, 1)}
        else:
            return {"status": "ERROR", "error": msg[:300], "elapsed_s": round(elapsed, 1)}
    finally:
        spark.conf.unset(conf_flag)
        if max_files_override is not None:
            spark.conf.unset(conf_max)


# ============================================================================
# Main benchmark
# ============================================================================

print("=" * 80)
print("AddFile Limit Benchmark - V2 Streaming DataFrame-based Initial Snapshot")
print("=" * 80)
print(f"Catalog:            {CATALOG}")
print(f"Schema:             {SCHEMA}")
print(f"Driver memory:      {DRIVER_MEMORY}")
print(f"Executor memory:    {EXECUTOR_MEMORY}")
print(f"Shuffle partitions: {spark.conf.get('spark.sql.shuffle.partitions', '200')}")
print(f"Scales:             {[s[0] for s in SCALE_LEVELS]}")
print()

results = []

for label, target_files, rows_per_file in SCALE_LEVELS:
    table_name = f"bench_{label.lower()}"
    print("-" * 60)
    print(f"[{label}] Target: {target_files:,} files")
    print("-" * 60)

    # --- Setup ---
    print(f"  Creating {fqn(table_name)}...")
    setup_start = time.time()
    try:
        create_table_with_n_files(table_name, target_files, rows_per_file)
        setup_time = time.time() - setup_start
        verified = count_addfiles(table_name)
        print(f"  Done: {verified:,} files in {setup_time:.1f}s")
    except Exception as e:
        print(f"  SETUP FAILED: {e}")
        traceback.print_exc()
        results.append({"scale": label, "target_files": target_files, "setup": "FAILED"})
        continue

    # --- Test 1: Original in-memory path (raise maxFiles to allow through) ---
    print(f"\n  [Original path] maxFiles={target_files + 1}...")
    orig = run_streaming_test(
        table_name, f"orig_{label}", use_df_path=False,
        max_files_override=target_files + 1
    )
    print(f"    -> {orig}")

    # --- Test 2: DataFrame distributed path ---
    print(f"\n  [DataFrame path] distributedInitialSnapshot=true...")
    dfr = run_streaming_test(
        table_name, f"df_{label}", use_df_path=True
    )
    print(f"    -> {dfr}")

    results.append({
        "scale": label,
        "target_files": target_files,
        "actual_files": verified,
        "setup_time_s": round(setup_time, 1),
        "original": orig,
        "dataframe": dfr,
    })

    # Cleanup
    try:
        spark.sql(f"DROP TABLE IF EXISTS {fqn(table_name)}")
    except:
        pass
    print()

# ============================================================================
# Summary
# ============================================================================

print("=" * 80)
print("SUMMARY")
print("=" * 80)
header = f"{'Scale':<8} {'Files':>10} {'Original':>15} {'Orig Time':>10} {'DataFrame':>15} {'DF Time':>10}"
print(header)
print("-" * len(header))
for r in results:
    if r.get("setup") == "FAILED":
        print(f"{r['scale']:<8} {'SETUP FAIL':>10}")
        continue
    o = r["original"]
    d = r["dataframe"]
    ot = str(o.get("elapsed_s", "?")) + "s"
    dt = str(d.get("elapsed_s", "?")) + "s"
    print(f"{r['scale']:<8} {r['actual_files']:>10,} {o['status']:>15} {ot:>10} {d['status']:>15} {dt:>10}")

print()
print("Status legend:")
print("  OK             = stream completed, all data consumed")
print("  MAX_FILES_LIMIT = hit maxInitialSnapshotFiles config guard")
print("  OOM            = java.lang.OutOfMemoryError")
print("  ERROR          = other exception (check error field)")
print()

# Cleanup schema
try:
    spark.sql(f"DROP SCHEMA IF EXISTS {CATALOG}.{SCHEMA} CASCADE")
    print(f"Cleaned up schema {CATALOG}.{SCHEMA}")
except:
    pass
