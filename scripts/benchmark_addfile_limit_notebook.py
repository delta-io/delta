# ============================================================================
# AddFile Limit Scale Test - Single Notebook Cell
# ============================================================================
# Paste this into a Databricks notebook cell AFTER SparkShell is running.
# Prerequisites: `shell` (SparkShell) and `spark` (DBR SparkSession) are available.
# ============================================================================

import time, os, subprocess

CATALOG = "migration_bugbash"
SCHEMA = "addfile_benchmark"
CHECKPOINT_ROOT = "/tmp/addfile_benchmark_checkpoints"

SCALE_LEVELS = [
    # (label, target_file_count)
    ("1K",    1_000),
    ("5K",    5_000),
    ("10K",  10_000),
    ("50K",  50_000),
]

# ---- Helpers ----

def run_dbr(sql):
    """Run SQL on DBR Spark."""
    return spark.sql(sql)

def run_oss(sql):
    """Run SQL on OSS SparkShell."""
    return shell.execute_sql(sql)

def fqn(name):
    return f"{CATALOG}.{SCHEMA}.{name}"

# ---- Setup schema (via DBR) ----
print("=" * 70)
print("AddFile Limit Scale Test")
print("=" * 70)

run_dbr(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
run_dbr(f"USE {CATALOG}.{SCHEMA}")
print(f"Schema: {CATALOG}.{SCHEMA}")

# ---- Create tables with N files (via DBR) ----

def create_table(label, num_files):
    """Create a Delta table with `num_files` data files using DBR."""
    tbl = f"bench_{label.lower()}"
    print(f"\n  Creating {fqn(tbl)} with {num_files:,} files...")
    t0 = time.time()

    run_dbr(f"DROP TABLE IF EXISTS {fqn(tbl)}")
    run_dbr(f"""
        CREATE TABLE {fqn(tbl)} (
            id LONG, name STRING, value DOUBLE, ts TIMESTAMP
        ) USING DELTA
    """)

    batch_size = min(1000, num_files)
    created = 0
    while created < num_files:
        batch = min(batch_size, num_files - created)
        start_id = created
        end_id = created + batch
        run_dbr(f"""
            INSERT INTO {fqn(tbl)}
            SELECT id,
                   concat('name_', cast(id as string)),
                   id * 1.1,
                   current_timestamp()
            FROM range({start_id}, {end_id})
        """)
        created += batch
        if created % 10_000 == 0 or created == num_files:
            print(f"    {created:,}/{num_files:,} files...")

    elapsed = time.time() - t0
    detail = run_dbr(f"DESCRIBE DETAIL {fqn(tbl)}").collect()[0]
    actual = detail["numFiles"]
    location = detail["location"]
    print(f"    Done: {actual:,} files, {elapsed:.0f}s")
    return tbl, actual, location

# ---- Streaming test (via OSS SparkShell) ----

def run_streaming_test_oss(table_location, label, use_df_path):
    """
    Run streaming read via OSS SparkShell.
    Returns dict with status, rows, elapsed_s.
    """
    conf_flag = "spark.databricks.delta.streaming.distributedInitialSnapshot"
    query_name = f"bench_{label}_{int(time.time())}"
    checkpoint = f"{CHECKPOINT_ROOT}/{query_name}"
    os.makedirs(checkpoint, exist_ok=True)

    try:
        run_oss(f"SET {conf_flag} = {str(use_df_path).lower()}")

        t0 = time.time()
        # Use availableNow trigger: reads initial snapshot then stops
        result = run_oss(f"""
            CREATE OR REPLACE TEMPORARY VIEW bench_stream
            USING org.apache.spark.sql.delta
            OPTIONS (path '{table_location}')
        """)

        result = run_oss(f"SELECT count(*) as cnt FROM bench_stream")
        elapsed = time.time() - t0
        return {"status": "OK", "result": result.strip(), "elapsed_s": round(elapsed, 1)}

    except Exception as e:
        elapsed = time.time() - t0
        msg = str(e)
        if "OutOfMemoryError" in msg or "OOM" in msg:
            return {"status": "OOM", "elapsed_s": round(elapsed, 1), "error": msg[:200]}
        elif "DELTA_STREAMING_INITIAL_SNAPSHOT_TOO_LARGE" in msg:
            return {"status": "MAX_FILES_LIMIT", "elapsed_s": round(elapsed, 1)}
        else:
            return {"status": "ERROR", "elapsed_s": round(elapsed, 1), "error": msg[:200]}
    finally:
        try:
            run_oss(f"RESET {conf_flag}")
        except:
            pass

# ---- Batch read test (via OSS SparkShell) — simpler, always works ----

def run_batch_read_oss(table_location, label):
    """Batch read the table via OSS SparkShell to verify it can read it at all."""
    try:
        t0 = time.time()
        result = run_oss(f"SELECT count(*) FROM delta.`{table_location}`")
        elapsed = time.time() - t0
        return {"status": "OK", "result": result.strip(), "elapsed_s": round(elapsed, 1)}
    except Exception as e:
        elapsed = time.time() - t0
        return {"status": "ERROR", "elapsed_s": round(elapsed, 1), "error": str(e)[:200]}

# ============================================================================
# Main benchmark
# ============================================================================

results = []

for label, num_files in SCALE_LEVELS:
    print("\n" + "-" * 60)
    print(f"[{label}] Target: {num_files:,} files")
    print("-" * 60)

    # Create table via DBR
    try:
        tbl, actual_files, location = create_table(label, num_files)
    except Exception as e:
        print(f"  SETUP FAILED: {e}")
        results.append({"scale": label, "files": num_files, "setup": "FAILED"})
        continue

    # Test 1: OSS batch read (baseline)
    print(f"\n  [OSS Batch Read]...")
    batch = run_batch_read_oss(location, f"batch_{label}")
    print(f"    -> {batch}")

    # Test 2: OSS streaming — original path
    print(f"\n  [OSS Streaming - Original Path]...")
    orig = run_streaming_test_oss(location, f"orig_{label}", use_df_path=False)
    print(f"    -> {orig}")

    # Test 3: OSS streaming — DataFrame path
    print(f"\n  [OSS Streaming - DataFrame Path]...")
    dfr = run_streaming_test_oss(location, f"df_{label}", use_df_path=True)
    print(f"    -> {dfr}")

    results.append({
        "scale": label,
        "files": actual_files,
        "batch": batch,
        "original": orig,
        "dataframe": dfr,
    })

# ============================================================================
# Summary
# ============================================================================

print("\n" + "=" * 70)
print("SUMMARY")
print("=" * 70)
print(f"{'Scale':<8} {'Files':>8} {'Batch':>10} {'Original':>12} {'DataFrame':>12}")
print("-" * 55)
for r in results:
    if r.get("setup") == "FAILED":
        print(f"{r['scale']:<8} {'FAILED':>8}")
        continue
    b = r["batch"]["status"]
    o = r["original"]["status"]
    d = r["dataframe"]["status"]
    bt = f"{r['batch'].get('elapsed_s', '?')}s"
    ot = f"{r['original'].get('elapsed_s', '?')}s"
    dt = f"{r['dataframe'].get('elapsed_s', '?')}s"
    print(f"{r['scale']:<8} {r['files']:>8,} {b+'/'+bt:>10} {o+'/'+ot:>12} {d+'/'+dt:>12}")

# Cleanup
print("\nCleaning up...")
try:
    run_dbr(f"DROP SCHEMA IF EXISTS {CATALOG}.{SCHEMA} CASCADE")
    print(f"Dropped {CATALOG}.{SCHEMA}")
except:
    pass
print("Done.")
