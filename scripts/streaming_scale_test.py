# ============================================================================
# Streaming Scale Test — notebook cell
# Paste into a Databricks notebook cell AFTER SparkShell is built.
# Requires: `shell` (SparkShell, built but server need NOT be running),
#           `spark` (DBR SparkSession), `uc_config` (UCConfig)
# ============================================================================
import subprocess, os, json, time, shutil

# ---------- Config ----------
CATALOG = "migration_bugbash"
SCHEMA  = "addfile_benchmark"
SCALE_LEVELS = [
    ("1K",     1_000),
    ("5K",     5_000),
    ("10K",   10_000),
    ("50K",   50_000),
    ("100K", 100_000),
    ("200K", 200_000),
    ("500K", 500_000),
]

# ---------- Helpers ----------
def fqn(t):
    return f"{CATALOG}.{SCHEMA}.{t}"

def create_table(label, n):
    """Create a Delta table with ~n files via DBR."""
    tbl = f"bench_{label.lower()}"
    print(f"  Creating {fqn(tbl)} with {n:,} files …")
    t0 = time.time()
    spark.sql(f"DROP TABLE IF EXISTS {fqn(tbl)}")
    spark.sql(f"CREATE TABLE {fqn(tbl)} (id LONG, val DOUBLE) USING DELTA")
    batch = min(5000, n)
    created = 0
    while created < n:
        b = min(batch, n - created)
        spark.sql(f"""
            INSERT INTO {fqn(tbl)}
            SELECT id, id * 1.1 FROM range({created}, {created + b})
        """)
        created += b
        if created % 50000 == 0 or created == n:
            print(f"    {created:,}/{n:,}")
    detail = spark.sql(f"DESCRIBE DETAIL {fqn(tbl)}").collect()[0]
    elapsed = time.time() - t0
    print(f"  -> {detail['numFiles']:,} files, {elapsed:.0f}s")
    return tbl, detail["numFiles"], detail["location"]


def _run_spark_shell(scala_code, configs, timeout=900, driver_memory="4g"):
    """
    Launch spark-shell from the SparkShell assembly JAR, pipe Scala code,
    return (stdout, stderr, returncode).
    """
    jar = str(shell.jar_path)
    java_home = os.environ.get("JAVA_HOME", "/usr/lib/jvm/java-17-openjdk-amd64")
    java_cmd = os.path.join(java_home, "bin", "java")
    os.makedirs("/tmp/spark-local", exist_ok=True)

    cmd = [
        java_cmd, "-cp", jar,
        "org.apache.spark.deploy.SparkSubmit",
        "--master", "local[*]",
        "--driver-memory", driver_memory,
        "--conf", "spark.local.dir=/tmp/spark-local",
        "--conf", "spark.ui.enabled=false",
        "--conf", "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension",
        "--conf", "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog",
    ]
    # UC configs
    if hasattr(uc_config, "uri") and uc_config.uri:
        cat = uc_config.catalog
        cmd += [
            "--conf", f"spark.sql.catalog.{cat}=io.unitycatalog.spark.UCSingleCatalog",
            "--conf", f"spark.sql.catalog.{cat}.uri={uc_config.uri}",
            "--conf", f"spark.sql.catalog.{cat}.token={uc_config.token}",
            "--conf", f"spark.sql.defaultCatalog={cat}",
        ]
    # extra configs
    for k, v in configs.items():
        cmd += ["--conf", f"{k}={v}"]

    cmd += ["--class", "org.apache.spark.repl.Main", "spark-shell"]

    env = os.environ.copy()
    env["LOCAL_DIRS"] = "/tmp/spark-local"
    env["SPARK_LOCAL_DIRS"] = "/tmp/spark-local"

    return subprocess.run(
        cmd,
        input=scala_code.strip() + "\n:quit\n",
        capture_output=True, text=True,
        env=env, timeout=timeout,
        cwd=str(shell.work_dir) if shell.work_dir else None,
    )


def run_streaming(table_loc, use_df_path, label, max_files=None, timeout=900):
    """Run one streaming test. Returns dict with status/rows/time_s."""
    ckpt = f"/tmp/oss_ckpt/{label}_{int(time.time())}"
    configs = {
        "spark.databricks.delta.streaming.distributedInitialSnapshot":
            str(use_df_path).lower(),
        "spark.hadoop.fs.s3.impl.disable.cache": "true",
    }
    if max_files is not None:
        configs["spark.databricks.delta.streaming.initialSnapshot.maxFiles"] = str(max_files)

    scala = f"""
import org.apache.spark.sql.streaming.Trigger
val ckpt = "{ckpt}"
val t0 = System.currentTimeMillis()
try {{
  val q = spark.readStream.format("delta").load("{table_loc}")
    .writeStream
    .format("noop")
    .option("checkpointLocation", ckpt)
    .trigger(Trigger.AvailableNow())
    .start()
  q.awaitTermination({timeout * 1000}L)
  val elapsed = (System.currentTimeMillis() - t0) / 1000.0
  val rows = q.recentProgress.map(_.numInputRows).sum
  q.stop()
  println("STREAM_RESULT:" + s\"\"\"{{"status":"OK","rows":${{rows}},"time_s":${{elapsed}}}}\"\"\")
}} catch {{
  case _: OutOfMemoryError =>
    val elapsed = (System.currentTimeMillis() - t0) / 1000.0
    println("STREAM_RESULT:" + s\"\"\"{{"status":"OOM","time_s":${{elapsed}}}}\"\"\")
  case e: Exception =>
    val elapsed = (System.currentTimeMillis() - t0) / 1000.0
    val msg = e.getMessage.replace("\\\"", "'").take(200)
    if (msg.contains("DELTA_STREAMING_INITIAL_SNAPSHOT_TOO_LARGE"))
      println("STREAM_RESULT:" + s\"\"\"{{"status":"MAX_FILES","time_s":${{elapsed}}}}\"\"\")
    else
      println("STREAM_RESULT:" + s\"\"\"{{"status":"ERROR","error":"${{msg}}","time_s":${{elapsed}}}}\"\"\")
}}
"""
    print(f"    spark-shell: use_df={use_df_path}, max_files={max_files}")
    try:
        r = _run_spark_shell(scala, configs, timeout=timeout)
    except subprocess.TimeoutExpired:
        return {"status": "TIMEOUT", "time_s": timeout}

    for line in r.stdout.split('\n'):
        if "STREAM_RESULT:" in line:
            raw = line.strip().split("STREAM_RESULT:", 1)[1]
            try:
                return json.loads(raw)
            except json.JSONDecodeError:
                return {"status": "PARSE_ERROR", "raw": raw}

    tail = (r.stderr or r.stdout or "")[-500:]
    return {"status": "NO_RESULT", "rc": r.returncode, "error": tail}


# ============================================================================
# Main
# ============================================================================
print("=" * 70)
print("Streaming Scale Test: Original vs DataFrame Path")
print("=" * 70)

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
results = []

for label, n in SCALE_LEVELS:
    print(f"\n{'─'*60}")
    print(f"[{label}] Target: {n:,} files")
    print(f"{'─'*60}")

    try:
        tbl, actual, loc = create_table(label, n)
    except Exception as e:
        print(f"  SETUP FAILED: {e}")
        results.append({"scale": label, "files": n, "setup": "FAILED"})
        continue

    # Test 1: Original path (raise maxFiles to allow it through)
    print(f"\n  [Original] maxFiles={actual+1}")
    orig = run_streaming(loc, use_df_path=False, label=f"orig_{label}",
                         max_files=actual + 1)
    print(f"    -> {orig}")

    # Test 2: DataFrame path (distributedInitialSnapshot=true)
    print(f"\n  [DataFrame] distributedInitialSnapshot=true")
    df_r = run_streaming(loc, use_df_path=True, label=f"df_{label}")
    print(f"    -> {df_r}")

    results.append({
        "scale": label, "files": actual,
        "original": orig, "dataframe": df_r,
    })

    spark.sql(f"DROP TABLE IF EXISTS {fqn(tbl)}")

# ============================================================================
# Summary
# ============================================================================
print(f"\n{'='*70}")
print("SUMMARY")
print(f"{'='*70}")
print(f"{'Scale':<8} {'Files':>8}  {'Original':>22}  {'DataFrame':>22}")
print("-" * 66)
for r in results:
    if r.get("setup") == "FAILED":
        print(f"{r['scale']:<8} {'SETUP FAIL':>8}")
        continue
    def fmt(d):
        return f"{d['status']}/{d.get('time_s','?')}s"
    print(f"{r['scale']:<8} {r['files']:>8,}  {fmt(r['original']):>22}  {fmt(r['dataframe']):>22}")
print(f"\nOK=success, OOM=OutOfMemory, MAX_FILES=limit hit, ERROR=other")

# Cleanup
print("\nCleaning up …")
try:
    spark.sql(f"DROP SCHEMA IF EXISTS {CATALOG}.{SCHEMA} CASCADE")
except Exception:
    pass
shutil.rmtree("/tmp/oss_ckpt", ignore_errors=True)
print("Done.")
