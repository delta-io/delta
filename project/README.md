# Refreshing Spark test durations

The test scheduler reads `project/test-durations.csv`. It balances the suites that
each project discovers across shards and JVM groups. It uses a default duration
for suites that have no timing data. Old names in the CSV do not reserve any load.

Refresh the full timing table with the authenticated GitHub CLI:

```sh
python3 project/scripts/collect_test_durations.py --last-n-runs 12 --max-minutes 0
```

The collector checks recent completed `Delta Spark` workflows on `master`. A failed
workflow can still supply timings for a Spark version if all eight shard jobs pass
and all eight report artifacts are present. Failed or incomplete versions are
excluded. Keep `SHARDS_PER_SPARK_VERSION` in the collector in sync with the workflow.

The collector keeps all suites by default. It takes each suite's maximum time in
each run, then averages those values across runs. It prints the run dates, suite
counts, and sample counts. Check these before committing a refresh. The optional
`--top-n` argument limits the output for inspection. Use the full table for CI.

The default five-minute limit is checked between downloads. It does not interrupt
a download. A version that is incomplete when the limit is reached is excluded.
Use `--max-minutes 0` to process all selected runs.

Run the collector and scheduler regression checks from the repository root:

```sh
python3 -m unittest discover -s project/tests -p 'test_collect_test_durations.py'
NUM_SHARDS=8 build/sbt 'reload plugins' 'Test / runMain TestParallelizationTest'
```
