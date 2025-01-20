# Updating delta-spark TestParallelization Top 50 Slowest Test Suites List

- Cherry-pick changes from https://github.com/delta-io/delta/pull/3694
- That PR adds a test report listener to delta-spark that will output csv files containing per-JVM, per-group (thread), and per-test runtimes
- Run the CI and download the generated csv artifacts
- You can use the following pyspark code to get the top 50 slowest test suites
- You can copy and paste that into Chat GPT and ask it to format it as a Scala List

```python
from pyspark.sql.functions import col, sum
from pyspark.sql.types import StructType, StructField, StringType, LongType

schema = StructType([
    StructField("test_suite", StringType(), True),
    StructField("test_name", StringType(), True),
    StructField("execution_time_ms", LongType(), True),
    StructField("result", StringType(), True)
])

csv_dir = "..."

spark.read.csv(csv_dir, schema=schema) \
    .filter(col("execution_time_ms") != -1) \
    .groupBy("test_suite") \
    .agg((sum("execution_time_ms") / 60000).alias("execution_time_mins")) \
    .orderBy(col("execution_time_mins").desc()) \
    .limit(50) \
    .select("test_suite", "execution_time_mins") \
    .show(50, truncate=False)
```
