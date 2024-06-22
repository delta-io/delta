# test-non-partitioned-delta-table-4-versions table info
This table contains 75 rows with 3 columns for each row. This table has no partition columns.
This table has four Delta Snapshot versions.

Table schea:

| Column name | Column Type |
|-------------|:-----------:|
| col1        |    long     |
| col2        |    long     |
| col3        |   string    |

This table was generated using scala/spark code:
```
spark.range(0, 5) 
      .map(x => (x, x % 5, s"test-${x % 2}"))
      .toDF("col1", "col2", "col3")
      .write
      .mode("append")
      .format("delta")
      .save(table)
```
This code was executed 4 times, adding new version to Delta table.
Each time spark.range(a, b) had different values, resulting with different number of rows per version

| Version number | Number of rows for version | col1 min value | col1 max value |
|----------------|:--------------------------:|:--------------:|:--------------:|
| 0              |             5              |       0        |       4        |
| 1              |             10             |       5        |       14       |
| 2              |             20             |       15       |       34       |
| 3              |             40             |       35       |       74       |
