# test-non-partitioned-delta-table-alltypes table info
This table contains 5 rows with 10 columns for each row. This table has no partition columns.
This table has only one Delta Snapshot version (version 0).

Table Schema

| Column name | Column Type |
|-------------|:-----------:|
| col1        |    byte     |
| col2        |    short    |
| col3        |     int     |
| col4        |   double    |
| col5        |    float    |
| col6        |   BitInt    |
| col7        | BigDecimal  |
| col8        |  Timestamp  |
| col9        |   String    |
| col10       |   boolean   |

This table was generated using scala/spark code:
```
park.range(0, 5)
.map(x => (
    x.toByte, x.toShort, x.toInt, x.toDouble, x.toFloat, BigInt(x), BigDecimal(x), Timestamp.valueOf(java.time.LocalDateTime.now), x.toString, true)
    )
.toDF("col1", "col2", "col3", "col4", "col5", "col6", "col7", "col8", "col9", "col10")
.write
.mode("append")
.format("delta")
.save(table)
```
