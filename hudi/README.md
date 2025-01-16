# Converting to Hudi with UniForm
## Create a table with Hudi UniForm enabled
Using spark-sql you can create a table and insert a few records into it. You will need to include the delta-hudi-assembly jar on the path.
```
spark-sql --packages io.delta:delta-spark_2.12:3.2.0-SNAPSHOT --jars delta-hudi-assembly_2.12-3.2.0-SNAPSHOT.jar --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"
```
Then you can create a table with Hudi UniForm enabled.
```
CREATE TABLE `delta_table_with_hudi` (col1 INT) USING DELTA TBLPROPERTIES('delta.universalFormat.enabledFormats' = 'hudi') LOCATION '/tmp/delta-table-with-hudi';
```
And insert a record into it.
```
INSERT INTO delta_table_with_hudi VALUES (1);
```

## Read the table with Hudi
Hudi does not currently support spark 3.5.X so you will need to launch a spark shell with spark 3.4.X or earlier.  
Instructions for launching the spark-shell with Hudi can be found [here](https://hudi.apache.org/docs/quick-start-guide#spark-shellsql).  
After launching the shell, you can read the table by enabling the hudi metadata table in the reader and loading from the path used in the create table step.
```scala
val df = spark.read.format("hudi").option("hoodie.metadata.enable", "true").load("/tmp/delta-table-with-hudi")
```
