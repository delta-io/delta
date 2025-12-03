# sql-delta-import

 Imports data from a relational database or any other JDBC source into your Delta Lake. 
 Import either entire table or only a subset of columns, control level of parallelism, 
 include any custom transformations
 
Destination delta table has to exist before import. It's schema will be used to infer 
desired columns and their data types

## Basic Usage

You can use included runner to import data without custom transformations by submitting
a spark job

Ex: split data in source table by `id` into 10 chunks (default) and import it into 
destination delta table

```shell script
spark-submit /
--class "io.delta.connectors.spark.jdbc.ImportRunner" sql-delta-import.jar /
--jdbc-url jdbc:mysql://hostName:port/database /
--source source.table
--destination destination.table
--split-by id
```
A good `split-by` column will be indexed and ideally will have close to uniform distribution
of data between it's `min` and `max` values

## Control degree of import parallelism using `chunks` parameter and spark executor configuration

```shell script
spark-submit --num-executors 15 --executor-cores 4 /
--conf spark.databricks.delta.optimizeWrite.enabled=true /
--conf spark.databricks.delta.autoCompact.enabled=true /
--class "io.delta.connectors.spark.jdbc.ImportRunner" sql-delta-import.jar /
--jdbc-url jdbc:mysql://hostName:port/database /
--source source.table
--destination destination.table
--split-by id
--chunks 500
```
Source table will be split by `id` column into 500 chunks but only at most 60 connections 
(15 executors x 4 cores) will be used to import the data. This allows us to import large 
tables without overloading underlying data store with large volume of connections. This 
configuration is also useful when distribution of data by `split-by` column is not uniform 
and there are "gaps" for large ranges of values. Delta's auto compaction and optimization 
features are enabled via spark configuration to make sure that storage of imported data is 
optimized - avoid small files and skewed file sizes. 

## Use JDBCImport in your project to specify custom transformations that will be applied during import

```scala
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import io.delta.connectors.spark.jdbc._
  
  implicit val spark: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .getOrCreate()

 // All additional possible jdbc connector properties described here -
 // https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-reference-configuration-properties.html
  
  val jdbcUrl = "jdbc:mysql://hostName:port/database"

  val config = ImportConfig(
    source = "table",
    destination = "target_database.table",
    splitBy = "id",
    chunks = 10)

  // define a transform to convert all timestamp columns to strings
  val timeStampsToStrings : DataFrame => DataFrame = source => {
    val tsCols = source.schema.fields.filter(_.dataType == DataTypes.TimestampType).map(_.name)
     tsCols.foldLeft(source)((df, colName) =>
       df.withColumn(colName, from_unixtime(unix_timestamp(col(colName)), "yyyy-MM-dd HH:mm:ss.S")))
}

  // Whatever functions are passed to below transform will be applied during import
  val transforms = new DataTransforms(Seq(
      df => df.withColumn("id", col("id").cast(types.StringType)), // cast id column to string
      timeStampsToStrings // use transform defined above for timestamp conversion
    ))

  new JDBCImport(jdbcUrl = jdbcUrl, importConfig = config, dataTransform = transforms)
    .run()
```
