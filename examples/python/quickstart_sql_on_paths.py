
from pyspark.sql import SparkSession
import shutil

table_dir = "/tmp/delta-table"
# Clear any previous runs
shutil.rmtree(table_dir, ignore_errors=True)

# Enable SQL/DML commands and Metastore tables for the current spark session.
# We need to set the following configs

spark = SparkSession.builder \
    .appName("quickstart_sql_on_paths") \
    .master("local[*]") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Clear any previous runs
spark.sql("DROP TABLE IF EXISTS newData")

try:
    # Create a table
    print("############# Creating a table ###############")
    spark.sql("CREATE TABLE delta.`%s`(id LONG) USING delta" % table_dir)
    spark.sql("INSERT INTO delta.`%s` VALUES 0, 1, 2, 3, 4" % table_dir)

    # Read the table
    print("############ Reading the table ###############")
    spark.sql("SELECT * FROM delta.`%s`" % table_dir).show()

    # Upsert (merge) new data
    print("########### Upsert new data #############")
    spark.sql("CREATE TABLE newData(id LONG) USING parquet")
    spark.sql("INSERT INTO newData VALUES 3, 4, 5, 6")

    spark.sql('''MERGE INTO delta.`{0}` AS data USING newData
            ON data.id = newData.id
            WHEN MATCHED THEN
              UPDATE SET data.id = newData.id
            WHEN NOT MATCHED THEN INSERT *
        '''.format(table_dir))

    spark.sql("SELECT * FROM delta.`%s`" % table_dir).show()

    # Update table data
    print("########## Overwrite the table ###########")
    spark.sql("INSERT OVERWRITE delta.`%s` select * FROM (VALUES 5, 6, 7, 8, 9) x (id)" % table_dir)
    spark.sql("SELECT * FROM delta.`%s`" % table_dir).show()

    # Update every even value by adding 100 to it
    print("########### Update to the table(add 100 to every even value) ##############")
    spark.sql("UPDATE delta.`{0}` SET id = (id + 100) WHERE (id % 2 == 0)".format(table_dir))
    spark.sql("SELECT * FROM delta.`%s`" % table_dir).show()

    # Delete every even value
    print("######### Delete every even value ##############")
    spark.sql("DELETE FROM delta.`{0}` WHERE (id % 2 == 0)".format(table_dir))
    spark.sql("SELECT * FROM delta.`%s`" % table_dir).show()
finally:
    # cleanup
    spark.sql("DROP TABLE IF EXISTS newData")
    spark.stop()
