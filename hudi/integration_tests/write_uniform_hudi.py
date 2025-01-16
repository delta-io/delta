from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date, current_timestamp
from pyspark.testing import assertDataFrameEqual
from delta.tables import DeltaTable
import shutil
import random
import os
import time

###################### Setup ######################

test_root = "/tmp/delta-uniform-hudi/"
warehouse_path = test_root + "uniform_tables"
shutil.rmtree(test_root, ignore_errors=True)
hudi_table_base_name = "delta_table_with_hudi"

# we need to set the following configs
spark_delta = SparkSession.builder \
    .appName("delta-uniform-hudi-writer") \
    .master("local[*]") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.sql.warehouse.dir", warehouse_path) \
    .getOrCreate()

###################### Helper functions ######################

def get_delta_df(spark, table_name):
    hudi_table_path = os.path.join(warehouse_path, table_name)
    print('hudi_table_path:', hudi_table_path)
    df_delta = spark.read.format("delta").load(hudi_table_path)
    return df_delta

def get_hudi_df(spark, table_name):
    hudi_table_path = os.path.join(warehouse_path, table_name)
    df_hudi = (spark.read.format("hudi")
               .option("hoodie.metadata.enable", "true")
               .option("hoodie.datasource.write.hive_style_partitioning", "true")
               .load(hudi_table_path))
    return df_hudi

###################### Create tables in Delta ######################
print('Delta tables:')

# validate various data types
spark_delta.sql(f"""CREATE TABLE `{hudi_table_base_name}_0` (col1 BIGINT, col2 BOOLEAN, col3 DATE,
    col4 DOUBLE, col5 FLOAT, col6 INT, col7 STRING, col8 TIMESTAMP,
    col9 BINARY, col10 DECIMAL(5, 2), 
    col11 STRUCT<field1: INT, field2: STRING,
    field3: STRUCT<field4: INT, field5: INT, field6: STRING>>,
    col12 ARRAY<STRUCT<field1: INT, field2: STRING>>,
    col13 MAP<STRING, STRUCT<field1: INT, field2: STRING>>) USING DELTA
    TBLPROPERTIES('delta.universalFormat.enabledFormats' = 'hudi') """)
spark_delta.sql(f"""INSERT INTO `{hudi_table_base_name}_0` VALUES 
    (123, true, date(current_timestamp()), 32.1, 1.23, 456, 'hello world', 
    current_timestamp(), X'1ABF', -999.99,
    STRUCT(1, 'hello', STRUCT(2, 3, 'world')),
    ARRAY(
        STRUCT(1, 'first'),
        STRUCT(2, 'second')
    ),
    MAP(
        'key1', STRUCT(1, 'delta'),
        'key2', STRUCT(1, 'lake')
    )); """)

df_delta_0 = get_delta_df(spark_delta, f"{hudi_table_base_name}_0")
df_delta_0.show()

# conversion happens correctly when enabling property after table creation
spark_delta.sql(f"CREATE TABLE {hudi_table_base_name}_1 (col1 INT, col2 STRING) USING DELTA")
spark_delta.sql(f"INSERT INTO {hudi_table_base_name}_1 VALUES (1, 'a'), (2, 'b')")
spark_delta.sql(f"ALTER TABLE {hudi_table_base_name}_1 SET TBLPROPERTIES('delta.universalFormat.enabledFormats' = 'hudi')")

df_delta_1 = get_delta_df(spark_delta, f"{hudi_table_base_name}_1")
df_delta_1.show()

# validate deletes
spark_delta.sql(f"""CREATE TABLE {hudi_table_base_name}_2 (col1 INT, col2 STRING) USING DELTA
                TBLPROPERTIES('delta.universalFormat.enabledFormats' = 'hudi')""")
spark_delta.sql(f"INSERT INTO {hudi_table_base_name}_2 VALUES (1, 'a'), (2, 'b')")
spark_delta.sql(f"DELETE FROM {hudi_table_base_name}_2 WHERE col1 = 1")

df_delta_2 = get_delta_df(spark_delta, f"{hudi_table_base_name}_2")
df_delta_2.show()

# basic schema evolution
spark_delta.sql(f"""CREATE TABLE {hudi_table_base_name}_3 (col1 INT, col2 STRING) USING DELTA
                TBLPROPERTIES('delta.universalFormat.enabledFormats' = 'hudi')""")
spark_delta.sql(f"INSERT INTO {hudi_table_base_name}_3 VALUES (1, 'a'), (2, 'b')")
spark_delta.sql(f"ALTER TABLE {hudi_table_base_name}_3 ADD COLUMN col3 INT FIRST")
spark_delta.sql(f"INSERT INTO {hudi_table_base_name}_3 VALUES (3, 4, 'c')")

df_delta_3 = get_delta_df(spark_delta, f"{hudi_table_base_name}_3")
df_delta_3.show()

# schema evolution for nested fields
spark_delta.sql(f"""CREATE TABLE {hudi_table_base_name}_4 (col1 STRUCT<field1: INT, field2: STRING>) 
                USING DELTA
                TBLPROPERTIES('delta.universalFormat.enabledFormats' = 'hudi')""")
spark_delta.sql(f"""INSERT INTO {hudi_table_base_name}_4 VALUES 
                    (named_struct('field1', 1, 'field2', 'hello'))
                    """)
spark_delta.sql(f"ALTER TABLE {hudi_table_base_name}_4 ADD COLUMN col1.field3 INT AFTER field1")
spark_delta.sql(f"INSERT INTO {hudi_table_base_name}_4 VALUES (named_struct('field1', 3, 'field3', 4, 'field2', 'delta'))")

df_delta_4 = get_delta_df(spark_delta, f"{hudi_table_base_name}_4")
df_delta_4.show()

# time travel
spark_delta.sql(f"""CREATE TABLE {hudi_table_base_name}_5 (col1 INT, col2 STRING) USING DELTA
                TBLPROPERTIES('delta.universalFormat.enabledFormats' = 'hudi',
                              'delta.columnMapping.mode' = 'name')""")
spark_delta.sql(f"INSERT INTO {hudi_table_base_name}_5 VALUES (1, 'a')")
spark_delta.sql(f"INSERT INTO {hudi_table_base_name}_5 VALUES (2, 'b')")

df_history_5 = spark_delta.sql(f"DESCRIBE HISTORY {hudi_table_base_name}_5")
timestamp = df_history_5.collect()[0]['timestamp'] # get the timestamp of the first commit
df_delta_5 = spark_delta.sql(f"""
    SELECT * FROM {hudi_table_base_name}_5
    TIMESTAMP AS OF '{timestamp}'""")
df_delta_5.show()

time.sleep(5)

###################### Read tables from Hudi engine ######################
print('Hudi tables:')

spark_hudi = SparkSession.builder \
    .appName("delta-uniform-hudi-reader") \
    .master("local[*]") \
    .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog") \
    .config("spark.sql.warehouse.dir", warehouse_path) \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.kryo.registrator", "org.apache.spark.HoodieSparkKryoRegistrar") \
    .getOrCreate()

df_hudi_0 = get_hudi_df(spark_hudi, f"{hudi_table_base_name}_0")
df_hudi_0.show()
assertDataFrameEqual(df_delta_0, df_hudi_0)

df_hudi_1 = get_hudi_df(spark_hudi, f"{hudi_table_base_name}_1")
df_hudi_1.show()
assertDataFrameEqual(df_delta_1, df_hudi_1)

df_hudi_2 = get_hudi_df(spark_hudi, f"{hudi_table_base_name}_2")
df_hudi_2.show()
assertDataFrameEqual(df_delta_2, df_hudi_2)

df_hudi_3 = get_hudi_df(spark_hudi, f"{hudi_table_base_name}_3")
df_hudi_3.show()
assertDataFrameEqual(df_delta_3, df_hudi_3)

df_hudi_4 = get_hudi_df(spark_hudi, f"{hudi_table_base_name}_4")
df_hudi_4.show()
assertDataFrameEqual(df_delta_4, df_hudi_4)

df_hudi_5 = spark_hudi.sql(f"""
    SELECT * FROM {hudi_table_base_name}_5
    TIMESTAMP AS OF '{timestamp}'""")
df_hudi_5.show()
assertDataFrameEqual(df_delta_5, df_hudi_5)

print('UniForm Hudi integration test passed!')
