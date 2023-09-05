#
# Copyright (2021) The Delta Lake Project Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr
from delta.tables import DeltaTable
import shutil


path = "/tmp/delta-change-data-feed/student"
otherPath = "/tmp/delta-change-data-feed/student_source"

# Enable SQL commands and Update/Delete/Merge for the current spark session.
# we need to set the following configs
spark = SparkSession.builder \
    .appName("Change Data Feed") \
    .master("local[*]") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()


def cleanup():
    shutil.rmtree(path, ignore_errors=True)
    shutil.rmtree(otherPath, ignore_errors=True)
    spark.sql("DROP TABLE IF EXISTS student")
    spark.sql("DROP TABLE IF EXISTS student_source")


def read_cdc_by_table_name(starting_version):
    return spark.read.format("delta") \
        .option("readChangeFeed", "true") \
        .option("startingVersion", str(starting_version)) \
        .table("student") \
        .orderBy("_change_type", "id")


def stream_cdc_by_table_name(starting_version):
    return spark.readStream.format("delta") \
        .option("readChangeFeed", "true") \
        .option("startingVersion", str(starting_version)) \
        .table("student") \
        .writeStream \
        .format("console") \
        .option("numRows", 1000) \
        .start()


cleanup()

try:
    # =============== Create student table ===============
    spark.sql('''CREATE TABLE student (id INT, name STRING, age INT)
                 USING DELTA
                 PARTITIONED BY (age)
                 TBLPROPERTIES (delta.enableChangeDataFeed = true)
                 LOCATION '{0}'
             '''.format(path))

    spark.range(0, 10) \
        .selectExpr(
            "CAST(id as INT) as id",
            "CAST(id as STRING) as name",
            "CAST(id % 4 + 18 as INT) as age") \
        .write.format("delta").mode("append").save(path)  # v1

    # =============== Show table data + changes ===============

    print("(v1) Initial Table")
    spark.read.format("delta").load(path).orderBy("id").show()

    print("(v1) CDC changes")
    read_cdc_by_table_name(1).show()

    table = DeltaTable.forPath(spark, path)

    # =============== Perform UPDATE ===============

    print("(v2) Updated id -> id + 1")
    table.update(set={"id": expr("id + 1")})  # v2
    read_cdc_by_table_name(2).show()

    # =============== Perform DELETE ===============

    print("(v3) Deleted where id >= 7")
    table.delete(condition=expr("id >= 7"))  # v3
    read_cdc_by_table_name(3).show()

    # =============== Perform partition DELETE ===============

    print("(v4) Deleted where age = 18")
    table.delete(condition=expr("age = 18"))  # v4, partition delete
    read_cdc_by_table_name(4).show()

    # =============== Create source table for MERGE ===============

    spark.sql('''CREATE TABLE student_source (id INT, name STRING, age INT)
                 USING DELTA
                 LOCATION '{0}'
             '''.format(otherPath))
    spark.range(0, 3) \
        .selectExpr(
            "CAST(id as INT) as id",
            "CAST(id as STRING) as name",
            "CAST(id % 4 + 18 as INT) as age") \
        .write.format("delta").mode("append").saveAsTable("student_source")
    source = spark.sql("SELECT * FROM student_source")

    # =============== Perform MERGE ===============

    table.alias("target") \
        .merge(
            source.alias("source"),
            "target.id = source.id")\
        .whenMatchedUpdate(set={"id": "source.id", "age": "source.age + 10"}) \
        .whenNotMatchedInsertAll() \
        .execute() # v5
    print("(v5) Merged with a source table")
    read_cdc_by_table_name(5).show()

    # =============== Stream changes ===============

    print("Streaming by table name")
    cdfStream = stream_cdc_by_table_name(0)
    cdfStream.awaitTermination(10)
    cdfStream.stop()

finally:
    cleanup()
    spark.stop()
