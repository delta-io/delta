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

tableName = "tbltestpython"

# Enable SQL/DML commands and Metastore tables for the current spark session.
# We need to set the following configs

spark = SparkSession.builder \
    .appName("quickstart_sql") \
    .master("local[*]") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Clear any previous runs
spark.sql("DROP TABLE IF EXISTS " + tableName)
spark.sql("DROP TABLE IF EXISTS newData")

try:
    # Create a table
    print("############# Creating a table ###############")
    spark.sql("CREATE TABLE %s(id LONG) USING delta" % tableName)
    spark.sql("INSERT INTO %s VALUES 0, 1, 2, 3, 4" % tableName)

    # Read the table
    print("############ Reading the table ###############")
    spark.sql("SELECT * FROM %s" % tableName).show()

    # Upsert (merge) new data
    print("########### Upsert new data #############")
    spark.sql("CREATE TABLE newData(id LONG) USING parquet")
    spark.sql("INSERT INTO newData VALUES 3, 4, 5, 6")

    spark.sql('''MERGE INTO {0} USING newData
            ON {0}.id = newData.id
            WHEN MATCHED THEN
              UPDATE SET {0}.id = newData.id
            WHEN NOT MATCHED THEN INSERT *
        '''.format(tableName))

    spark.sql("SELECT * FROM %s" % tableName).show()

    # Update table data
    print("########## Overwrite the table ###########")
    spark.sql("INSERT OVERWRITE %s select * FROM (VALUES 5, 6, 7, 8, 9) x (id)" % tableName)
    spark.sql("SELECT * FROM %s" % tableName).show()

    # Update every even value by adding 100 to it
    print("########### Update to the table(add 100 to every even value) ##############")
    spark.sql("UPDATE {0} SET id = (id + 100) WHERE (id % 2 == 0)".format(tableName))
    spark.sql("SELECT * FROM %s" % tableName).show()

    # Delete every even value
    print("######### Delete every even value ##############")
    spark.sql("DELETE FROM {0} WHERE (id % 2 == 0)".format(tableName))
    spark.sql("SELECT * FROM %s" % tableName).show()

    # Read old version of data using time travel
    print("######## Read old data using time travel ############")
    df = spark.read.format("delta").option("versionAsOf", 0).table(tableName)
    df.show()
finally:
    # cleanup
    spark.sql("DROP TABLE " + tableName)
    spark.sql("DROP TABLE IF EXISTS newData")
    spark.stop()
