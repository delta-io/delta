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
import shutil

path = "/tmp/delta-table/missing_logstore_jar"

try:
    # Clear any previous runs
    shutil.rmtree(path, ignore_errors=True)

    spark = SparkSession.builder \
        .appName("missing logstore jar") \
        .master("local[*]") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

    spark.range(0, 5).write.format("delta").save(path)

except Exception as e:
    assert "Please ensure that the delta-storage dependency is included." in str(e)
    print("SUCCESS - error was thrown, as expected")

else:
    assert False, "The write to the delta table should have thrown without the delta-storage JAR."

finally:
    # cleanup
    shutil.rmtree(path, ignore_errors=True)
