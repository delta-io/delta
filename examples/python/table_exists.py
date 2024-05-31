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
from pyspark.sql.utils import AnalysisException
import shutil

def exists(spark, filepath):
    """Checks if a delta table exists at `filepath`"""
    try:
        spark.read.load(path=filepath, format="delta")
    except AnalysisException as exception:
        if "is not a Delta table" in exception.getMessage() or "Path does not exist" in exception.getMessage():
            return False
        raise exception
    return True

spark = SparkSession.builder \
    .appName("table_exists") \
    .master("local[*]") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

filepath = "/tmp/table_exists"

# Clear any previous runs
shutil.rmtree(filepath, ignore_errors=True)

# Verify table doesn't exist yet
print(f"Verifying table does not exist at {filepath}")
assert not exists(spark, filepath)

# Create a delta table at filepath
print(f"Creating delta table at {filepath}")
data = spark.range(0, 5)
data.write.format("delta").save(filepath)

# Verify table now exists
print(f"Verifying table exists at {filepath}")
assert exists(spark, filepath)

# Clean up
shutil.rmtree(filepath)
