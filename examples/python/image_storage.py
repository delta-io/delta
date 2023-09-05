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

# This example shows
# 1 - How to load the TensorFlow Flowers Images into a dataframe
# 2 - Manipulate the dataframe
# 3 - Write the dataframe to a Delta Lake table
# 4 - Read the new Delta Lake table

import pyspark.sql.functions as fn
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
import shutil
from urllib import request
import os

# To run this example directly, set up the spark session using the following 2 commands
# You will need to run using Python3
# You will also need to install the python packages pyspark and delta-spark, we advise using pip
builder = (
  SparkSession.builder
    .appName('image_storage')
    .config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension')
    .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog')
)

# This is only for testing staged release artifacts. Ignore this completely.
if os.getenv('EXTRA_MAVEN_REPO'):
    builder = builder.config("spark.jars.repositories", os.getenv('EXTRA_MAVEN_REPO'))

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# Flowers dataset from the TensorFlow team - https://www.tensorflow.org/datasets/catalog/tf_flowers
imageGzipUrl = "https://storage.googleapis.com/download.tensorflow.org/example_images/flower_photos.tgz"
imageGzipPath = "/tmp/flower_photos.tgz"
imagePath = "/tmp/image-folder"
deltaPath = "/tmp/delta-table"

# Clear previous run's zipper file, image folder and delta tables
if os.path.exists(imageGzipPath):
  os.remove(imageGzipPath)
shutil.rmtree(imagePath, ignore_errors=True)
shutil.rmtree(deltaPath, ignore_errors=True)

request.urlretrieve(imageGzipUrl, imageGzipPath)
shutil.unpack_archive(imageGzipPath, imagePath)

# read the images from the flowers dataset
images = spark.read.format("binaryFile").\
  option("recursiveFileLookup", "true").\
  option("pathGlobFilter", "*.jpg").\
  load(imagePath)

# Knowing the file path, extract the flower type and filename using substring_index
# Remember, Spark dataframes are immutable, here we are just reusing the images dataframe
images = images.withColumn("flowerType_filename", fn.substring_index(images.path, "/", -2))
images = images.withColumn("flowerType", fn.substring_index(images.flowerType_filename, "/", 1))
images = images.withColumn("filename", fn.substring_index(images.flowerType_filename, "/", -1))
images = images.drop("flowerType_filename")
images.show()

# Select the columns we want to write out to
df = images.select("path", "content", "flowerType", "filename").repartition(4)
df.show()

# Write out the delta table to the given path, this will overwrite any table that is currently there
df.write.format("delta").mode("overwrite").save(deltaPath)

# Reads the delta table that was just written
dfDelta = spark.read.format("delta").load(deltaPath)
dfDelta.show()

# Cleanup
if os.path.exists(imageGzipPath):
  os.remove(imageGzipPath)
shutil.rmtree(imagePath)
shutil.rmtree(deltaPath)
