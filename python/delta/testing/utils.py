#
# Copyright (2020) The Delta Lake Project Authors.
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

import os
import shutil
import sys
import tempfile
import unittest

from pyspark.sql import SparkSession


class DeltaTestCase(unittest.TestCase):
    """Test class base that sets up a correctly configured SparkSession for querying Delta tables.
    """

    def setUp(self):
        self._old_sys_path = list(sys.path)
        class_name = self.__class__.__name__
        self.warehouse_dir = tempfile.mkdtemp()
        # Configurations to speed up tests and reduce memory footprint
        self.spark = SparkSession.builder \
            .appName(class_name) \
            .master('local[4]') \
            .config("spark.ui.enabled", "false") \
            .config("spark.databricks.delta.snapshotPartitions", "2") \
            .config("spark.sql.shuffle.partitions", "5") \
            .config("delta.log.cacheSize", "3") \
            .config("spark.sql.sources.parallelPartitionDiscovery.parallelism", "5") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog",
                    "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config("spark.sql.warehouse.dir", self.warehouse_dir) \
            .getOrCreate()
        self.sc = self.spark.sparkContext
        self.tempPath = tempfile.mkdtemp()
        self.tempFile = os.path.join(self.tempPath, "tempFile")

    def tearDown(self):
        self.sc.stop()
        shutil.rmtree(self.tempPath)
        if os.path.exists(self.warehouse_dir) and os.path.isdir(self.warehouse_dir):
            shutil.rmtree(self.warehouse_dir)
        sys.path = self._old_sys_path
