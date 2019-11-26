#
# Copyright 2019 Databricks, Inc.
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

import unittest

from pyspark import SparkContext, SparkConf
import sys


class PySparkTestCase(unittest.TestCase):

    def setUp(self):
        self._old_sys_path = list(sys.path)
        class_name = self.__class__.__name__
        # Configurations to speed up tests and reduce memory footprint
        conf = SparkConf() \
            .setAppName(class_name) \
            .setMaster('local[4]') \
            .set("spark.ui.enabled", "false") \
            .set("spark.databricks.delta.snapshotPartitions", "2") \
            .set("spark.sql.shuffle.partitions", "5") \
            .set("delta.log.cacheSize", "3") \
            .set("spark.sql.sources.parallelPartitionDiscovery.parallelism", "5")
        # Enable Delta's SQL syntax for Spark 3.0+. Older versions require a hack to
        # enable it. See "DeltaSqlTests.setUp" for details.
        conf.set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        self.sc = SparkContext(conf=conf)

    def tearDown(self):
        self.sc.stop()
        sys.path = self._old_sys_path
