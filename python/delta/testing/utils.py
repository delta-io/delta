#
# Copyright (2023) The Delta Lake Project Authors.
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

from pyspark import SparkConf
from pyspark.testing.sqlutils import ReusedSQLTestCase  # type: ignore[import]


class DeltaTestCase(ReusedSQLTestCase):
    """Test class base that sets up a correctly configured SparkSession for querying Delta tables.
    """

    @classmethod
    def conf(cls) -> SparkConf:
        _conf = super(DeltaTestCase, cls).conf()
        _conf.set("spark.app.name", cls.__name__)
        _conf.set("spark.master", "local[4]")
        _conf.set("spark.ui.enabled", "false")
        _conf.set("spark.databricks.delta.snapshotPartitions", "2")
        _conf.set("spark.sql.shuffle.partitions", "5")
        _conf.set("delta.log.cacheSize", "3")
        _conf.set("spark.sql.sources.parallelPartitionDiscovery.parallelism", "5")
        _conf.set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        _conf.set("spark.sql.catalog.spark_catalog",
                  "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        return _conf

    def setUp(self) -> None:
        super(DeltaTestCase, self).setUp()
        self.tempPath = tempfile.mkdtemp()
        self.tempFile = os.path.join(self.tempPath, "tempFile")

    def tearDown(self) -> None:
        super(DeltaTestCase, self).tearDown()
        shutil.rmtree(self.tempPath)
