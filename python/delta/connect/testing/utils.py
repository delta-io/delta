#
# Copyright (2024) The Delta Lake Project Authors.
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

import tempfile
import shutil
import os
import unittest

from pyspark import SparkConf
from pyspark.sql import SparkSession


class DeltaTestCase(unittest.TestCase):
    """
    Test suite base for setting up a properly configured SparkSession for using Delta Connect.
    """
    
    @classmethod
    def setUpClass(cls):
        remote = os.environ.get("SPARK_CONNECT_TESTING_REMOTE", "local[4]")
        cls.spark = (
            SparkSession.builder
            .remote(remote)
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", 
                   "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.connect.extensions.relation.classes",
                   "org.apache.spark.sql.connect.delta.DeltaRelationPlugin")
            .config("spark.connect.extensions.command.classes",
                   "org.apache.spark.sql.connect.delta.DeltaCommandPlugin")
            .getOrCreate()
        )
    
    @classmethod
    def tearDownClass(cls):
        if hasattr(cls, "spark") and cls.spark is not None:
            cls.spark.stop()
            cls.spark = None

    def setUp(self) -> None:
        self.tempPath = tempfile.mkdtemp()
        self.tempFile = os.path.join(self.tempPath, "tempFile")

    def tearDown(self) -> None:
        shutil.rmtree(self.tempPath)
