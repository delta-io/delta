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
import pprint
import unittest

from pyspark import SparkConf
from pyspark.testing.connectutils import ReusedConnectTestCase


class DeltaTestCase(ReusedConnectTestCase):
    """
    Test suite base for setting up a properly configured SparkSession for using Delta Connect.
    """

    @classmethod
    def setUpClass(cls):
        print("OS before setUpClass: ")
        pprint.pprint(dict(os.environ), width = 1)
        if "MASTER" in os.environ:
            del os.environ["MASTER"]
        super(DeltaTestCase, self).setUpClass()
        print("OS after setUpClass: ")
        pprint.pprint(dict(os.environ), width = 1)

    @classmethod
    def conf(cls) -> SparkConf:
        _conf = super(DeltaTestCase, cls).conf()
        print("after super: " + _conf.toDebugString())
        print("OS after super: ")
        pprint.pprint(dict(os.environ), width = 1)
        _conf.set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        _conf.set("spark.sql.catalog.spark_catalog",
                  "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        _conf.set("spark.connect.extensions.relation.classes",
                  "org.apache.spark.sql.connect.delta.DeltaRelationPlugin")
        _conf.set("spark.connect.extensions.command.classes",
                  "org.apache.spark.sql.connect.delta.DeltaCommandPlugin")
        print("after everything: " + _conf.toDebugString())
        print("OS after everything: ")
        pprint.pprint(dict(os.environ), width = 1)
        return _conf

    def setUp(self) -> None:
        unittest.TestCase.setUp(self)
        self.tempPath = tempfile.mkdtemp()
        self.tempFile = os.path.join(self.tempPath, "tempFile")

    def tearDown(self) -> None:
        super(DeltaTestCase, self).tearDown()
        shutil.rmtree(self.tempPath)
