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


import glob
import os
import struct
import sys
import unittest
import tempfile
import shutil
import logging

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql import functions
from pyspark.sql import Row
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

current_path = os.path.abspath(os.path.dirname(__file__))
sys.path.insert(1, current_path.replace("test", "main"))

from tables import DeltaTable


class PySparkTestCase(unittest.TestCase):

    def setUp(self):
        self._old_sys_path = list(sys.path)
        class_name = self.__class__.__name__
        self.sc = SparkContext('local[4]', class_name)

    def tearDown(self):
        self.sc.stop()
        sys.path = self._old_sys_path


class DeltaTableTests(PySparkTestCase):
    def setUp(self):
        super(DeltaTableTests, self).setUp()
        self.sqlContext = SQLContext(self.sc)
        self.spark = SparkSession(self.sc)
        self.tempPath = tempfile.mkdtemp()
        self.tempFile = self.tempPath + "/tempFile"

    def tearDown(self):
        self.spark.stop()
        shutil.rmtree(self.tempPath)
        super(DeltaTableTests, self).tearDown()

    def __checkAnswer(self, df, expectedAnswer):
        if not expectedAnswer:
            self.assertEqual(df.count(), 0)
            return
        expectedDF = self.spark.createDataFrame(expectedAnswer, ["key", "val"])
        self.assertEqual(df.count(), expectedDF.count())
        self.assertEqual(len(df.columns), len(expectedDF.columns))
        self.assertEqual([], df.subtract(expectedDF).take(1))
        self.assertEqual([], expectedDF.subtract(df).take(1))

    def __writeDeltaTable(self, datalist, schema):
        df = self.spark.createDataFrame(datalist, schema)
        df.write.format("delta").save(self.tempFile)

    def test_forPath_without_session(self):
        self.__writeDeltaTable(
            [('Ankit', 25), ('Jalfaizy', 22), ('saurabh', 20), ('Bala', 26)], ["key", "val"])
        self.__checkAnswer(
            DeltaTable.forPath(self.tempFile).toDF(),
            [('Ankit', 25), ('Jalfaizy', 22), ('saurabh', 20), ('Bala', 26)])

    def test_forPath_with_session(self):
        self.__writeDeltaTable(
            [('Ankit', 25), ('Jalfaizy', 22), ('saurabh', 20), ('Bala', 26)], ["key", "val"])
        self.__checkAnswer(
            DeltaTable.forPath(self.tempFile, self.spark).toDF(),
            [('Ankit', 25), ('Jalfaizy', 22), ('saurabh', 20), ('Bala', 26)])


if __name__ == "__main__":
    from test_deltatable import *

    try:
        import xmlrunner
        testRunner = xmlrunner.XMLTestRunner(output='target/test-reports', verbosity=4)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=4)
