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
import tempfile
import shutil
import os

from pyspark.sql import SQLContext, functions, Row, SparkSession

from delta.tables import DeltaTable
from delta.testing.utils import PySparkTestCase


class DeltaTableTests(PySparkTestCase):

    def setUp(self):
        super(DeltaTableTests, self).setUp()
        self.sqlContext = SQLContext(self.sc)
        self.spark = SparkSession(self.sc)
        self.tempPath = tempfile.mkdtemp()
        self.tempFile = os.path.join(self.tempPath, "tempFile")

    def tearDown(self):
        self.spark.stop()
        shutil.rmtree(self.tempPath)
        super(DeltaTableTests, self).tearDown()

    def test_forPath(self):
        self.__writeDeltaTable([('a', 1), ('b', 2), ('c', 3)])
        dt = DeltaTable.forPath(self.spark, self.tempFile).toDF()
        self.__checkAnswer(dt, [('a', 1), ('b', 2), ('c', 3)])

    def test_alias_and_toDF(self):
        self.__writeDeltaTable([('a', 1), ('b', 2), ('c', 3)])
        dt = DeltaTable.forPath(self.spark, self.tempFile).toDF()
        self.__checkAnswer(
            dt.alias("myTable").select('myTable.key', 'myTable.value'),
            [('a', 1), ('b', 2), ('c', 3)])

    def __checkAnswer(self, df, expectedAnswer, schema=["key", "value"]):
        if not expectedAnswer:
            self.assertEqual(df.count(), 0)
            return
        expectedDF = self.spark.createDataFrame(expectedAnswer, schema)
        self.assertEqual(df.count(), expectedDF.count())
        self.assertEqual(len(df.columns), len(expectedDF.columns))
        self.assertEqual([], df.subtract(expectedDF).take(1))
        self.assertEqual([], expectedDF.subtract(df).take(1))

    def __writeDeltaTable(self, datalist):
        df = self.spark.createDataFrame(datalist, ["key", "value"])
        df.write.format("delta").save(self.tempFile)

if __name__ == "__main__":
    try:
        import xmlrunner
        testRunner = xmlrunner.XMLTestRunner(output='target/test-reports', verbosity=4)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=4)
