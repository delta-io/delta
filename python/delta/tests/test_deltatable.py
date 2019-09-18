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
from pyspark.sql.types import *

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

    def test_history(self):
        self.__writeDeltaTable([('a', 1), ('b', 2), ('c', 3)])
        self.__overwriteDeltaTable([('a', 3), ('b', 2), ('c', 1)])
        dt = DeltaTable.forPath(self.spark, self.tempFile)
        operations = dt.history().select('operation')
        self.__checkAnswer(operations,
                           [Row("WRITE"), Row("WRITE")],
                           StructType([StructField(
                               "operation", StringType(), True)]))

        lastMode = dt.history(1).select('operationParameters.mode')
        self.__checkAnswer(
            lastMode,
            [Row("Overwrite")],
            StructType([StructField("operationParameters.mode", StringType(), True)]))

    def test_vacuum(self):
        self.__writeDeltaTable([('a', 1), ('b', 2), ('c', 3)])
        dt = DeltaTable.forPath(self.spark, self.tempFile)
        self.__createFile('abc.txt', 'abcde')
        self.__createFile('bac.txt', 'abcdf')
        self.assertEqual(True, self.__checkFileExists('abc.txt'))
        dt.vacuum()  # will not delete files as default retention is used.

        self.assertEqual(True, self.__checkFileExists('bac.txt'))
        retentionConf = "spark.databricks.delta.retentionDurationCheck.enabled"
        self.spark.conf.set(retentionConf, "false")
        dt.vacuum(0.0)
        self.spark.conf.set(retentionConf, "true")
        self.assertEqual(False, self.__checkFileExists('bac.txt'))
        self.assertEqual(False, self.__checkFileExists('abc.txt'))

    def test_convertToDelta(self):
        df = self.spark.createDataFrame([('a', 1), ('b', 2), ('c', 3)], ["key", "value"])
        df.write.format("parquet").save(self.tempFile)
        self.tempFile2 = self.tempFile + "_"
        dt = DeltaTable.convertToDelta(self.spark, "parquet.`" + self.tempFile + "`")
        self.__checkAnswer(
            self.spark.read.format("delta").load(self.tempFile),
            [('a', 1), ('b', 2), ('c', 3)])

        # test if convert to delta with partition columns work
        df.write.partitionBy("value").format("parquet").save(self.tempFile2)
        schema = StructType()
        schema.add("value", IntegerType(), True)
        dt = DeltaTable.convertToDelta(
            self.spark,
            "parquet.`" + self.tempFile2 + "`",
            schema)
        self.__checkAnswer(
            self.spark.read.format("delta").load(self.tempFile2),
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

    def __overwriteDeltaTable(self, datalist):
        df = self.spark.createDataFrame(datalist, ["key", "value"])
        df.write.format("delta").mode("overwrite").save(self.tempFile)

    def __createFile(self, fileName, content):
        with open(os.path.join(self.tempFile, fileName), 'w') as f:
            f.write(content)

    def __checkFileExists(self, fileName):
        return os.path.exists(os.path.join(self.tempFile, fileName))


if __name__ == "__main__":
    try:
        import xmlrunner
        testRunner = xmlrunner.XMLTestRunner(output='target/test-reports', verbosity=4)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=4)
