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
from pyspark.sql import SQLContext, functions, Row, SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

current_path = os.path.abspath(os.path.dirname(__file__))
sys.path.insert(1, current_path.replace("test", "main"))

from tables import DeltaTable


class PySparkTestCase(unittest.TestCase):

    def setUp(self):
        self._old_sys_path = list(sys.path)
        class_name = self.__class__.__name__
        self.sc = SparkContext('local[4]', class_name)
        logger = self.sc._jvm.org.apache.log4j
        logger.LogManager.getLogger("org"). setLevel( logger.Level.ERROR )
        logger.LogManager.getLogger("akka").setLevel( logger.Level.ERROR )

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

    def test_delete_without_condition(self):
        self.__writeDeltaTable(
            [('Ankit', 25), ('Jalfaizy', 22), ('saurabh', 20), ('Bala', 26)], ["key", "val"])
        deltatable = DeltaTable.forPath(self.tempFile, self.spark)

        deltatable.delete()
        self.__checkAnswer(deltatable.toDF(), [])

    def test_delete_with_str_condition(self):
        self.__writeDeltaTable(
            [('Ankit', 25), ('Jalfaizy', 22), ('saurabh', 20), ('Bala', 26)], ["key", "val"])
        deltatable = DeltaTable.forPath(self.tempFile, self.spark)

        deltatable.delete("key = 'Ankit'")
        self.__checkAnswer(deltatable.toDF(), [('Jalfaizy', 22), ('saurabh', 20), ('Bala', 26)])

    def test_delete_with_column_condition(self):
        self.__writeDeltaTable(
            [('Ankit', 25), ('Jalfaizy', 22), ('saurabh', 20), ('Bala', 26)], ["key", "val"])
        deltatable = DeltaTable.forPath(self.tempFile, self.spark)

        deltatable.delete(functions.expr("key = 'Jalfaizy'"))
        self.__checkAnswer(deltatable.toDF(), [('Ankit', 25), ('saurabh', 20), ('Bala', 26)])

    def test_update_col_map(self):
        self.__writeDeltaTable(
            [('Ankit', 25), ('Jalfaizy', 22), ('saurabh', 20), ('Bala', 26)], ["key", "val"])
        deltatable = DeltaTable.forPath(self.tempFile, self.spark)

        deltatable.update({"val": functions.expr("1")})
        self.__checkAnswer(
            deltatable.toDF(), [('Ankit', 1), ('Jalfaizy', 1), ('saurabh', 1), ('Bala', 1)])

    def test_update_col_map_with_condition(self):
        self.__writeDeltaTable(
            [('Ankit', 25), ('Jalfaizy', 22), ('saurabh', 20), ('Bala', 26)], ["key", "val"])
        deltatable = DeltaTable.forPath(self.tempFile, self.spark)

        deltatable.update(
            {"val": functions.expr("1")}, functions.expr("key = 'Ankit' or key = 'Jalfaizy'"))
        self.__checkAnswer(
            deltatable.toDF(), [('Ankit', 1), ('Jalfaizy', 1), ('saurabh', 20), ('Bala', 26)])

    def test_update_str_map(self):
        self.__writeDeltaTable(
            [('Ankit', 25), ('Jalfaizy', 22), ('saurabh', 20), ('Bala', 26)], ["key", "val"])
        deltatable = DeltaTable.forPath(self.tempFile, self.spark)

        deltatable.update({"val": "1"})
        self.__checkAnswer(
            deltatable.toDF(), [('Ankit', 1), ('Jalfaizy', 1), ('saurabh', 1), ('Bala', 1)])

    def test_update_str_map_with_condition(self):
        self.__writeDeltaTable(
            [('Ankit', 25), ('Jalfaizy', 22), ('saurabh', 20), ('Bala', 26)], ["key", "val"])
        deltatable = DeltaTable.forPath(self.tempFile, self.spark)

        deltatable.update({"val": "1"}, "key = 'Ankit' or key = 'Jalfaizy'")
        self.__checkAnswer(
            deltatable.toDF(), [('Ankit', 1), ('Jalfaizy', 1), ('saurabh', 20), ('Bala', 26)])

    def test_basic_merge(self):
        self.__writeDeltaTable(
            [('Ankit', 25), ('Jalfaizy', 22), ('saurabh', 20), ('Bala', 26)], ["key", "val"])
        deltatable = DeltaTable.forPath(self.tempFile, self.spark)
        source = self.spark.createDataFrame(
            [('Ankit', 52), ('Jalfaizy', 22), ('newperson', 20), ('Bala', 62)], ["Col1", "Col2"])

        deltatable.merge(source, "key = Col1") \
            .whenMatchedUpdate({"val": "Col2"}) \
            .whenNotMatchedThenInsert({"key": "Col1", "val": "Col2"}).execute()
        self.__checkAnswer(
            deltatable.toDF(),
            [('Ankit', 52), ('Jalfaizy', 22), ('newperson', 20), ('saurabh', 20), ('Bala', 62)])

    def test_extended_merge(self):
        self.__writeDeltaTable(
            [('Ankit', 25), ('Jalfaizy', 22), ('saurabh', 20), ('Bala', 26)], ["key", "val"])
        deltatable = DeltaTable.forPath(self.tempFile, self.spark)
        source = self.spark.createDataFrame(
            [('Ankit', 52), ('Jalfaizy', 22), ('newperson', 20), ('Bala', 62)], ["Col1", "Col2"])

        deltatable.merge(source, "key = Col1") \
            .whenMatchedThenDelete("key = 'Jalfaizy'") \
            .whenMatchedUpdate({"val": "Col2"}, "key = 'Ankit'") \
            .whenNotMatchedThenInsert({"key": "Col1", "val": "Col2"}, "Col1 = 'newperson'") \
            .execute()
        self.__checkAnswer(
            deltatable.toDF(),
            [('Ankit', 52), ('newperson', 20), ('saurabh', 20), ('Bala', 26)])

    def test_extended_merge_with_column(self):
        self.__writeDeltaTable(
            [('Ankit', 25), ('Jalfaizy', 22), ('saurabh', 20), ('Bala', 26)], ["key", "val"])
        deltatable = DeltaTable.forPath(self.tempFile, self.spark)
        source = self.spark.createDataFrame(
            [('Ankit', 52), ('Jalfaizy', 22), ('newperson', 20), ('Bala', 62)], ["Col1", "Col2"])

        deltatable.merge(source, functions.expr("key = Col1")) \
            .whenMatchedThenDelete(functions.expr("key = 'Jalfaizy'")) \
            .whenMatchedUpdate({"val": functions.expr("Col2")}, functions.expr("key = 'Ankit'")) \
            .whenNotMatchedThenInsert(
                {"key": functions.expr("Col1"), "val": functions.expr("Col2")},
                functions.expr("Col1 = 'newperson'")) \
            .execute()
        self.__checkAnswer(
            deltatable.toDF(),
            [('Ankit', 52), ('newperson', 20), ('saurabh', 20), ('Bala', 26)])


if __name__ == "__main__":
    from test_deltatable import *

    try:
        import xmlrunner
        testRunner = xmlrunner.XMLTestRunner(output='target/test-reports', verbosity=4)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=4)
