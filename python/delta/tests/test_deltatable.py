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

from pyspark.sql import SQLContext, Row, SparkSession
from pyspark.sql.functions import *
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

    def test_delete(self):
        self.__writeDeltaTable([('a', 1), ('b', 2), ('c', 3), ('d', 4)])
        dt = DeltaTable.forPath(self.spark, self.tempFile)

        # delete with condition as str
        dt.delete("key = 'a'")
        self.__checkAnswer(dt.toDF(), [('b', 2), ('c', 3), ('d', 4)])

        # delete with condition as Column
        dt.delete(col("key") == lit("b"))
        self.__checkAnswer(dt.toDF(), [('c', 3), ('d', 4)])

        # delete without condition
        dt.delete()
        self.__checkAnswer(dt.toDF(), [])

        # bad args
        with self.assertRaises(TypeError):
            dt.delete(condition=1)

    def test_update(self):
        self.__writeDeltaTable([('a', 1), ('b', 2), ('c', 3), ('d', 4)])
        dt = DeltaTable.forPath(self.spark, self.tempFile)

        # positional args: update with condition as str and with set exprs as str
        dt.update("key = 'a' or key = 'b'", {"value": "1"})
        self.__checkAnswer(dt.toDF(), [('a', 1), ('b', 1), ('c', 3), ('d', 4)])

        # positional args: update with condition as Column and with set exprs as Columns
        dt.update(expr("key = 'a' or key = 'b'"), {"value": expr("0")})
        self.__checkAnswer(dt.toDF(), [('a', 0), ('b', 0), ('c', 3), ('d', 4)])

        # positional args: update without condition and with set exprs as strs
        dt.update({"value": "50"})
        self.__checkAnswer(dt.toDF(), [('a', 50), ('b', 50), ('c', 50), ('d', 50)])

        # positional args: update without condition and with set exprs as Columns
        dt.update({"value": expr("100")})
        self.__checkAnswer(dt.toDF(), [('a', 100), ('b', 100), ('c', 100), ('d', 100)])

        # keyword args: update without condition
        dt.update(set={"value": "200"})
        self.__checkAnswer(dt.toDF(), [('a', 200), ('b', 200), ('c', 200), ('d', 200)])

        # keyword args: update without condition
        dt.update(condition=None, set={"value": "300"})
        self.__checkAnswer(dt.toDF(), [('a', 300), ('b', 300), ('c', 300), ('d', 300)])

        # keyword args: update with condition
        dt.update(set={"value": "400"}, condition="key = 'a'")
        self.__checkAnswer(dt.toDF(), [('a', 400), ('b', 300), ('c', 300), ('d', 300)])

        # bad args
        with self.assertRaises(TypeError):
            dt.update(set=1)
        with self.assertRaises(ValueError):
            dt.update(condition='a', set=None)
        with self.assertRaises(ValueError):
            dt.update(condition='a')  # set = None by default
        with self.assertRaises(TypeError):
            dt.update(1, {})

    def test_merge(self):
        self.__writeDeltaTable([('a', 1), ('b', 2), ('c', 3), ('d', 4)])
        dt = DeltaTable.forPath(self.spark, self.tempFile)

        # positional args: string expressions in all conditions and dicts
        source = self.spark.createDataFrame([('a', -1), ('b', 0), ('e', -5), ('f', -6)], ["k", "v"])
        dt.merge(source, "key = k") \
            .whenMatchedUpdate("k = 'a'", {"value": "v + 0"}) \
            .whenMatchedDelete() \
            .whenNotMatchedInsert("k = 'e'", {"key": "k", "value": "v + 0"}) \
            .execute()
        self.__checkAnswer(dt.toDF(), ([('a', -1), ('c', 3), ('d', 4), ('e', -5)]))

        # positional args: string expressions in all conditions and insertAll/updateAll + aliases
        self.__overwriteDeltaTable([('a', 1), ('b', 2), ('c', 3), ('d', 4)])
        dt.alias("t").merge(source.toDF("key", "value").alias("s"), "s.key = t.key") \
            .whenMatchedUpdateAll("s.key = 'a'") \
            .whenNotMatchedInsertAll("s.key = 'e'") \
            .execute()
        self.__checkAnswer(dt.toDF(), ([('a', -1), ('b', 2), ('c', 3), ('d', 4), ('e', -5)]))

        # positional args: Column expressions in all conditions and dicts
        self.__overwriteDeltaTable([('a', 1), ('b', 2), ('c', 3), ('d', 4)])
        dt.merge(source, expr("key = k")) \
            .whenMatchedUpdate(expr("k = 'a'"), {"value": col("v") + 0}) \
            .whenMatchedDelete() \
            .whenNotMatchedInsert(expr("k = 'e'"), {"key": "k", "value": col("v") + 0}) \
            .execute()
        self.__checkAnswer(dt.toDF(), ([('a', -1), ('c', 3), ('d', 4), ('e', -5)]))

        # positional args: Column expressions in all conditions and insertAll/updateAll + aliases
        self.__overwriteDeltaTable([('a', 1), ('b', 2), ('c', 3), ('d', 4)])
        dt.alias("t").merge(
                source.toDF("key", "value").alias("s"),
                expr("t.key = s.key")) \
            .whenMatchedUpdateAll(expr("s.key = 'a'")) \
            .whenNotMatchedInsertAll(expr("s.key = 'e'")) \
            .execute()
        self.__checkAnswer(dt.toDF(), ([('a', -1), ('b', 2), ('c', 3), ('d', 4), ('e', -5)]))

        # positional args: no clause conditions and dicts
        self.__overwriteDeltaTable([('a', 1), ('b', 2), ('c', 3), ('d', 4)])
        source = self.spark.createDataFrame([('a', -1), ('e', -5)], ["k", "v"])
        dt.merge(source, "key = k") \
            .whenMatchedUpdate({"value": "v + 0"}) \
            .whenNotMatchedInsert({"key": "k", "value": "v + 0"}) \
            .execute()
        self.__checkAnswer(dt.toDF(), ([('a', -1), ('b', 2), ('c', 3), ('d', 4), ('e', -5)]))

        # positional args: no clause conditions and insertAll/updateAll
        self.__overwriteDeltaTable([('a', 1), ('b', 2), ('c', 3), ('d', 4)])
        source = self.spark.createDataFrame([('a', -1), ('e', -5)], ["key", "value"])
        dt.alias("t").merge(
                source.alias("s"),
                expr("t.key = s.key")) \
            .whenMatchedUpdateAll() \
            .whenNotMatchedInsertAll() \
            .execute()
        self.__checkAnswer(dt.toDF(), ([('a', -1), ('b', 2), ('c', 3), ('d', 4), ('e', -5)]))


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
