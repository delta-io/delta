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


class DeltaSqlTests(PySparkTestCase):

    def setUp(self):
        super(DeltaSqlTests, self).setUp()
        spark = SparkSession(self.sc)
        if self.sc.version < "3.":
            # Manually activate "DeltaSparkSessionExtension" in PySpark 2.4 in a cloned session
            # because "spark.sql.extensions" is not picked up. (See SPARK-25003).
            self.sc._jvm.io.delta.sql.DeltaSparkSessionExtension() \
                .apply(spark._jsparkSession.extensions())
            self.spark = SparkSession(self.sc, spark._jsparkSession.cloneSession())
        else:
            self.spark = spark
        self.temp_path = tempfile.mkdtemp()
        self.temp_file = os.path.join(self.temp_path, "delta_sql_test_table")
        # Create a simple Delta table inside the temp directory to test SQL commands.
        df = self.spark.createDataFrame([('a', 1), ('b', 2), ('c', 3)], ["key", "value"])
        df.write.format("delta").save(self.temp_file)
        df.write.mode("overwrite").format("delta").save(self.temp_file)

    def tearDown(self):
        self.spark.stop()
        shutil.rmtree(self.temp_path)
        super(DeltaSqlTests, self).tearDown()

    def test_vacuum(self):
        self.spark.sql("set spark.databricks.delta.retentionDurationCheck.enabled = false")
        try:
            deleted_files = self.spark.sql("VACUUM '%s' RETAIN 0 HOURS" % self.temp_file).collect()
            # Verify `VACUUM` did delete some data files
            self.assertTrue(self.temp_file in deleted_files[0][0])
        finally:
            self.spark.sql("set spark.databricks.delta.retentionDurationCheck.enabled = true")

    def test_describe_history(self):
        assert(len(self.spark.sql("desc history delta.`%s`" % (self.temp_file)).collect()) > 0)

    def test_convert(self):
        df = self.spark.createDataFrame([('a', 1), ('b', 2), ('c', 3)], ["key", "value"])
        temp_path2 = tempfile.mkdtemp()
        temp_path3 = tempfile.mkdtemp()
        temp_file2 = os.path.join(temp_path2, "delta_sql_test2")
        temp_file3 = os.path.join(temp_path3, "delta_sql_test3")

        df.write.format("parquet").save(temp_file2)
        self.spark.sql("CONVERT TO DELTA parquet.`" + temp_file2 + "`")
        self.__checkAnswer(
            self.spark.read.format("delta").load(temp_file2),
            [('a', 1), ('b', 2), ('c', 3)])

        # test if convert to delta with partition columns work
        df.write.partitionBy("value").format("parquet").save(temp_file3)
        self.spark.sql("CONVERT TO DELTA parquet.`" + temp_file3 + "` PARTITIONED BY (value INT)")
        self.__checkAnswer(
            self.spark.read.format("delta").load(temp_file3),
            [('a', 1), ('b', 2), ('c', 3)])

        shutil.rmtree(temp_path2)
        shutil.rmtree(temp_path3)

    def __checkAnswer(self, df, expectedAnswer, schema=["key", "value"]):
        if not expectedAnswer:
            self.assertEqual(df.count(), 0)
            return
        expectedDF = self.spark.createDataFrame(expectedAnswer, schema)
        self.assertEqual(df.count(), expectedDF.count())
        self.assertEqual(len(df.columns), len(expectedDF.columns))
        self.assertEqual([], df.subtract(expectedDF).take(1))
        self.assertEqual([], expectedDF.subtract(df).take(1))

if __name__ == "__main__":
    try:
        import xmlrunner
        testRunner = xmlrunner.XMLTestRunner(output='target/test-reports', verbosity=4)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=4)
