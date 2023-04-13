#
# Copyright (2021) The Delta Lake Project Authors.
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
from typing import List, Any

from pyspark.sql import DataFrame

from delta.testing.utils import DeltaTestCase


class DeltaSqlTests(DeltaTestCase):

    def setUp(self) -> None:
        super(DeltaSqlTests, self).setUp()
        # Create a simple Delta table inside the temp directory to test SQL commands.
        df = self.spark.createDataFrame([('a', 1), ('b', 2), ('c', 3)], ["key", "value"])
        df.write.format("delta").save(self.tempFile)
        df.write.mode("overwrite").format("delta").save(self.tempFile)

    def test_vacuum(self) -> None:
        self.spark.sql("set spark.databricks.delta.retentionDurationCheck.enabled = false")
        try:
            deleted_files = self.spark.sql("VACUUM '%s' RETAIN 0 HOURS" % self.tempFile).collect()
            # Verify `VACUUM` did delete some data files
            self.assertTrue(self.tempFile in deleted_files[0][0])
        finally:
            self.spark.sql("set spark.databricks.delta.retentionDurationCheck.enabled = true")

    def test_describe_history(self) -> None:
        self.assertGreater(
            len(self.spark.sql("desc history delta.`%s`" % (self.tempFile)).collect()), 0)

    def test_generate(self) -> None:
        # create a delta table
        temp_path = tempfile.mkdtemp()
        temp_file = os.path.join(temp_path, "delta_sql_test_table")
        numFiles = 10
        self.spark.range(100).repartition(numFiles).write.format("delta").save(temp_file)

        # Generate the symlink format manifest
        self.spark.sql("GENERATE SYMLINK_FORMAT_MANIFEST FOR TABLE delta.`{}`"
                       .format(temp_file))

        # check the contents of the manifest
        # NOTE: this is not a correctness test, we are testing correctness in the scala suite
        manifestPath = os.path.join(temp_file,
                                    os.path.join("_symlink_format_manifest", "manifest"))
        files = []
        with open(manifestPath) as f:
            files = f.readlines()

        shutil.rmtree(temp_path)
        # the number of files we write should equal the number of lines in the manifest
        self.assertEqual(len(files), numFiles)

    def test_convert(self) -> None:
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

    def test_ddls(self) -> None:
        table = "deltaTable"
        table2 = "deltaTable2"
        with self.table(table, table2):
            def read_table() -> DataFrame:
                return self.spark.sql(f"SELECT * FROM {table}")

            self.spark.sql(f"DROP TABLE IF EXISTS {table}")
            self.spark.sql(f"DROP TABLE IF EXISTS {table2}")

            self.spark.sql(f"CREATE TABLE {table}(a LONG, b String NOT NULL) USING delta")
            self.assertEqual(read_table().count(), 0)

            answer = [("a", "bigint"), ("b", "string"), ("", ""), ("# Partitioning", ""),
                      ("Not partitioned", "")]
            self.__checkAnswer(
                self.spark.sql(f"DESCRIBE TABLE {table}").select("col_name", "data_type"),
                answer,
                schema=["col_name", "data_type"])

            self.spark.sql(f"ALTER TABLE {table} CHANGE COLUMN a a LONG AFTER b")
            self.assertSequenceEqual(["b", "a"], [f.name for f in read_table().schema.fields])

            self.spark.sql(f"ALTER TABLE {table} ALTER COLUMN b DROP NOT NULL")
            self.assertIn(True, [f.nullable for f in read_table().schema.fields if f.name == "b"])

            self.spark.sql(f"ALTER TABLE {table} ADD COLUMNS (x LONG)")
            self.assertIn("x", [f.name for f in read_table().schema.fields])

            self.spark.sql(f"ALTER TABLE {table} SET TBLPROPERTIES ('k' = 'v')")
            self.__checkAnswer(self.spark.sql(f"SHOW TBLPROPERTIES {table}"),
                               [('k', 'v'),
                                ('delta.minReaderVersion', '1'),
                                ('delta.minWriterVersion', '2')])

            self.spark.sql(f"ALTER TABLE {table} UNSET TBLPROPERTIES ('k')")
            self.__checkAnswer(self.spark.sql(f"SHOW TBLPROPERTIES {table}"),
                               [('delta.minReaderVersion', '1'),
                                ('delta.minWriterVersion', '2')])

            self.spark.sql(f"ALTER TABLE {table} RENAME TO {table2}")
            self.assertEqual(self.spark.sql(f"SELECT * FROM {table2}").count(), 0)

            test_dir = os.path.join(tempfile.mkdtemp(), table2)
            self.spark.createDataFrame([("", 0, 0)], ["b", "a", "x"]) \
                .write.format("delta").save(test_dir)

            self.spark.sql(f"ALTER TABLE {table2} SET LOCATION '{test_dir}'")
            self.assertEqual(self.spark.sql(f"SELECT * FROM {table2}").count(), 1)

    def __checkAnswer(self, df: DataFrame,
                      expectedAnswer: List[Any],
                      schema: List[str] = ["key", "value"]) -> None:
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
