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

# type: ignore[union-attr]

import unittest
import os
from typing import List, Set, Dict, Optional, Any, Callable, Union, Tuple

from pyspark.sql import DataFrame, Row
from pyspark.sql.column import _to_seq  # type: ignore[attr-defined]
from pyspark.sql.functions import col, lit, expr, floor
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, DataType
from pyspark.sql.utils import AnalysisException, ParseException

from delta.tables import DeltaTable, DeltaTableBuilder, DeltaOptimizeBuilder
from delta.testing.utils import DeltaTestCase


class DeltaTableTestsMixin:

    def test_forPath(self) -> None:
        self.__writeDeltaTable([('a', 1), ('b', 2), ('c', 3)])
        dt = DeltaTable.forPath(self.spark, self.tempFile).toDF()
        self.__checkAnswer(dt, [('a', 1), ('b', 2), ('c', 3)])

    def test_forPathWithOptions(self) -> None:
        path = self.tempFile
        fsOptions = {"fs.fake.impl": "org.apache.spark.sql.delta.FakeFileSystem",
                     "fs.fake.impl.disable.cache": "true"}
        self.__writeDeltaTable([('a', 1), ('b', 2), ('c', 3)])
        dt = DeltaTable.forPath(self.spark, path, fsOptions).toDF()
        self.__checkAnswer(dt, [('a', 1), ('b', 2), ('c', 3)])

    def test_forName(self) -> None:
        with self.table("test"):
            self.__writeAsTable([('a', 1), ('b', 2), ('c', 3)], "test")
            df = DeltaTable.forName(self.spark, "test").toDF()
            self.__checkAnswer(df, [('a', 1), ('b', 2), ('c', 3)])

    def test_alias_and_toDF(self) -> None:
        self.__writeDeltaTable([('a', 1), ('b', 2), ('c', 3)])
        dt = DeltaTable.forPath(self.spark, self.tempFile).toDF()
        self.__checkAnswer(
            dt.alias("myTable").select('myTable.key', 'myTable.value'),
            [('a', 1), ('b', 2), ('c', 3)])

    def test_delete(self) -> None:
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
            dt.delete(condition=1)  # type: ignore[arg-type]

    def test_generate(self) -> None:
        # create a delta table
        numFiles = 10
        self.spark.range(100).repartition(numFiles).write.format("delta").save(self.tempFile)
        dt = DeltaTable.forPath(self.spark, self.tempFile)

        # Generate the symlink format manifest
        dt.generate("symlink_format_manifest")

        # check the contents of the manifest
        # NOTE: this is not a correctness test, we are testing correctness in the scala suite
        manifestPath = os.path.join(self.tempFile,
                                    os.path.join("_symlink_format_manifest", "manifest"))
        files = []
        with open(manifestPath) as f:
            files = f.readlines()

        # the number of files we write should equal the number of lines in the manifest
        self.assertEqual(len(files), numFiles)

    def test_update(self) -> None:
        self.__writeDeltaTable([('a', 1), ('b', 2), ('c', 3), ('d', 4)])
        dt = DeltaTable.forPath(self.spark, self.tempFile)

        # update with condition as str and with set exprs as str
        dt.update("key = 'a' or key = 'b'", {"value": "1"})
        self.__checkAnswer(dt.toDF(), [('a', 1), ('b', 1), ('c', 3), ('d', 4)])

        # update with condition as Column and with set exprs as Columns
        dt.update(expr("key = 'a' or key = 'b'"), {"value": expr("0")})
        self.__checkAnswer(dt.toDF(), [('a', 0), ('b', 0), ('c', 3), ('d', 4)])

        # update without condition
        dt.update(set={"value": "200"})
        self.__checkAnswer(dt.toDF(), [('a', 200), ('b', 200), ('c', 200), ('d', 200)])

        # bad args
        with self.assertRaisesRegex(ValueError, "cannot be None"):
            dt.update({"value": "200"})  # type: ignore[call-overload]

        with self.assertRaisesRegex(ValueError, "cannot be None"):
            dt.update(condition='a')  # type: ignore[call-overload]

        with self.assertRaisesRegex(TypeError, "must be a dict"):
            dt.update(set=1)  # type: ignore[call-overload]

        with self.assertRaisesRegex(TypeError, "must be a Spark SQL Column or a string"):
            dt.update(1, {})  # type: ignore[call-overload]

        with self.assertRaisesRegex(TypeError, "Values of dict in .* must contain only"):
            dt.update(set={"value": 1})  # type: ignore[dict-item]

        with self.assertRaisesRegex(TypeError, "Keys of dict in .* must contain only"):
            dt.update(set={1: ""})  # type: ignore[dict-item]

        with self.assertRaises(TypeError):
            dt.update(set=1)  # type: ignore[call-overload]

    def test_merge(self) -> None:
        self.__writeDeltaTable([('a', 1), ('b', 2), ('c', 3), ('d', 4)])
        source = self.spark.createDataFrame([('a', -1), ('b', 0), ('e', -5), ('f', -6)], ["k", "v"])

        def reset_table() -> None:
            self.__overwriteDeltaTable([('a', 1), ('b', 2), ('c', 3), ('d', 4)])

        dt = DeltaTable.forPath(self.spark, self.tempFile)

        # ============== Test basic syntax ==============

        # String expressions in merge condition and dicts
        reset_table()
        dt.merge(source, "key = k") \
            .whenMatchedUpdate(set={"value": "v + 0"}) \
            .whenNotMatchedInsert(values={"key": "k", "value": "v + 0"}) \
            .whenNotMatchedBySourceUpdate(set={"value": "value + 0"}) \
            .execute()
        self.__checkAnswer(dt.toDF(),
                           ([('a', -1), ('b', 0), ('c', 3), ('d', 4), ('e', -5), ('f', -6)]))

        # Column expressions in merge condition and dicts
        reset_table()
        dt.merge(source, expr("key = k")) \
            .whenMatchedUpdate(set={"value": col("v") + 0}) \
            .whenNotMatchedInsert(values={"key": "k", "value": col("v") + 0}) \
            .whenNotMatchedBySourceUpdate(set={"value": col("value") + 0}) \
            .execute()
        self.__checkAnswer(dt.toDF(),
                           ([('a', -1), ('b', 0), ('c', 3), ('d', 4), ('e', -5), ('f', -6)]))

        # Multiple matched update clauses
        reset_table()
        dt.merge(source, expr("key = k")) \
            .whenMatchedUpdate(condition="key = 'a'", set={"value": "5"}) \
            .whenMatchedUpdate(set={"value": "0"}) \
            .execute()
        self.__checkAnswer(dt.toDF(), ([('a', 5), ('b', 0), ('c', 3), ('d', 4)]))

        # Multiple matched delete clauses
        reset_table()
        dt.merge(source, expr("key = k")) \
            .whenMatchedDelete(condition="key = 'a'") \
            .whenMatchedDelete() \
            .execute()
        self.__checkAnswer(dt.toDF(), ([('c', 3), ('d', 4)]))

        # Redundant matched update and delete clauses
        reset_table()
        dt.merge(source, expr("key = k")) \
            .whenMatchedUpdate(condition="key = 'a'", set={"value": "5"}) \
            .whenMatchedUpdate(condition="key = 'a'", set={"value": "0"}) \
            .whenMatchedUpdate(condition="key = 'b'", set={"value": "6"}) \
            .whenMatchedDelete(condition="key = 'b'") \
            .execute()
        self.__checkAnswer(dt.toDF(), ([('a', 5), ('b', 6), ('c', 3), ('d', 4)]))

        # Interleaved matched update and delete clauses
        reset_table()
        dt.merge(source, expr("key = k")) \
            .whenMatchedDelete(condition="key = 'a'") \
            .whenMatchedUpdate(condition="key = 'a'", set={"value": "5"}) \
            .whenMatchedDelete(condition="key = 'b'") \
            .whenMatchedUpdate(set={"value": "6"}) \
            .execute()
        self.__checkAnswer(dt.toDF(), ([('c', 3), ('d', 4)]))

        # Multiple not matched insert clauses
        reset_table()
        dt.alias("t")\
            .merge(source.toDF("key", "value").alias("s"), expr("t.key = s.key")) \
            .whenNotMatchedInsert(condition="s.key = 'e'",
                                  values={"t.key": "s.key", "t.value": "5"}) \
            .whenNotMatchedInsertAll() \
            .execute()
        self.__checkAnswer(dt.toDF(),
                           ([('a', 1), ('b', 2), ('c', 3), ('d', 4), ('e', 5), ('f', -6)]))

        # Redundant not matched update and delete clauses
        reset_table()
        dt.merge(source, expr("key = k")) \
            .whenNotMatchedInsert(condition="k = 'e'", values={"key": "k", "value": "5"}) \
            .whenNotMatchedInsert(condition="k = 'e'", values={"key": "k", "value": "6"}) \
            .whenNotMatchedInsert(condition="k = 'f'", values={"key": "k", "value": "7"}) \
            .whenNotMatchedInsert(condition="k = 'f'", values={"key": "k", "value": "8"}) \
            .execute()
        self.__checkAnswer(dt.toDF(),
                           ([('a', 1), ('b', 2), ('c', 3), ('d', 4), ('e', 5), ('f', 7)]))

        # Multiple not matched by source update clauses
        reset_table()
        dt.merge(source, expr("key = k")) \
            .whenNotMatchedBySourceUpdate(condition="key = 'c'", set={"value": "5"}) \
            .whenNotMatchedBySourceUpdate(set={"value": "0"}) \
            .execute()
        self.__checkAnswer(dt.toDF(), ([('a', 1), ('b', 2), ('c', 5), ('d', 0)]))

        # Multiple not matched by source delete clauses
        reset_table()
        dt.merge(source, expr("key = k")) \
            .whenNotMatchedBySourceDelete(condition="key = 'c'") \
            .whenNotMatchedBySourceDelete() \
            .execute()
        self.__checkAnswer(dt.toDF(), ([('a', 1), ('b', 2)]))

        # Redundant not matched by source update and delete clauses
        reset_table()
        dt.merge(source, expr("key = k")) \
            .whenNotMatchedBySourceUpdate(condition="key = 'c'", set={"value": "5"}) \
            .whenNotMatchedBySourceUpdate(condition="key = 'c'", set={"value": "0"}) \
            .whenNotMatchedBySourceUpdate(condition="key = 'd'", set={"value": "6"}) \
            .whenNotMatchedBySourceDelete(condition="key = 'd'") \
            .execute()
        self.__checkAnswer(dt.toDF(), ([('a', 1), ('b', 2), ('c', 5), ('d', 6)]))

        # Interleaved update and delete clauses
        reset_table()
        dt.merge(source, expr("key = k")) \
            .whenNotMatchedBySourceDelete(condition="key = 'c'") \
            .whenNotMatchedBySourceUpdate(condition="key = 'c'", set={"value": "5"}) \
            .whenNotMatchedBySourceDelete(condition="key = 'd'") \
            .whenNotMatchedBySourceUpdate(set={"value": "6"}) \
            .execute()
        self.__checkAnswer(dt.toDF(), ([('a', 1), ('b', 2)]))

        # ============== Test clause conditions ==============

        # String expressions in all conditions and dicts
        reset_table()
        dt.merge(source, "key = k") \
            .whenMatchedUpdate(condition="k = 'a'", set={"value": "v + 0"}) \
            .whenMatchedDelete(condition="k = 'b'") \
            .whenNotMatchedInsert(condition="k = 'e'", values={"key": "k", "value": "v + 0"}) \
            .whenNotMatchedBySourceUpdate(condition="key = 'c'", set={"value": col("value") + 0}) \
            .whenNotMatchedBySourceDelete(condition="key = 'd'") \
            .execute()
        self.__checkAnswer(dt.toDF(), ([('a', -1), ('c', 3), ('e', -5)]))

        # Column expressions in all conditions and dicts
        reset_table()
        dt.merge(source, expr("key = k")) \
            .whenMatchedUpdate(
                condition=expr("k = 'a'"),
                set={"value": col("v") + 0}) \
            .whenMatchedDelete(condition=expr("k = 'b'")) \
            .whenNotMatchedInsert(
                condition=expr("k = 'e'"),
                values={"key": "k", "value": col("v") + 0}) \
            .whenNotMatchedBySourceUpdate(
                condition=expr("key = 'c'"),
                set={"value": col("value") + 0}) \
            .whenNotMatchedBySourceDelete(condition=expr("key = 'd'")) \
            .execute()
        self.__checkAnswer(dt.toDF(), ([('a', -1), ('c', 3), ('e', -5)]))

        # Positional arguments
        reset_table()
        dt.merge(source, "key = k") \
            .whenMatchedUpdate("k = 'a'", {"value": "v + 0"}) \
            .whenMatchedDelete("k = 'b'") \
            .whenNotMatchedInsert("k = 'e'", {"key": "k", "value": "v + 0"}) \
            .whenNotMatchedBySourceUpdate("key = 'c'", {"value": "value + 0"}) \
            .whenNotMatchedBySourceDelete("key = 'd'") \
            .execute()
        self.__checkAnswer(dt.toDF(), ([('a', -1), ('c', 3), ('e', -5)]))

        # ============== Test updateAll/insertAll ==============

        # No clause conditions and insertAll/updateAll + aliases
        reset_table()
        dt.alias("t") \
            .merge(source.toDF("key", "value").alias("s"), expr("t.key = s.key")) \
            .whenMatchedUpdateAll() \
            .whenNotMatchedInsertAll() \
            .execute()
        self.__checkAnswer(dt.toDF(),
                           ([('a', -1), ('b', 0), ('c', 3), ('d', 4), ('e', -5), ('f', -6)]))

        # String expressions in all clause conditions and insertAll/updateAll + aliases
        reset_table()
        dt.alias("t") \
            .merge(source.toDF("key", "value").alias("s"), "s.key = t.key") \
            .whenMatchedUpdateAll("s.key = 'a'") \
            .whenNotMatchedInsertAll("s.key = 'e'") \
            .execute()
        self.__checkAnswer(dt.toDF(), ([('a', -1), ('b', 2), ('c', 3), ('d', 4), ('e', -5)]))

        # Column expressions in all clause conditions and insertAll/updateAll + aliases
        reset_table()
        dt.alias("t") \
            .merge(source.toDF("key", "value").alias("s"), expr("t.key = s.key")) \
            .whenMatchedUpdateAll(expr("s.key = 'a'")) \
            .whenNotMatchedInsertAll(expr("s.key = 'e'")) \
            .execute()
        self.__checkAnswer(dt.toDF(), ([('a', -1), ('b', 2), ('c', 3), ('d', 4), ('e', -5)]))

        # ============== Test bad args ==============
        # ---- bad args in merge()
        with self.assertRaisesRegex(TypeError, "must be DataFrame"):
            dt.merge(1, "key = k")  # type: ignore[arg-type]

        with self.assertRaisesRegex(TypeError, "must be a Spark SQL Column or a string"):
            dt.merge(source, 1)  # type: ignore[arg-type]

        # ---- bad args in whenMatchedUpdate()
        with self.assertRaisesRegex(ValueError, "cannot be None"):
            (dt  # type: ignore[call-overload]
                .merge(source, "key = k")
                .whenMatchedUpdate({"value": "v"}))

        with self.assertRaisesRegex(ValueError, "cannot be None"):
            (dt  # type: ignore[call-overload]
                .merge(source, "key = k")
                .whenMatchedUpdate(1))

        with self.assertRaisesRegex(ValueError, "cannot be None"):
            (dt  # type: ignore[call-overload]
                .merge(source, "key = k")
                .whenMatchedUpdate(condition="key = 'a'"))

        with self.assertRaisesRegex(TypeError, "must be a Spark SQL Column or a string"):
            (dt  # type: ignore[call-overload]
                .merge(source, "key = k")
                .whenMatchedUpdate(1, {"value": "v"}))

        with self.assertRaisesRegex(TypeError, "must be a dict"):
            (dt  # type: ignore[call-overload]
                .merge(source, "key = k")
                .whenMatchedUpdate("k = 'a'", 1))

        with self.assertRaisesRegex(TypeError, "Values of dict in .* must contain only"):
            (dt
                .merge(source, "key = k")
                .whenMatchedUpdate(set={"value": 1}))  # type: ignore[dict-item]

        with self.assertRaisesRegex(TypeError, "Keys of dict in .* must contain only"):
            (dt
                .merge(source, "key = k")
                .whenMatchedUpdate(set={1: ""}))  # type: ignore[dict-item]

        with self.assertRaises(TypeError):
            (dt  # type: ignore[call-overload]
                .merge(source, "key = k")
                .whenMatchedUpdate(set="k = 'a'", condition={"value": 1}))

        # bad args in whenMatchedDelete()
        with self.assertRaisesRegex(TypeError, "must be a Spark SQL Column or a string"):
            dt.merge(source, "key = k").whenMatchedDelete(1)  # type: ignore[arg-type]

        # ---- bad args in whenNotMatchedInsert()
        with self.assertRaisesRegex(ValueError, "cannot be None"):
            (dt  # type: ignore[call-overload]
                .merge(source, "key = k")
                .whenNotMatchedInsert({"value": "v"}))

        with self.assertRaisesRegex(ValueError, "cannot be None"):
            dt.merge(source, "key = k").whenNotMatchedInsert(1)  # type: ignore[call-overload]

        with self.assertRaisesRegex(ValueError, "cannot be None"):
            (dt  # type: ignore[call-overload]
                .merge(source, "key = k")
                .whenNotMatchedInsert(condition="key = 'a'"))

        with self.assertRaisesRegex(TypeError, "must be a Spark SQL Column or a string"):
            (dt  # type: ignore[call-overload]
                .merge(source, "key = k")
                .whenNotMatchedInsert(1, {"value": "v"}))

        with self.assertRaisesRegex(TypeError, "must be a dict"):
            (dt  # type: ignore[call-overload]
                .merge(source, "key = k")
                .whenNotMatchedInsert("k = 'a'", 1))

        with self.assertRaisesRegex(TypeError, "Values of dict in .* must contain only"):
            (dt
                .merge(source, "key = k")
                .whenNotMatchedInsert(values={"value": 1}))  # type: ignore[dict-item]

        with self.assertRaisesRegex(TypeError, "Keys of dict in .* must contain only"):
            (dt
                .merge(source, "key = k")
                .whenNotMatchedInsert(values={1: "value"}))  # type: ignore[dict-item]

        with self.assertRaises(TypeError):
            (dt  # type: ignore[call-overload]
                .merge(source, "key = k")
                .whenNotMatchedInsert(values="k = 'a'", condition={"value": 1}))

        # ---- bad args in whenNotMatchedBySourceUpdate()
        with self.assertRaisesRegex(ValueError, "cannot be None"):
            (dt  # type: ignore[call-overload]
                .merge(source, "key = k")
                .whenNotMatchedBySourceUpdate({"value": "value"}))

        with self.assertRaisesRegex(ValueError, "cannot be None"):
            (dt  # type: ignore[call-overload]
                .merge(source, "key = k")
                .whenNotMatchedBySourceUpdate(1))

        with self.assertRaisesRegex(ValueError, "cannot be None"):
            (dt  # type: ignore[call-overload]
                .merge(source, "key = k")
                .whenNotMatchedBySourceUpdate(condition="key = 'a'"))

        with self.assertRaisesRegex(TypeError, "must be a Spark SQL Column or a string"):
            (dt  # type: ignore[call-overload]
                .merge(source, "key = k")
                .whenNotMatchedBySourceUpdate(1, {"value": "value"}))

        with self.assertRaisesRegex(TypeError, "must be a dict"):
            (dt  # type: ignore[call-overload]
                .merge(source, "key = k")
                .whenNotMatchedBySourceUpdate("key = 'a'", 1))

        with self.assertRaisesRegex(TypeError, "Values of dict in .* must contain only"):
            (dt
                .merge(source, "key = k")
                .whenNotMatchedBySourceUpdate(set={"value": 1}))  # type: ignore[dict-item]

        with self.assertRaisesRegex(TypeError, "Keys of dict in .* must contain only"):
            (dt
                .merge(source, "key = k")
                .whenNotMatchedBySourceUpdate(set={1: ""}))  # type: ignore[dict-item]

        with self.assertRaises(TypeError):
            (dt  # type: ignore[call-overload]
                .merge(source, "key = k")
                .whenNotMatchedBySourceUpdate(set="key = 'a'", condition={"value": 1}))

        # bad args in whenNotMatchedBySourceDelete()
        with self.assertRaisesRegex(TypeError, "must be a Spark SQL Column or a string"):
            dt.merge(source, "key = k").whenNotMatchedBySourceDelete(1)  # type: ignore[arg-type]

    def test_history(self) -> None:
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

    def test_detail(self) -> None:
        self.__writeDeltaTable([('a', 1), ('b', 2), ('c', 3)])
        dt = DeltaTable.forPath(self.spark, self.tempFile)
        details = dt.detail()
        self.__checkAnswer(
            details.select('format'),
            [Row('delta')],
            StructType([StructField('format', StringType(), True)])
        )

    def test_vacuum(self) -> None:
        self.__writeDeltaTable([('a', 1), ('b', 2), ('c', 3)])
        dt = DeltaTable.forPath(self.spark, self.tempFile)
        self.__createFile('abc.txt', 'abcde')
        self.__createFile('bac.txt', 'abcdf')
        self.assertEqual(True, self.__checkFileExists('abc.txt'))
        dt.vacuum()  # will not delete files as default retention is used.
        dt.vacuum(1000)  # test whether integers work

        self.assertEqual(True, self.__checkFileExists('bac.txt'))
        retentionConf = "spark.databricks.delta.retentionDurationCheck.enabled"
        self.spark.conf.set(retentionConf, "false")
        dt.vacuum(0.0)
        self.spark.conf.set(retentionConf, "true")
        self.assertEqual(False, self.__checkFileExists('bac.txt'))
        self.assertEqual(False, self.__checkFileExists('abc.txt'))

    def test_convertToDelta(self) -> None:
        df = self.spark.createDataFrame([('a', 1), ('b', 2), ('c', 3)], ["key", "value"])
        df.write.format("parquet").save(self.tempFile)
        dt = DeltaTable.convertToDelta(self.spark, "parquet.`%s`" % self.tempFile)
        self.__checkAnswer(
            self.spark.read.format("delta").load(self.tempFile),
            [('a', 1), ('b', 2), ('c', 3)])

        # test if convert to delta with partition columns work
        tempFile2 = self.tempFile + "_2"
        df.write.partitionBy("value").format("parquet").save(tempFile2)
        schema = StructType()
        schema.add("value", IntegerType(), True)
        dt = DeltaTable.convertToDelta(
            self.spark,
            "parquet.`%s`" % tempFile2,
            schema)
        self.__checkAnswer(
            self.spark.read.format("delta").load(tempFile2),
            [('a', 1), ('b', 2), ('c', 3)])
        self.assertEqual(type(dt), type(DeltaTable.forPath(self.spark, tempFile2)))

        # convert to delta with partition column provided as a string
        tempFile3 = self.tempFile + "_3"
        df.write.partitionBy("value").format("parquet").save(tempFile3)
        dt = DeltaTable.convertToDelta(
            self.spark,
            "parquet.`%s`" % tempFile3,
            "value int")
        self.__checkAnswer(
            self.spark.read.format("delta").load(tempFile3),
            [('a', 1), ('b', 2), ('c', 3)])
        self.assertEqual(type(dt), type(DeltaTable.forPath(self.spark, tempFile3)))

    def test_isDeltaTable(self) -> None:
        df = self.spark.createDataFrame([('a', 1), ('b', 2), ('c', 3)], ["key", "value"])
        df.write.format("parquet").save(self.tempFile)
        tempFile2 = self.tempFile + '_2'
        df.write.format("delta").save(tempFile2)
        self.assertEqual(DeltaTable.isDeltaTable(self.spark, self.tempFile), False)
        self.assertEqual(DeltaTable.isDeltaTable(self.spark, tempFile2), True)

    def __verify_table_schema(self, tableName: str, schema: StructType, cols: List[str],
                              types: List[DataType], nullables: Set[str] = set(),
                              comments: Dict[str, str] = {},
                              properties: Dict[str, str] = {},
                              partitioningColumns: List[str] = [],
                              tblComment: Optional[str] = None) -> None:
        fields = []
        for i in range(len(cols)):
            col = cols[i]
            dataType = types[i]
            metadata = {}
            if col in comments:
                metadata["comment"] = comments[col]
            fields.append(StructField(col, dataType, col in nullables, metadata))
        self.assertEqual(StructType(fields), schema)
        if len(properties) > 0:
            result = (
                self.spark.sql(  # type: ignore[assignment, misc]
                    "SHOW TBLPROPERTIES {}".format(tableName)
                )
                .collect())
            tablePropertyMap = {row.key: row.value for row in result}
            for key in properties:
                self.assertIn(key, tablePropertyMap)
                self.assertEqual(tablePropertyMap[key], properties[key])
        tableDetails = self.spark.sql("DESCRIBE DETAIL {}".format(tableName))\
            .collect()[0]
        self.assertEqual(tableDetails.format, "delta")
        actualComment = tableDetails.description
        self.assertEqual(actualComment, tblComment)
        partitionCols = tableDetails.partitionColumns
        self.assertEqual(sorted(partitionCols), sorted((partitioningColumns)))

    def __verify_generated_column(self, tableName: str, deltaTable: DeltaTable) -> None:
        cmd = "INSERT INTO {table} (col1, col2) VALUES (1, 11)".format(table=tableName)
        self.spark.sql(cmd)
        deltaTable.update(expr("col2 = 11"), {"col1": expr("2")})
        self.__checkAnswer(deltaTable.toDF(), [(2, 12)], schema=["col1", "col2"])

    def __build_delta_table(self, builder: DeltaTableBuilder) -> DeltaTable:
        return builder.addColumn("col1", "int", comment="foo", nullable=False) \
            .addColumn("col2", IntegerType(), generatedAlwaysAs="col1 + 10") \
            .property("foo", "bar") \
            .comment("comment") \
            .partitionedBy("col1").execute()

    def __create_table(self, ifNotExists: bool,
                       tableName: Optional[str] = None,
                       location: Optional[str] = None) -> DeltaTable:
        builder = DeltaTable.createIfNotExists(self.spark) if ifNotExists \
            else DeltaTable.create(self.spark)
        if tableName:
            builder = builder.tableName(tableName)
        if location:
            builder = builder.location(location)
        return self.__build_delta_table(builder)

    def __replace_table(self,
                        orCreate: bool,
                        tableName: Optional[str] = None,
                        location: Optional[str] = None) -> DeltaTable:
        builder = DeltaTable.createOrReplace(self.spark) if orCreate \
            else DeltaTable.replace(self.spark)
        if tableName:
            builder = builder.tableName(tableName)
        if location:
            builder = builder.location(location)
        return self.__build_delta_table(builder)

    def test_create_table_with_existing_schema(self) -> None:
        df = self.spark.createDataFrame([('a', 1), ('b', 2), ('c', 3)], ["key", "value"])
        with self.table("test"):
            deltaTable = DeltaTable.create(self.spark).tableName("test") \
                .addColumns(df.schema) \
                .addColumn("value2", dataType="int")\
                .partitionedBy(["value2", "value"])\
                .execute()
            self.__verify_table_schema("test",
                                       deltaTable.toDF().schema,
                                       ["key", "value", "value2"],
                                       [StringType(), LongType(), IntegerType()],
                                       nullables={"key", "value", "value2"},
                                       partitioningColumns=["value", "value2"])

        with self.table("test2"):
            # verify creating table with list of structFields
            deltaTable2 = DeltaTable.create(self.spark).tableName("test2").addColumns(
                df.schema.fields) \
                .addColumn("value2", dataType="int") \
                .partitionedBy("value2", "value")\
                .execute()
            self.__verify_table_schema("test2",
                                       deltaTable2.toDF().schema,
                                       ["key", "value", "value2"],
                                       [StringType(), LongType(), IntegerType()],
                                       nullables={"key", "value", "value2"},
                                       partitioningColumns=["value", "value2"])

    def test_create_replace_table_with_no_spark_session_passed(self) -> None:
        with self.table("test"):
            # create table.
            deltaTable = DeltaTable.create().tableName("test")\
                .addColumn("value", dataType="int").execute()
            self.__verify_table_schema("test",
                                       deltaTable.toDF().schema,
                                       ["value"],
                                       [IntegerType()],
                                       nullables={"value"})

            # ignore existence with createIfNotExists
            deltaTable = DeltaTable.createIfNotExists().tableName("test") \
                .addColumn("value2", dataType="int").execute()
            self.__verify_table_schema("test",
                                       deltaTable.toDF().schema,
                                       ["value"],
                                       [IntegerType()],
                                       nullables={"value"})

            # replace table with replace
            deltaTable = DeltaTable.replace().tableName("test") \
                .addColumn("key", dataType="int").execute()
            self.__verify_table_schema("test",
                                       deltaTable.toDF().schema,
                                       ["key"],
                                       [IntegerType()],
                                       nullables={"key"})

            # replace with a new column again
            deltaTable = DeltaTable.createOrReplace().tableName("test") \
                .addColumn("col1", dataType="int").execute()

            self.__verify_table_schema("test",
                                       deltaTable.toDF().schema,
                                       ["col1"],
                                       [IntegerType()],
                                       nullables={"col1"})

    def test_create_table_with_name_only(self) -> None:
        for ifNotExists in (False, True):
            tableName = "testTable{}".format(ifNotExists)
            with self.table(tableName):
                deltaTable = self.__create_table(ifNotExists, tableName=tableName)

                self.__verify_table_schema(tableName,
                                           deltaTable.toDF().schema,
                                           ["col1", "col2"],
                                           [IntegerType(), IntegerType()],
                                           nullables={"col2"},
                                           comments={"col1": "foo"},
                                           properties={"foo": "bar"},
                                           partitioningColumns=["col1"],
                                           tblComment="comment")
                # verify generated columns.
                self.__verify_generated_column(tableName, deltaTable)

    def test_create_table_with_location_only(self) -> None:
        for ifNotExists in (False, True):
            path = self.tempFile + str(ifNotExists)
            deltaTable = self.__create_table(ifNotExists, location=path)

            self.__verify_table_schema("delta.`{}`".format(path),
                                       deltaTable.toDF().schema,
                                       ["col1", "col2"],
                                       [IntegerType(), IntegerType()],
                                       nullables={"col2"},
                                       comments={"col1": "foo"},
                                       partitioningColumns=["col1"],
                                       tblComment="comment")
            # verify generated columns.
            self.__verify_generated_column("delta.`{}`".format(path), deltaTable)

    def test_create_table_with_name_and_location(self) -> None:
        for ifNotExists in (False, True):
            path = self.tempFile + str(ifNotExists)
            tableName = "testTable{}".format(ifNotExists)
            with self.table(tableName):
                deltaTable = self.__create_table(
                    ifNotExists, tableName=tableName, location=path)

                self.__verify_table_schema(tableName,
                                           deltaTable.toDF().schema,
                                           ["col1", "col2"],
                                           [IntegerType(), IntegerType()],
                                           nullables={"col2"},
                                           comments={"col1": "foo"},
                                           properties={"foo": "bar"},
                                           partitioningColumns=["col1"],
                                           tblComment="comment")
                # verify generated columns.
                self.__verify_generated_column(tableName, deltaTable)

    def test_create_table_behavior(self) -> None:
        with self.table("testTable"):
            self.spark.sql("CREATE TABLE testTable (c1 int) USING DELTA")

            # Errors out if doesn't ignore.
            with self.assertRaises(AnalysisException) as error_ctx:
                self.__create_table(False, tableName="testTable")
            msg = str(error_ctx.exception)
            assert ("testTable" in msg and "already exists" in msg)

            # ignore table creation.
            self.__create_table(True, tableName="testTable")
            schema = self.spark.read.format("delta").table("testTable").schema
            self.__verify_table_schema("testTable",
                                       schema,
                                       ["c1"],
                                       [IntegerType()],
                                       nullables={"c1"})

    def test_replace_table_with_name_only(self) -> None:
        for orCreate in (False, True):
            tableName = "testTable{}".format(orCreate)
            with self.table(tableName):
                self.spark.sql("CREATE TABLE {} (c1 int) USING DELTA".format(tableName))
                deltaTable = self.__replace_table(orCreate, tableName=tableName)

                self.__verify_table_schema(tableName,
                                           deltaTable.toDF().schema,
                                           ["col1", "col2"],
                                           [IntegerType(), IntegerType()],
                                           nullables={"col2"},
                                           comments={"col1": "foo"},
                                           properties={"foo": "bar"},
                                           partitioningColumns=["col1"],
                                           tblComment="comment")
                # verify generated columns.
                self.__verify_generated_column(tableName, deltaTable)

    def test_replace_table_with_location_only(self) -> None:
        for orCreate in (False, True):
            path = self.tempFile + str(orCreate)
            self.__create_table(False, location=path)
            deltaTable = self.__replace_table(orCreate, location=path)

            self.__verify_table_schema("delta.`{}`".format(path),
                                       deltaTable.toDF().schema,
                                       ["col1", "col2"],
                                       [IntegerType(), IntegerType()],
                                       nullables={"col2"},
                                       comments={"col1": "foo"},
                                       properties={"foo": "bar"},
                                       partitioningColumns=["col1"],
                                       tblComment="comment")
            # verify generated columns.
            self.__verify_generated_column("delta.`{}`".format(path), deltaTable)

    def test_replace_table_with_name_and_location(self) -> None:
        for orCreate in (False, True):
            path = self.tempFile + str(orCreate)
            tableName = "testTable{}".format(orCreate)
            with self.table(tableName):
                self.spark.sql("CREATE TABLE {} (col int) USING DELTA LOCATION '{}'"
                               .format(tableName, path))
                deltaTable = self.__replace_table(
                    orCreate, tableName=tableName, location=path)

                self.__verify_table_schema(tableName,
                                           deltaTable.toDF().schema,
                                           ["col1", "col2"],
                                           [IntegerType(), IntegerType()],
                                           nullables={"col2"},
                                           comments={"col1": "foo"},
                                           properties={"foo": "bar"},
                                           partitioningColumns=["col1"],
                                           tblComment="comment")
                # verify generated columns.
                self.__verify_generated_column(tableName, deltaTable)

    def test_replace_table_behavior(self) -> None:
        with self.table("testTable"):
            with self.assertRaises(AnalysisException) as error_ctx:
                self.__replace_table(False, tableName="testTable")
            msg = str(error_ctx.exception)
            self.assertIn("testTable", msg)
            self.assertTrue("did not exist" in msg or "cannot be found" in msg)
            deltaTable = self.__replace_table(True, tableName="testTable")
            self.__verify_table_schema("testTable",
                                       deltaTable.toDF().schema,
                                       ["col1", "col2"],
                                       [IntegerType(), IntegerType()],
                                       nullables={"col2"},
                                       comments={"col1": "foo"},
                                       properties={"foo": "bar"},
                                       partitioningColumns=["col1"],
                                       tblComment="comment")

    def test_verify_paritionedBy_compatibility(self) -> None:
        with self.table("testTable"):
            tableBuilder = DeltaTable.create(self.spark).tableName("testTable") \
                .addColumn("col1", "int", comment="foo", nullable=False) \
                .addColumn("col2", IntegerType(), generatedAlwaysAs="col1 + 10") \
                .property("foo", "bar") \
                .comment("comment")
            tableBuilder._jbuilder = tableBuilder._jbuilder.partitionedBy(
                _to_seq(self.spark._sc, ["col1"])  # type: ignore[attr-defined]
            )
            deltaTable = tableBuilder.execute()
            self.__verify_table_schema("testTable",
                                       deltaTable.toDF().schema,
                                       ["col1", "col2"],
                                       [IntegerType(), IntegerType()],
                                       nullables={"col2"},
                                       comments={"col1": "foo"},
                                       properties={"foo": "bar"},
                                       partitioningColumns=["col1"],
                                       tblComment="comment")

    def test_delta_table_builder_with_bad_args(self) -> None:
        builder = DeltaTable.create(self.spark).location(self.tempFile)

        # bad table name
        with self.assertRaises(TypeError):
            builder.tableName(1)  # type: ignore[arg-type]

        # bad location
        with self.assertRaises(TypeError):
            builder.location(1)  # type: ignore[arg-type]

        # bad comment
        with self.assertRaises(TypeError):
            builder.comment(1)  # type: ignore[arg-type]

        # bad column name
        with self.assertRaises(TypeError):
            builder.addColumn(1, "int")  # type: ignore[arg-type]

        # bad datatype.
        with self.assertRaises(TypeError):
            builder.addColumn("a", 1)  # type: ignore[arg-type]

        # bad column datatype - can't be pared
        with self.assertRaises(ParseException):
            builder.addColumn("a", "1")
            builder.execute()

        # bad comment
        with self.assertRaises(TypeError):
            builder.addColumn("a", "int", comment=1)  # type: ignore[arg-type]

        # bad generatedAlwaysAs
        with self.assertRaises(TypeError):
            builder.addColumn("a", "int", generatedAlwaysAs=1)  # type: ignore[arg-type]

        # bad nullable
        with self.assertRaises(TypeError):
            builder.addColumn("a", "int", nullable=1)  # type: ignore[arg-type]

        # bad existing schema
        with self.assertRaises(TypeError):
            builder.addColumns(1)  # type: ignore[arg-type]

        # bad existing schema.
        with self.assertRaises(TypeError):
            builder.addColumns([StructField("1", IntegerType()), 1])  # type: ignore[list-item]

        # bad partitionedBy col name
        with self.assertRaises(TypeError):
            builder.partitionedBy(1)  # type: ignore[call-overload]

        with self.assertRaises(TypeError):
            builder.partitionedBy(1, "1")   # type: ignore[call-overload]

        with self.assertRaises(TypeError):
            builder.partitionedBy([1])  # type: ignore[list-item]

        # bad property key
        with self.assertRaises(TypeError):
            builder.property(1, "1")  # type: ignore[arg-type]

        # bad property value
        with self.assertRaises(TypeError):
            builder.property("1", 1)  # type: ignore[arg-type]

    def test_protocolUpgrade(self) -> None:
        try:
            self.spark.conf.set('spark.databricks.delta.minWriterVersion', '2')
            self.spark.conf.set('spark.databricks.delta.minReaderVersion', '1')
            self.__writeDeltaTable([('a', 1), ('b', 2), ('c', 3), ('d', 4)])
            dt = DeltaTable.forPath(self.spark, self.tempFile)
            dt.upgradeTableProtocol(1, 3)
        finally:
            self.spark.conf.unset('spark.databricks.delta.minWriterVersion')
            self.spark.conf.unset('spark.databricks.delta.minReaderVersion')

        # cannot downgrade once upgraded
        failed = False
        try:
            dt.upgradeTableProtocol(1, 2)
        except BaseException:
            failed = True
        self.assertTrue(failed, "The upgrade should have failed, because downgrades aren't allowed")

        # bad args
        with self.assertRaisesRegex(ValueError, "readerVersion"):
            dt.upgradeTableProtocol("abc", 3)  # type: ignore[arg-type]
        with self.assertRaisesRegex(ValueError, "readerVersion"):
            dt.upgradeTableProtocol([1], 3)  # type: ignore[arg-type]
        with self.assertRaisesRegex(ValueError, "readerVersion"):
            dt.upgradeTableProtocol([], 3)  # type: ignore[arg-type]
        with self.assertRaisesRegex(ValueError, "readerVersion"):
            dt.upgradeTableProtocol({}, 3)  # type: ignore[arg-type]
        with self.assertRaisesRegex(ValueError, "writerVersion"):
            dt.upgradeTableProtocol(1, "abc")  # type: ignore[arg-type]
        with self.assertRaisesRegex(ValueError, "writerVersion"):
            dt.upgradeTableProtocol(1, [3])  # type: ignore[arg-type]
        with self.assertRaisesRegex(ValueError, "writerVersion"):
            dt.upgradeTableProtocol(1, [])  # type: ignore[arg-type]
        with self.assertRaisesRegex(ValueError, "writerVersion"):
            dt.upgradeTableProtocol(1, {})  # type: ignore[arg-type]

    def test_restore_to_version(self) -> None:
        self.__writeDeltaTable([('a', 1), ('b', 2)])
        self.__overwriteDeltaTable([('a', 3), ('b', 2)],
                                   schema=["key_new", "value_new"],
                                   overwriteSchema='true')

        overwritten = DeltaTable.forPath(self.spark, self.tempFile).toDF()
        self.__checkAnswer(overwritten,
                           [Row(key_new='a', value_new=3), Row(key_new='b', value_new=2)])

        DeltaTable.forPath(self.spark, self.tempFile).restoreToVersion(0)
        restored = DeltaTable.forPath(self.spark, self.tempFile).toDF()

        self.__checkAnswer(restored, [Row(key='a', value=1), Row(key='b', value=2)])

    def test_restore_to_timestamp(self) -> None:
        self.__writeDeltaTable([('a', 1), ('b', 2)])
        timestampToRestore = DeltaTable.forPath(self.spark, self.tempFile) \
            .history() \
            .head() \
            .timestamp \
            .strftime('%Y-%m-%d %H:%M:%S.%f')

        self.__overwriteDeltaTable([('a', 3), ('b', 2)],
                                   schema=["key_new", "value_new"],
                                   overwriteSchema='true')

        overwritten = DeltaTable.forPath(self.spark, self.tempFile).toDF()
        self.__checkAnswer(overwritten,
                           [Row(key_new='a', value_new=3), Row(key_new='b', value_new=2)])

        DeltaTable.forPath(self.spark, self.tempFile).restoreToTimestamp(timestampToRestore)

        restored = DeltaTable.forPath(self.spark, self.tempFile).toDF()
        self.__checkAnswer(restored, [Row(key='a', value=1), Row(key='b', value=2)])

        # we cannot test the actual working of restore to timestamp here but we can make sure
        # that the api is being called at least
        def runRestore() -> None:
            DeltaTable.forPath(self.spark, self.tempFile).restoreToTimestamp('05/04/1999')
        self.__intercept(runRestore, "The provided timestamp ('05/04/1999') "
                                     "cannot be converted to a valid timestamp")

    def test_restore_invalid_inputs(self) -> None:
        df = self.spark.createDataFrame([('a', 1), ('b', 2), ('c', 3)], ["key", "value"])
        df.write.format("delta").save(self.tempFile)

        dt = DeltaTable.forPath(self.spark, self.tempFile)

        def runRestoreToTimestamp() -> None:
            dt.restoreToTimestamp(12342323232)  # type: ignore[arg-type]
        self.__intercept(runRestoreToTimestamp,
                         "timestamp needs to be a string but got '<class 'int'>'")

        def runRestoreToVersion() -> None:
            dt.restoreToVersion("0")  # type: ignore[arg-type]
        self.__intercept(runRestoreToVersion,
                         "version needs to be an int but got '<class 'str'>'")

    def test_optimize(self) -> None:
        # write an unoptimized delta table
        df = self.spark.createDataFrame([("a", 1), ("a", 2)], ["key", "value"]).repartition(1)
        df.write.format("delta").save(self.tempFile)
        df = self.spark.createDataFrame([("a", 3), ("a", 4)], ["key", "value"]).repartition(1)
        df.write.format("delta").save(self.tempFile, mode="append")
        df = self.spark.createDataFrame([("b", 1), ("b", 2)], ["key", "value"]).repartition(1)
        df.write.format("delta").save(self.tempFile, mode="append")

        # create DeltaTable
        dt = DeltaTable.forPath(self.spark, self.tempFile)

        # execute bin compaction
        optimizer = dt.optimize()
        res = optimizer.executeCompaction()
        op_params = dt.history().first().operationParameters

        # assertions
        self.assertEqual(1, res.first().metrics.numFilesAdded)
        self.assertEqual(3, res.first().metrics.numFilesRemoved)
        self.assertEqual('[]', op_params['predicate'])

        # test non-partition column
        def optimize() -> None:
            dt.optimize().where("key = 'a'").executeCompaction()
        self.__intercept(optimize,
                         "Predicate references non-partition column 'key'. "
                         "Only the partition columns may be referenced: []")

    def test_optimize_w_partition_filter(self) -> None:
        # write an unoptimized delta table
        df = self.spark.createDataFrame([("a", 1), ("a", 2)], ["key", "value"]).repartition(1)
        df.write.partitionBy("key").format("delta").save(self.tempFile)
        df = self.spark.createDataFrame([("a", 3), ("a", 4)], ["key", "value"]).repartition(1)
        df.write.partitionBy("key").format("delta").save(self.tempFile, mode="append")
        df = self.spark.createDataFrame([("b", 1), ("b", 2)], ["key", "value"]).repartition(1)
        df.write.partitionBy("key").format("delta").save(self.tempFile, mode="append")

        # create DeltaTable
        dt = DeltaTable.forPath(self.spark, self.tempFile)

        # execute bin compaction
        optimizer = dt.optimize().where("key = 'a'")
        res = optimizer.executeCompaction()
        op_params = dt.history().first().operationParameters

        # assertions
        self.assertEqual(1, res.first().metrics.numFilesAdded)
        self.assertEqual(2, res.first().metrics.numFilesRemoved)
        self.assertEqual('''["('key = a)"]''', op_params['predicate'])

        # test non-partition column
        def optimize() -> None:
            dt.optimize().where("value = 1").executeCompaction()
        self.__intercept(optimize,
                         "Predicate references non-partition column 'value'. "
                         "Only the partition columns may be referenced: [key]")

    def test_optimize_zorder_by(self) -> None:
        # write an unoptimized delta table
        self.spark.createDataFrame([i for i in range(0, 100)], IntegerType()) \
            .withColumn("col1", floor(col("value") % 7)) \
            .withColumn("col2", floor(col("value") % 27)) \
            .withColumn("p", floor(col("value") % 10)) \
            .repartition(4).write.partitionBy("p").format("delta").save(self.tempFile)

        # get the number of data files in the current version
        numDataFilesPreZOrder = self.spark.read.format("delta").load(self.tempFile) \
            .select("_metadata.file_path").distinct().count()

        # create DeltaTable
        dt = DeltaTable.forPath(self.spark, self.tempFile)

        # execute Z-Order Optimization
        optimizer = dt.optimize()
        result = optimizer.executeZOrderBy(["col1", "col2"])
        metrics = result.select("metrics.*").head()

        # expect there is only one file after the Z-Order as Z-Order also
        # does the compaction implicitly and all small files are written to one file
        # for each partition. Ther are 10 partitions in the table, so expect 10 final files
        numDataFilesPostZOrder = 10

        self.assertEqual(numDataFilesPostZOrder, metrics.numFilesAdded)
        self.assertEqual(numDataFilesPreZOrder, metrics.numFilesRemoved)
        self.assertEqual(0, metrics.totalFilesSkipped)
        self.assertEqual(numDataFilesPreZOrder, metrics.totalConsideredFiles)
        self.assertEqual('all', metrics.zOrderStats.strategyName)
        self.assertEqual(10, metrics.zOrderStats.numOutputCubes)  # one for each partition

        # negative test: Z-Order on partition column
        def optimize() -> None:
            dt.optimize().where("p = 1").executeZOrderBy(["p"])
        self.__intercept(optimize,
                         "p is a partition column. "
                         "Z-Ordering can only be performed on data columns")

    def test_optimize_zorder_by_w_partition_filter(self) -> None:
        # write an unoptimized delta table
        df = self.spark.createDataFrame([i for i in range(0, 100)], IntegerType()) \
            .withColumn("col1", floor(col("value") % 7)) \
            .withColumn("col2", floor(col("value") % 27)) \
            .withColumn("p", floor(col("value") % 10)) \
            .repartition(4).write.partitionBy("p")

        df.format("delta").save(self.tempFile)

        # get the number of data files in the current version in partition p = 2
        numDataFilesPreZOrder = self.spark.read.format("delta").load(self.tempFile) \
            .filter("p=2").select("_metadata.file_path").distinct().count()

        # create DeltaTable
        dt = DeltaTable.forPath(self.spark, self.tempFile)

        # execute Z-OrderBy
        optimizer = dt.optimize().where("p = 2")
        result = optimizer.executeZOrderBy(["col1", "col2"])
        metrics = result.select("metrics.*").head()

        # expect there is only one file after the Z-Order as Z-Order also
        # does the compaction implicitly and all small files are written to one file
        numDataFilesPostZOrder = 1

        self.assertEqual(numDataFilesPostZOrder, metrics.numFilesAdded)
        self.assertEqual(numDataFilesPreZOrder, metrics.numFilesRemoved)
        self.assertEqual(0, metrics.totalFilesSkipped)
        # expected to consider all input files for Z-Order
        self.assertEqual(numDataFilesPreZOrder, metrics.totalConsideredFiles)
        self.assertEqual('all', metrics.zOrderStats.strategyName)
        self.assertEqual(1, metrics.zOrderStats.numOutputCubes)  # one per each affected partition

    def __checkAnswer(self, df: DataFrame,
                      expectedAnswer: List[Any],
                      schema: Union[StructType, List[str]] = ["key", "value"]) -> None:
        if not expectedAnswer:
            self.assertEqual(df.count(), 0)
            return
        expectedDF = self.spark.createDataFrame(expectedAnswer, schema)
        try:
            self.assertEqual(df.count(), expectedDF.count())
            self.assertEqual(len(df.columns), len(expectedDF.columns))
            self.assertEqual([], df.subtract(expectedDF).take(1))
            self.assertEqual([], expectedDF.subtract(df).take(1))
        except AssertionError:
            print("Expected:")
            expectedDF.show()
            print("Found:")
            df.show()
            raise

    def __writeDeltaTable(self, datalist: List[Tuple[Any, Any]]) -> None:
        df = self.spark.createDataFrame(datalist, ["key", "value"])
        df.write.format("delta").save(self.tempFile)

    def __writeAsTable(self, datalist: List[Tuple[Any, Any]], tblName: str) -> None:
        df = self.spark.createDataFrame(datalist, ["key", "value"])
        df.write.format("delta").saveAsTable(tblName)

    def __overwriteDeltaTable(self, datalist: List[Tuple[Any, Any]],
                              schema: Union[StructType, List[str]] = ["key", "value"],
                              overwriteSchema: str = 'false') -> None:
        df = self.spark.createDataFrame(datalist, schema)
        df.write.format("delta") \
            .option('overwriteSchema', overwriteSchema) \
            .mode("overwrite") \
            .save(self.tempFile)

    def __createFile(self, fileName: str, content: Any) -> None:
        with open(os.path.join(self.tempFile, fileName), 'w') as f:
            f.write(content)

    def __checkFileExists(self, fileName: str) -> bool:
        return os.path.exists(os.path.join(self.tempFile, fileName))

    def __intercept(self, func: Callable[[], None], exceptionMsg: str) -> None:
        seenTheRightException = False
        try:
            func()
        except Exception as e:
            if exceptionMsg in str(e):
                seenTheRightException = True
        assert seenTheRightException, ("Did not catch expected Exception:" + exceptionMsg)


class DeltaTableTests(DeltaTableTestsMixin, DeltaTestCase):
    pass


if __name__ == "__main__":
    try:
        import xmlrunner
        testRunner = xmlrunner.XMLTestRunner(output='target/test-reports', verbosity=4)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=4)
