#
# Copyright (2020) The Delta Lake Project Authors.
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
import os

from pyspark.sql import Row
from pyspark.sql.functions import col, lit, expr
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType
from pyspark.sql.utils import AnalysisException, ParseException

from delta.tables import DeltaTable
from delta.testing.utils import DeltaTestCase


class DeltaTableTests(DeltaTestCase):

    def test_forPath(self):
        self.__writeDeltaTable([('a', 1), ('b', 2), ('c', 3)])
        dt = DeltaTable.forPath(self.spark, self.tempFile).toDF()
        self.__checkAnswer(dt, [('a', 1), ('b', 2), ('c', 3)])

    def test_forName(self):
        self.__writeAsTable([('a', 1), ('b', 2), ('c', 3)], "test")
        df = DeltaTable.forName(self.spark, "test").toDF()
        self.__checkAnswer(df, [('a', 1), ('b', 2), ('c', 3)])

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

    def test_generate(self):
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
        assert(len(files) == numFiles)

    def test_update(self):
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
            dt.update({"value": "200"})

        with self.assertRaisesRegex(ValueError, "cannot be None"):
            dt.update(condition='a')

        with self.assertRaisesRegex(TypeError, "must be a dict"):
            dt.update(set=1)

        with self.assertRaisesRegex(TypeError, "must be a Spark SQL Column or a string"):
            dt.update(1, {})

        with self.assertRaisesRegex(TypeError, "Values of dict in .* must contain only"):
            dt.update(set={"value": 1})

        with self.assertRaisesRegex(TypeError, "Keys of dict in .* must contain only"):
            dt.update(set={1: ""})

        with self.assertRaises(TypeError):
            dt.update(set=1)

    def test_merge(self):
        self.__writeDeltaTable([('a', 1), ('b', 2), ('c', 3), ('d', 4)])
        source = self.spark.createDataFrame([('a', -1), ('b', 0), ('e', -5), ('f', -6)], ["k", "v"])

        def reset_table():
            self.__overwriteDeltaTable([('a', 1), ('b', 2), ('c', 3), ('d', 4)])

        dt = DeltaTable.forPath(self.spark, self.tempFile)

        # ============== Test basic syntax ==============

        # String expressions in merge condition and dicts
        reset_table()
        dt.merge(source, "key = k") \
            .whenMatchedUpdate(set={"value": "v + 0"}) \
            .whenNotMatchedInsert(values={"key": "k", "value": "v + 0"}) \
            .execute()
        self.__checkAnswer(dt.toDF(),
                           ([('a', -1), ('b', 0), ('c', 3), ('d', 4), ('e', -5), ('f', -6)]))

        # Column expressions in merge condition and dicts
        reset_table()
        dt.merge(source, expr("key = k")) \
            .whenMatchedUpdate(set={"value": col("v") + 0}) \
            .whenNotMatchedInsert(values={"key": "k", "value": col("v") + 0}) \
            .execute()
        self.__checkAnswer(dt.toDF(),
                           ([('a', -1), ('b', 0), ('c', 3), ('d', 4), ('e', -5), ('f', -6)]))

        # ============== Test clause conditions ==============

        # String expressions in all conditions and dicts
        reset_table()
        dt.merge(source, "key = k") \
            .whenMatchedUpdate(condition="k = 'a'", set={"value": "v + 0"}) \
            .whenMatchedDelete(condition="k = 'b'") \
            .whenNotMatchedInsert(condition="k = 'e'", values={"key": "k", "value": "v + 0"}) \
            .execute()
        self.__checkAnswer(dt.toDF(), ([('a', -1), ('c', 3), ('d', 4), ('e', -5)]))

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
            .execute()
        self.__checkAnswer(dt.toDF(), ([('a', -1), ('c', 3), ('d', 4), ('e', -5)]))

        # Positional arguments
        reset_table()
        dt.merge(source, "key = k") \
            .whenMatchedUpdate("k = 'a'", {"value": "v + 0"}) \
            .whenMatchedDelete("k = 'b'") \
            .whenNotMatchedInsert("k = 'e'", {"key": "k", "value": "v + 0"}) \
            .execute()
        self.__checkAnswer(dt.toDF(), ([('a', -1), ('c', 3), ('d', 4), ('e', -5)]))

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
            dt.merge(1, "key = k")

        with self.assertRaisesRegex(TypeError, "must be a Spark SQL Column or a string"):
            dt.merge(source, 1)

        # ---- bad args in whenMatchedUpdate()
        with self.assertRaisesRegex(ValueError, "cannot be None"):
            dt.merge(source, "key = k").whenMatchedUpdate({"value": "v"})

        with self.assertRaisesRegex(ValueError, "cannot be None"):
            dt.merge(source, "key = k").whenMatchedUpdate(1)

        with self.assertRaisesRegex(ValueError, "cannot be None"):
            dt.merge(source, "key = k").whenMatchedUpdate(condition="key = 'a'")

        with self.assertRaisesRegex(TypeError, "must be a Spark SQL Column or a string"):
            dt.merge(source, "key = k").whenMatchedUpdate(1, {"value": "v"})

        with self.assertRaisesRegex(TypeError, "must be a dict"):
            dt.merge(source, "key = k").whenMatchedUpdate("k = 'a'", 1)

        with self.assertRaisesRegex(TypeError, "Values of dict in .* must contain only"):
            dt.merge(source, "key = k").whenMatchedUpdate(set={"value": 1})

        with self.assertRaisesRegex(TypeError, "Keys of dict in .* must contain only"):
            dt.merge(source, "key = k").whenMatchedUpdate(set={1: ""})

        with self.assertRaises(TypeError):
            dt.merge(source, "key = k").whenMatchedUpdate(set="k = 'a'", condition={"value": 1})

        # bad args in whenMatchedDelete()
        with self.assertRaisesRegex(TypeError, "must be a Spark SQL Column or a string"):
            dt.merge(source, "key = k").whenMatchedDelete(1)

        # ---- bad args in whenNotMatchedInsert()
        with self.assertRaisesRegex(ValueError, "cannot be None"):
            dt.merge(source, "key = k").whenNotMatchedInsert({"value": "v"})

        with self.assertRaisesRegex(ValueError, "cannot be None"):
            dt.merge(source, "key = k").whenNotMatchedInsert(1)

        with self.assertRaisesRegex(ValueError, "cannot be None"):
            dt.merge(source, "key = k").whenNotMatchedInsert(condition="key = 'a'")

        with self.assertRaisesRegex(TypeError, "must be a Spark SQL Column or a string"):
            dt.merge(source, "key = k").whenNotMatchedInsert(1, {"value": "v"})

        with self.assertRaisesRegex(TypeError, "must be a dict"):
            dt.merge(source, "key = k").whenNotMatchedInsert("k = 'a'", 1)

        with self.assertRaisesRegex(TypeError, "Values of dict in .* must contain only"):
            dt.merge(source, "key = k").whenNotMatchedInsert(values={"value": 1})

        with self.assertRaisesRegex(TypeError, "Keys of dict in .* must contain only"):
            dt.merge(source, "key = k").whenNotMatchedInsert(values={1: "value"})

        with self.assertRaises(TypeError):
            dt.merge(source, "key = k").whenNotMatchedInsert(
                values="k = 'a'", condition={"value": 1})

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
        dt.vacuum(1000)  # test whether integers work

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

    def test_isDeltaTable(self):
        df = self.spark.createDataFrame([('a', 1), ('b', 2), ('c', 3)], ["key", "value"])
        df.write.format("parquet").save(self.tempFile)
        tempFile2 = self.tempFile + '_2'
        df.write.format("delta").save(tempFile2)
        self.assertEqual(DeltaTable.isDeltaTable(self.spark, self.tempFile), False)
        self.assertEqual(DeltaTable.isDeltaTable(self.spark, tempFile2), True)

    def __verify_table_schema(self, tableName, schema, cols,
                              types, nullables={}, comments={},
                              properties={}):
        fields = []
        for i in range(len(cols)):
            col = cols[i]
            dataType = types[i]
            metadata = {}
            if col in comments:
                metadata["comment"] = comments[col]
            fields.append(StructField(col, dataType, col in nullables, metadata))
        assert (StructType(fields) == schema)
        if len(properties) > 0:
            tablePropertyMap = self.spark.sql("SHOW TBLPROPERTIES {}".format(tableName)) \
                .rdd.collectAsMap()
            for key in properties:
                assert (key in tablePropertyMap)
                assert (tablePropertyMap[key] == properties[key])

    def __verify_generated_column(self, tableName, deltaTable):
        cmd = "INSERT INTO {table} (col1, col2) VALUES (1, 11)".format(table=tableName)
        self.spark.sql(cmd)
        deltaTable.update(expr("col2 = 11"), {"col1": expr("2")})
        self.__checkAnswer(deltaTable.toDF(), [(2, 12)], schema=["col1", "col2"])

    def __build_delta_table(self, builder):
        return builder.addColumn("col1", "int", comment="foo", nullable=False) \
            .addColumn("col2", IntegerType(), generatedAlwaysAs="col1 + 10") \
            .property("foo", "bar") \
            .comment("comment") \
            .partitionedBy("col1").execute()

    def __create_table(self, ifNotExists, tableName=None, location=None):
        builder = DeltaTable.createIfNotExists(self.spark) if ifNotExists \
            else DeltaTable.create(self.spark)
        if tableName:
            builder = builder.tableName(tableName)
        if location:
            builder = builder.location(location)
        return self.__build_delta_table(builder)

    def __replace_table(self, orCreate, tableName=None, location=None):
        builder = DeltaTable.createOrReplace(self.spark) if orCreate \
            else DeltaTable.replace(self.spark)
        if tableName:
            builder = builder.tableName(tableName)
        if location:
            builder = builder.location(location)
        return self.__build_delta_table(builder)

    def test_create_table_with_existing_schema(self):
        df = self.spark.createDataFrame([('a', 1), ('b', 2), ('c', 3)], ["key", "value"])
        deltaTable = DeltaTable.create(self.spark).tableName("test") \
            .addColumns(df.schema) \
            .addColumn("value2", dataType="int").execute()
        self.__verify_table_schema("test",
                                   deltaTable.toDF().schema,
                                   ["key", "value", "value2"],
                                   [StringType(), LongType(), IntegerType()],
                                   nullables={"key", "value", "value2"})

        # verify creating table with list of structFields
        deltaTable2 = DeltaTable.create(self.spark).tableName("test2").addColumns(
            df.schema.fields) \
            .addColumn("value2", dataType="int").execute()
        self.__verify_table_schema("test2",
                                   deltaTable2.toDF().schema,
                                   ["key", "value", "value2"],
                                   [StringType(), LongType(), IntegerType()],
                                   nullables={"key", "value", "value2"})

    def test_create_replace_table_with_no_spark_session_passed(self):
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

    def test_create_table_with_name_only(self):
        for ifNotExists in (False, True):
            tableName = "testTable{}".format(ifNotExists)
            deltaTable = self.__create_table(ifNotExists, tableName=tableName)

            self.__verify_table_schema(tableName,
                                       deltaTable.toDF().schema,
                                       ["col1", "col2"],
                                       [IntegerType(), IntegerType()],
                                       nullables={"col2"},
                                       comments={"col1": "foo"},
                                       properties={"foo": "bar"})
            # verify generated columns.
            self.__verify_generated_column(tableName, deltaTable)
            self.spark.sql("DROP TABLE IF EXISTS {}".format(tableName))

    def test_create_table_with_location_only(self):
        for ifNotExists in (False, True):
            path = self.tempFile + str(ifNotExists)
            deltaTable = self.__create_table(ifNotExists, location=path)

            self.__verify_table_schema("delta.`{}`".format(path),
                                       deltaTable.toDF().schema,
                                       ["col1", "col2"],
                                       [IntegerType(), IntegerType()],
                                       nullables={"col2"},
                                       comments={"col1": "foo"})
            # verify generated columns.
            self.__verify_generated_column("delta.`{}`".format(path), deltaTable)

    def test_create_table_with_name_and_location(self):
        for ifNotExists in (False, True):
            path = self.tempFile + str(ifNotExists)
            tableName = "testTable{}".format(ifNotExists)
            deltaTable = self.__create_table(
                ifNotExists, tableName=tableName, location=path)

            self.__verify_table_schema(tableName,
                                       deltaTable.toDF().schema,
                                       ["col1", "col2"],
                                       [IntegerType(), IntegerType()],
                                       nullables={"col2"},
                                       comments={"col1": "foo"},
                                       properties={"foo": "bar"})
            # verify generated columns.
            self.__verify_generated_column(tableName, deltaTable)
            self.spark.sql("DROP TABLE IF EXISTS {}".format(tableName))

    def test_create_table_behavior(self):
        self.spark.sql("CREATE TABLE testTable (c1 int) USING DELTA")

        # Errors out if doesn't ignore.
        try:
            self.__create_table(False, tableName="testTable")
        except AnalysisException as e:
            msg = e.desc
        assert (msg == "Table default.testTable already exists")

        # ignore table creation.
        self.__create_table(True, tableName="testTable")
        schema = self.spark.read.format("delta").table("testTable").schema
        self.__verify_table_schema("testTable",
                                   schema,
                                   ["c1"],
                                   [IntegerType()],
                                   nullables={"c1"})

    def test_replace_table_with_name_only(self):
        for orCreate in (False, True):
            tableName = "testTable{}".format(orCreate)
            self.spark.sql("CREATE TABLE {} (c1 int) USING DELTA".format(tableName))
            deltaTable = self.__replace_table(orCreate, tableName=tableName)

            self.__verify_table_schema(tableName,
                                       deltaTable.toDF().schema,
                                       ["col1", "col2"],
                                       [IntegerType(), IntegerType()],
                                       nullables={"col2"},
                                       comments={"col1": "foo"},
                                       properties={"foo": "bar"})
            # verify generated columns.
            self.__verify_generated_column(tableName, deltaTable)
            self.spark.sql("DROP TABLE IF EXISTS {}".format(tableName))

    def test_replace_table_with_location_only(self):
        for orCreate in (False, True):
            path = self.tempFile + str(orCreate)
            self.__create_table(False, location=path)
            deltaTable = self.__replace_table(orCreate, location=path)

            self.__verify_table_schema("delta.`{}`".format(path),
                                       deltaTable.toDF().schema,
                                       ["col1", "col2"],
                                       [IntegerType(), IntegerType()],
                                       nullables={"col2"},
                                       comments={"col1": "foo"})
            # verify generated columns.
            self.__verify_generated_column("delta.`{}`".format(path), deltaTable)

    def test_replace_table_with_name_and_location(self):
        for orCreate in (False, True):
            path = self.tempFile + str(orCreate)
            tableName = "testTable{}".format(orCreate)
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
                                       properties={"foo": "bar"})
            # verify generated columns.
            self.__verify_generated_column(tableName, deltaTable)
            self.spark.sql("DROP TABLE IF EXISTS {}".format(tableName))

    def test_replace_table_behavior(self):
        msg = None
        try:
            self.__replace_table(False, tableName="testTable")
        except AnalysisException as e:
            msg = e.desc
        assert (msg.startswith(
            "Table default.testTable cannot be replaced as it did not exist."))
        deltaTable = self.__replace_table(True, tableName="testTable")
        self.__verify_table_schema("testTable",
                                   deltaTable.toDF().schema,
                                   ["col1", "col2"],
                                   [IntegerType(), IntegerType()],
                                   nullables={"col2"},
                                   comments={"col1": "foo"},
                                   properties={"foo": "bar"})

    def test_delta_table_builder_with_bad_args(self):
        builder = DeltaTable.create(self.spark)

        # bad table name
        with self.assertRaises(TypeError):
            builder.tableName(1)

        # bad location
        with self.assertRaises(TypeError):
            builder.location(1)

        # bad comment
        with self.assertRaises(TypeError):
            builder.comment(1)

        # bad column name
        with self.assertRaises(TypeError):
            builder.addColumn(1, "int")

        # bad datatype.
        with self.assertRaises(TypeError):
            builder.addColumn("a", 1)

        # bad column datatype - can't be pared
        with self.assertRaises(ParseException):
            builder.addColumn("a", "1")

        # bad comment
        with self.assertRaises(TypeError):
            builder.addColumn("a", "int", comment=1)

        # bad generatedAlwaysAs
        with self.assertRaises(TypeError):
            builder.addColumn("a", "int", generatedAlwaysAs=1)

        # bad nullable
        with self.assertRaises(TypeError):
            builder.addColumn("a", "int", nullable=1)

        # bad existing schema
        with self.assertRaises(TypeError):
            builder.addColumns(1)

        # bad existing schema.
        with self.assertRaises(TypeError):
            builder.addColumns([StructField("1", IntegerType()), 1])

        # bad partitionedBy col name
        with self.assertRaises(TypeError):
            builder.partitionedBy(1)

        with self.assertRaises(TypeError):
            builder.partitionedBy(1, "1")

        # bad property key
        with self.assertRaises(TypeError):
            builder.property(1, "1")

        # bad property value
        with self.assertRaises(TypeError):
            builder.property("1", 1)

    def test_protocolUpgrade(self):
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
        except:
            failed = True
        self.assertTrue(failed, "The upgrade should have failed, because downgrades aren't allowed")

        # bad args
        with self.assertRaisesRegex(ValueError, "readerVersion"):
            dt.upgradeTableProtocol("abc", 3)
        with self.assertRaisesRegex(ValueError, "readerVersion"):
            dt.upgradeTableProtocol([1], 3)
        with self.assertRaisesRegex(ValueError, "readerVersion"):
            dt.upgradeTableProtocol([], 3)
        with self.assertRaisesRegex(ValueError, "readerVersion"):
            dt.upgradeTableProtocol({}, 3)
        with self.assertRaisesRegex(ValueError, "writerVersion"):
            dt.upgradeTableProtocol(1, "abc")
        with self.assertRaisesRegex(ValueError, "writerVersion"):
            dt.upgradeTableProtocol(1, [3])
        with self.assertRaisesRegex(ValueError, "writerVersion"):
            dt.upgradeTableProtocol(1, [])
        with self.assertRaisesRegex(ValueError, "writerVersion"):
            dt.upgradeTableProtocol(1, {})

    def __checkAnswer(self, df, expectedAnswer, schema=["key", "value"]):
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

    def __writeDeltaTable(self, datalist):
        df = self.spark.createDataFrame(datalist, ["key", "value"])
        df.write.format("delta").save(self.tempFile)

    def __writeAsTable(self, datalist, tblName):
        df = self.spark.createDataFrame(datalist, ["key", "value"])
        df.write.format("delta").saveAsTable(tblName)

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
