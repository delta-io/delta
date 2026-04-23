/*
 * Copyright (2021) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.delta

import scala.language.implicitConversions

import org.apache.spark.sql.delta.sources.DeltaSQLConf

import org.apache.spark.sql.{AnalysisException, DataFrame, QueryTest, Row}
import org.apache.spark.sql.functions.{array, lit, struct}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.StoreAssignmentPolicy
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{ArrayType, DateType, IntegerType, LongType, MapType, NullType, StringType, StructField, StructType}
import org.apache.spark.util.Utils

/**
 * Trait collecting schema evolution test runner methods and other helpers.
 */
trait MergeIntoSchemaEvolutionMixin extends QueryTest {
  self: SharedSparkSession with MergeIntoTestUtils =>

  protected implicit def strToJsonSeq(str: String): Seq[String] = {
    str.split("\n").filter(_.trim.length > 0)
  }

  /**
   * Helper method similar to [[testEvolution()]] but without aliasing the target and source tables
   * as 't' and 's'. Used to check that attribute resolution works correctly with schema evolution
   * when using column name qualified with the actual table name: `table_name.column`.
   */
  def testEvolutionWithoutTableAliases(name: String)(
      targetData: => DataFrame,
      sourceData: => DataFrame,
      clauses: MergeClause*)(
      expected: => Seq[Row] = Seq.empty,
      expectErrorContains: String = null,
      expectErrorWithoutEvolutionContains: String = null): Unit =
    for (schemaEvolutionEnabled <- BOOLEAN_DOMAIN)
    test(s"schema evolution - $name - schemaEvolutionEnabled= $schemaEvolutionEnabled") {
      withTable("target", "source") {
        targetData.write.format("delta").saveAsTable("target")
        sourceData.write.format("delta").saveAsTable("source")
        withSQLConf(DeltaSQLConf.DELTA_SCHEMA_AUTO_MIGRATE.key -> schemaEvolutionEnabled.toString) {
          if (!schemaEvolutionEnabled && expectErrorWithoutEvolutionContains != null) {
            val ex = intercept[AnalysisException] {
              executeMerge(tgt = "target", src = "source", cond = "1 = 1", clauses: _*)
            }
            errorContains(ex.getMessage, expectErrorWithoutEvolutionContains)
          } else if (schemaEvolutionEnabled && expectErrorContains != null) {
            val ex = intercept[AnalysisException] {
              executeMerge(tgt = "target", src = "source", cond = "1 = 1", clauses: _*)
            }
            errorContains(ex.getMessage, expectErrorContains)
          } else {
            executeMerge(tgt = "target", src = "source", cond = "1 = 1", clauses: _*)
            checkAnswer(spark.read.table("target"), expected)
          }
        }
      }
    }

  /**
   * Test runner used by most non-nested schema evolution tests. Runs the MERGE operation once with
   * schema evolution disabled then with schema evolution enabled. Tests must provide for each case
   * either the expected result or the expected error message but not both.
   */
  // scalastyle:off argcount
  protected def testEvolution(name: String)(
      targetData: => DataFrame,
      sourceData: => DataFrame,
      cond: String = "t.key = s.key",
      clauses: Seq[MergeClause] = Seq.empty,
      expected: => DataFrame = null,
      expectedWithoutEvolution: => DataFrame = null,
      expectedSchema: StructType = null,
      expectedSchemaWithoutEvolution: StructType = null,
      expectErrorContains: String = null,
      expectErrorWithoutEvolutionContains: String = null,
      confs: Seq[(String, String)] = Seq(),
      partitionCols: Seq[String] = Seq.empty): Unit = {

    def executeMergeAndAssert(df: DataFrame, schema: StructType, error: String): Unit = {
      append(targetData, partitionCols)
      withTempView("source") {
        sourceData.createOrReplaceTempView("source")

        if (error != null) {
          val ex = intercept[AnalysisException] {
            executeMerge(s"$tableSQLIdentifier t", "source s", cond, clauses: _*)
          }
          errorContains(Utils.exceptionString(ex), error)
        } else {
          executeMerge(s"$tableSQLIdentifier t", "source s", cond, clauses: _*)
          checkAnswer(readDeltaTableByIdentifier(), df.collect())
          if (schema != null) {
            assert(readDeltaTableByIdentifier().schema === schema)
          } else {
            // Check against the schema of the expected result df if no explicit schema was
            // provided. Nullability of fields will vary depending on the actual data in the df so
            // we ignore it.
            assert(readDeltaTableByIdentifier().schema.asNullable ===
              df.schema.asNullable)
          }
        }
      }
    }

    test(s"schema evolution - $name - with evolution disabled") {
      withSQLConf(confs :+ (DeltaSQLConf.DELTA_SCHEMA_AUTO_MIGRATE.key, "false"): _*) {
        executeMergeAndAssert(expectedWithoutEvolution, expectedSchemaWithoutEvolution,
          expectErrorWithoutEvolutionContains)
      }
    }

    test(s"schema evolution - $name") {
      withSQLConf((confs :+ (DeltaSQLConf.DELTA_SCHEMA_AUTO_MIGRATE.key, "true")): _*) {
        executeMergeAndAssert(expected, expectedSchema, expectErrorContains)
      }
    }
  }
  // scalastyle:on argcount

   /**
   * Test runner used by most nested schema evolution tests. Similar to `testEvolution()` except
   * that the target & source data and expected results are parsed as JSON strings for convenience.
   */
  // scalastyle:off argcount
  protected def testNestedStructsEvolution(name: String)(
      target: Seq[String],
      source: Seq[String],
      targetSchema: StructType,
      sourceSchema: StructType,
      cond: String = "t.key = s.key",
      clauses: Seq[MergeClause] = Seq.empty,
      result: Seq[String] = null,
      resultSchema: StructType = null,
      resultWithoutEvolution: Seq[String] = null,
      expectErrorContains: String = null,
      expectErrorWithoutEvolutionContains: String = null,
      confs: Seq[(String, String)] = Seq()): Unit = {
    testEvolution(name) (
      targetData = readFromJSON(target, targetSchema),
      sourceData = readFromJSON(source, sourceSchema),
      cond,
      clauses = clauses,
      expected =
        if (result != null ) {
          val schema = if (resultSchema != null) resultSchema else targetSchema
          readFromJSON(result, schema)
        } else {
          null
        },
      expectedSchema = resultSchema,
      expectErrorContains = expectErrorContains,
      expectedWithoutEvolution =
        if (resultWithoutEvolution != null) {
          readFromJSON(resultWithoutEvolution, targetSchema)
        } else {
          null
        },
      expectedSchemaWithoutEvolution = targetSchema,
      expectErrorWithoutEvolutionContains = expectErrorWithoutEvolutionContains,
      confs = confs
    )
  }
  // scalastyle:on argcount
}

/**
 * Trait collecting a subset of tests providing core coverage for schema evolution. Mix this trait
 * in other suites to get basic test coverage for schema evolution in combination with other
 * features, e.g. CDF, DVs.
 */
trait MergeIntoSchemaEvolutionCoreTests extends MergeIntoSchemaEvolutionMixin {
  self: MergeIntoTestUtils with SharedSparkSession =>

  import testImplicits._

  testEvolution("new column with only insert *")(
    targetData = Seq((0, 0), (1, 10), (3, 30)).toDF("key", "value"),
    sourceData = Seq((1, 1, "extra1"), (2, 2, "extra2")).toDF("key", "value", "extra"),
    clauses = insert("*") :: Nil,
    expected =
      ((0, 0, null) +: (3, 30, null) +: // unchanged
        (1, 10, null) +:  // not updated
        (2, 2, "extra2") +: Nil // newly inserted
      ).toDF("key", "value", "extra"),
    expectedWithoutEvolution =
      ((0, 0) +: (3, 30) +: (1, 10) +: (2, 2) +: Nil).toDF("key", "value")
  )

  testEvolution("new column with only update *")(
    targetData = Seq((0, 0), (1, 10), (3, 30)).toDF("key", "value"),
    sourceData = Seq((1, 1, "extra1"), (2, 2, "extra2")).toDF("key", "value", "extra"),
    clauses = update("*") :: Nil,
    expected =
      ((0, 0, null) +: (3, 30, null) +:
        (1, 1, "extra1") +: // updated
        Nil // row 2 not inserted
      ).toDF("key", "value", "extra"),
    expectedWithoutEvolution = ((0, 0) +: (3, 30) +: (1, 1) +: Nil).toDF("key", "value")
  )

  testEvolution("new column with insert * and delete not matched by source")(
    sourceData = Seq((1, 1, "extra1"), (2, 2, "extra2")).toDF("key", "value", "extra"),
    targetData = Seq((0, 0), (1, 10), (3, 30)).toDF("key", "value"),
    clauses = insert("*") ::
      deleteNotMatched() :: Nil,
    expected = Seq(
      // (0, 0) Not matched by source, deleted
      (1, 10, null), // Matched, updated
      (2, 2, "extra2") // Not matched by target, inserted
      // (3, 30) Not matched by source, deleted
    ).toDF("key", "value", "extra"),
    expectedWithoutEvolution = Seq((1, 10), (2, 2)).toDF("key", "value"))

  testNestedStructsEvolution("new nested source field added when updating top-level column")(
    target = Seq("""{ "key": "A", "value": { "a": 1 } }"""),
    source = Seq("""{ "key": "A", "value": { "a": 2, "b": 3 } }"""),
    targetSchema = new StructType()
      .add("key", StringType)
      .add("value", new StructType()
        .add("a", IntegerType)),
    sourceSchema = new StructType()
      .add("key", StringType)
      .add("value", new StructType()
        .add("a", IntegerType)
        .add("b", IntegerType)),
    clauses = update("value = s.value") :: Nil,
    result = Seq("""{ "key": "A", "value": { "a": 2, "b": 3 } }"""),
    resultSchema = new StructType()
      .add("key", StringType)
      .add("value", new StructType()
        .add("a", IntegerType)
        .add("b", IntegerType)),
    expectErrorWithoutEvolutionContains = "Cannot cast")
}

/**
 * Trait collecting all base and new column tests for schema evolution.
 */
trait MergeIntoSchemaEvolutionBaseNewColumnTests extends MergeIntoSchemaEvolutionMixin {
  self: MergeIntoTestUtils
    with SharedSparkSession =>

  import testImplicits._

  // Schema evolution with UPDATE SET alone
  testEvolution("new column with update set")(
    targetData = Seq((0, 0), (1, 10), (3, 30)).toDF("key", "value"),
    sourceData = Seq((1, 1, "extra1"), (2, 2, "extra2")).toDF("key", "value", "extra"),
    clauses = update(set = "key = s.key, value = s.value, extra = s.extra") :: Nil,
    expected = ((0, 0, null) +: (3, 30, null) +: (1, 1, "extra1") +: Nil)
      .toDF("key", "value", "extra"),
    expectErrorWithoutEvolutionContains = "cannot resolve extra in UPDATE clause")

  testEvolution("new column updated with value from existing column")(
    targetData = Seq((0, 0), (1, 10), (3, 30)).toDF("key", "value"),
    sourceData = Seq((1, 1, -1), (2, 2, -2))
      .toDF("key", "value", "extra"),
    clauses = update(set = "extra = s.value") :: Nil,
    expected = ((0, 0, null) +: (1, 10, 1) +: (3, 30, null) +: Nil)
      .asInstanceOf[List[(Integer, Integer, Integer)]]
      .toDF("key", "value", "extra"),
    expectErrorWithoutEvolutionContains = "cannot resolve extra in UPDATE clause")

  // Schema evolution with INSERT alone
  testEvolution("new column with insert values")(
    targetData = Seq((0, 0), (1, 10), (3, 30)).toDF("key", "value"),
    sourceData = Seq((1, 1, "extra1"), (2, 2, "extra2")).toDF("key", "value", "extra"),
    clauses = insert(values = "(key, value, extra) VALUES (s.key, s.value, s.extra)") :: Nil,
    expected = ((0, 0, null) +: (1, 10, null) +: (3, 30, null) +: (2, 2, "extra2") +: Nil)
      .toDF("key", "value", "extra"),
    expectErrorWithoutEvolutionContains = "cannot resolve extra in INSERT clause")

   testEvolution("new column inserted with value from existing column")(
    targetData = Seq((0, 0), (1, 10), (3, 30)).toDF("key", "value"),
    sourceData = Seq((1, 1, -1), (2, 2, -2))
      .toDF("key", "value", "extra"),
    clauses = insert(values = "(key, extra) VALUES (s.key, s.value)") :: Nil,
    expected = ((0, 0, null) +: (1, 10, null) +: (3, 30, null) +: (2, null, 2) +: Nil)
      .asInstanceOf[List[(Integer, Integer, Integer)]]
      .toDF("key", "value", "extra"),
    expectErrorWithoutEvolutionContains = "cannot resolve extra in INSERT clause")

  // Schema evolution (UPDATE) with two new columns in the source but only one added to the target.
  testEvolution("new column with update set and column not updated")(
    targetData = Seq((0, 0), (1, 10), (3, 30)).toDF("key", "value"),
    sourceData = Seq((1, 1, "extra1", "unused1"), (2, 2, "extra2", "unused2"))
      .toDF("key", "value", "extra", "unused"),
    clauses = update(set = "extra = s.extra") :: Nil,
    expected = ((0, 0, null) +: (1, 10, "extra1") +: (3, 30, null) +: Nil)
      .asInstanceOf[List[(Integer, Integer, String)]]
      .toDF("key", "value", "extra"),
    expectErrorWithoutEvolutionContains = "cannot resolve extra in UPDATE clause")

  testEvolution("new column updated from other new column")(
    targetData = Seq((0, 0), (1, 10), (3, 30)).toDF("key", "value"),
    sourceData = Seq((1, 1, "extra1", "unused1"), (2, 2, "extra2", "unused2"))
      .toDF("key", "value", "extra", "unused"),
    clauses = update(set = "extra = s.unused") :: Nil,
    expected = ((0, 0, null) +: (1, 10, "unused1") +: (3, 30, null) +: Nil)
      .asInstanceOf[List[(Integer, Integer, String)]]
      .toDF("key", "value", "extra"),
    expectErrorWithoutEvolutionContains = "cannot resolve extra in UPDATE clause")

  // Schema evolution (INSERT) with two new columns in the source but only one added to the target.
  testEvolution("new column with insert values and column not inserted")(
    targetData = Seq((0, 0), (1, 10), (3, 30)).toDF("key", "value"),
    sourceData = Seq((1, 1, "extra1", "unused1"), (2, 2, "extra2", "unused2"))
      .toDF("key", "value", "extra", "unused"),
    clauses = insert(values = "(key, extra) VALUES (s.key, s.extra)") :: Nil,
    expected = ((0, 0, null) +: (1, 10, null) +: (3, 30, null) +: (2, null, "extra2") +: Nil)
      .asInstanceOf[List[(Integer, Integer, String)]]
      .toDF("key", "value", "extra"),
    expectErrorWithoutEvolutionContains = "cannot resolve extra in INSERT clause")

  testEvolution("new column inserted from other new column")(
    targetData = Seq((0, 0), (1, 10), (3, 30)).toDF("key", "value"),
    sourceData = Seq((1, 1, "extra1", "unused1"), (2, 2, "extra2", "unused2"))
      .toDF("key", "value", "extra", "unused"),
    clauses = insert(values = "(key, extra) VALUES (s.key, s.unused)") :: Nil,
    expected = ((0, 0, null) +: (1, 10, null) +: (3, 30, null) +: (2, null, "unused2") +: Nil)
      .asInstanceOf[List[(Integer, Integer, String)]]
      .toDF("key", "value", "extra"),
    expectErrorWithoutEvolutionContains = "cannot resolve extra in INSERT clause")

  // Schema evolution with two new columns added by UPDATE and INSERT resp.
  testEvolution("new column added by insert and other new column added by update")(
    targetData = Seq((0, 0), (1, 10), (3, 30)).toDF("key", "value"),
    sourceData = Seq((1, 1, "extra1", "other1"), (2, 2, "extra2", "other2"))
      .toDF("key", "value", "extra", "other"),
    clauses = update(set = "extra = s.extra") ::
      insert(values = "(key, other) VALUES (s.key, s.other)") :: Nil,
    expected =
      ((0, 0, null, null) +:
       (1, 10, "extra1", null) +:
       (3, 30, null, null) +:
       (2, null, null, "other2") +: Nil)
      .asInstanceOf[List[(Integer, Integer, String, String)]]
      .toDF("key", "value", "extra", "other"),
    expectErrorWithoutEvolutionContains = "cannot resolve extra in UPDATE clause")

  testEvolution("new column with insert existing column")(
    targetData = Seq((0, 0), (1, 10), (3, 30)).toDF("key", "value"),
    sourceData = Seq((1, 1, "extra1"), (2, 2, "extra2")).toDF("key", "value", "extra"),
    clauses = insert(values = "(key) VALUES (s.key)") :: Nil,
    expected = ((0, 0) +: (1, 10) +: (2, null) +: (3, 30) +: Nil)
      .asInstanceOf[List[(Integer, Integer)]]
      .toDF("key", "value"),
    expectedWithoutEvolution = ((0, 0) +: (1, 10) +: (2, null) +: (3, 30) +: Nil)
      .asInstanceOf[List[(Integer, Integer)]]
      .toDF("key", "value"))

  testEvolution("new column with update set and update *")(
    targetData = Seq((0, 0), (1, 10), (2, 20)).toDF("key", "value"),
    sourceData = Seq((1, 1, "extra1"), (2, 2, "extra2")).toDF("key", "value", "extra"),
    clauses = update(condition = "s.key < 2", set = "value = s.value") :: update("*") :: Nil,
    expected =
      ((0, 0, null) +:
        (1, 1, null) +: // updated by first clause
        (2, 2, "extra2") +: // updated by second clause
        Nil
      ).toDF("key", "value", "extra"),
    expectedWithoutEvolution = ((0, 0) +: (1, 1) +: (2, 2) +: Nil).toDF("key", "value")
  )

  testEvolution("new column with update non-* and insert *")(
    targetData = Seq((0, 0), (1, 10), (3, 30)).toDF("key", "value"),
    sourceData = Seq((1, 1, 1), (2, 2, 2)).toDF("key", "value", "extra"),
    clauses = update("key = s.key, value = s.value") :: insert("*") :: Nil,
    expected = ((0, 0, null) +: (2, 2, 2) +: (3, 30, null) +:
      // null because `extra` isn't an update action, even though it's 1 in the source data
      (1, 1, null) +: Nil)
      .asInstanceOf[List[(Integer, Integer, Integer)]].toDF("key", "value", "extra"),
    expectedWithoutEvolution = ((0, 0) +: (2, 2) +: (3, 30) +: (1, 1) +: Nil).toDF("key", "value")
  )

  testEvolution("new column with update * and insert non-*")(
    targetData = Seq((0, 0), (1, 10), (3, 30)).toDF("key", "value"),
    sourceData = Seq((1, 1, 1), (2, 2, 2)).toDF("key", "value", "extra"),
    clauses = update("*") :: insert("(key, value) VALUES (s.key, s.value)") :: Nil,
    expected = ((0, 0, null) +: (1, 1, 1) +: (3, 30, null) +:
      // null because `extra` isn't an insert action, even though it's 2 in the source data
      (2, 2, null) +: Nil)
      .asInstanceOf[List[(Integer, Integer, Integer)]].toDF("key", "value", "extra"),
    expectedWithoutEvolution = ((0, 0) +: (2, 2) +: (3, 30) +: (1, 1) +: Nil).toDF("key", "value")
  )

  testEvolution("evolve partitioned table")(
    targetData = Seq((0, 0), (1, 10), (3, 30)).toDF("key", "value"),
    sourceData = Seq((1, 1, "extra1"), (2, 2, "extra2")).toDF("key", "value", "extra"),
    clauses = update("*") :: insert("*") :: Nil,
    expected = ((0, 0, null) +: (1, 1, "extra1") +: (2, 2, "extra2") +: (3, 30, null) +: Nil)
      .toDF("key", "value", "extra"),
    expectedWithoutEvolution = ((0, 0) +: (2, 2) +: (3, 30) +: (1, 1) +: Nil).toDF("key", "value")
  )

  testEvolution("star expansion with names including dots")(
    targetData = Seq((0, 0), (1, 10), (3, 30)).toDF("key", "value.with.dotted.name"),
    sourceData = Seq((1, 1, "extra1"), (2, 2, "extra2")).toDF(
      "key", "value.with.dotted.name", "extra.dotted"),
    clauses = update("*") :: insert("*") :: Nil,
    expected = ((0, 0, null) +: (1, 1, "extra1") +: (2, 2, "extra2") +: (3, 30, null) +: Nil)
      .toDF("key", "value.with.dotted.name", "extra.dotted"),
    expectedWithoutEvolution = ((0, 0) +: (2, 2) +: (3, 30) +: (1, 1) +: Nil)
      .toDF("key", "value.with.dotted.name")
  )

  testEvolution("extra nested column in source - insert")(
    targetData = Seq((1, (1, 10))).toDF("key", "x"),
    sourceData = Seq((2, (2, 20, 30))).toDF("key", "x"),
    clauses = insert("*") :: Nil,
    expected = ((1, (1, 10, null)) +: (2, (2, 20, 30)) +: Nil)
      .asInstanceOf[List[(Integer, (Integer, Integer, Integer))]].toDF("key", "x"),
    expectErrorWithoutEvolutionContains = "Cannot cast"
  )

  testEvolution("add non-nullable column to target schema")(
    targetData = Seq(1, 2).toDF("key"),
    sourceData = Seq((1, 10), (3, 30)).toDF("key", "value"),
    clauses = update("*") :: insert("*") :: Nil,
    expected = ((1, 10) :: (2, null) :: (3, 30) :: Nil)
      .asInstanceOf[List[(Integer, Integer)]].toDF("key", "value"),
    expectedSchema = new StructType()
      .add("key", IntegerType)
      .add("value", IntegerType, nullable = true),
    expectedWithoutEvolution = Seq(1, 2, 3).toDF("key")
  )

  testEvolution("extra nested column in source - update - single target partition")(
    targetData = Seq((1, (1, 10)), (2, (2, 2000))).toDF("key", "x")
      .selectExpr("key", "named_struct('a', x._1, 'c', x._2) as x").repartition(1),
    sourceData = Seq((1, (10, 100, 1000))).toDF("key", "x")
      .selectExpr("key", "named_struct('a', x._1, 'b', x._2, 'c', x._3) as x"),
    clauses = update("*") :: Nil,
    expected = ((1, (10, 100, 1000)) +: (2, (2, null, 2000)) +: Nil)
      .asInstanceOf[List[(Integer, (Integer, Integer, Integer))]].toDF("key", "x")
      .selectExpr("key", "named_struct('a', x._1, 'c', x._3, 'b', x._2) as x"),
    expectErrorWithoutEvolutionContains = "Cannot cast"
  )

  testEvolution("multiple clauses")(
    // 1 and 2 should be updated from the source, 3 and 4 should be deleted. Only 5 is unchanged
    targetData = Seq((1, "a"), (2, "b"), (3, "c"), (4, "d"), (5, "e")).toDF("key", "targetVal"),
    // 1 and 2 should be updated into the target, 6 and 7 should be inserted. 8 should be ignored
    sourceData = Seq((1, "t"), (2, "u"), (3, "v"), (4, "w"), (6, "x"), (7, "y"), (8, "z"))
      .toDF("key", "srcVal"),
    clauses =
      update("targetVal = srcVal", "s.key = 1") :: update("*", "s.key = 2") ::
      delete("s.key = 3") :: delete("s.key = 4") ::
      insert("(key) VALUES (s.key)", "s.key = 6") :: insert("*", "s.key = 7") :: Nil,
    expected =
      ((1, "t", null) :: (2, "b", "u") :: (5, "e", null) ::
        (6, null, null) :: (7, null, "y") :: Nil)
        .asInstanceOf[List[(Integer, String, String)]].toDF("key", "targetVal", "srcVal"),
    // The UPDATE * clause won't resolve without evolution because the source and target columns
    // don't match.
    expectErrorWithoutEvolutionContains = "cannot resolve targetVal"
  )

  testEvolution("multiple INSERT * clauses with UPDATE")(
    // 1 and 2 should be updated from the source, 3 and 4 should be deleted. Only 5 is unchanged
    targetData = Seq((1, "a"), (2, "b"), (3, "c"), (4, "d"), (5, "e")).toDF("key", "targetVal"),
    // 1 and 2 should be updated into the target, 6 and 7 should be inserted. 8 should be ignored
    sourceData = Seq((1, "t"), (2, "u"), (3, "v"), (4, "w"), (6, "x"), (7, "y"), (8, "z"))
      .toDF("key", "srcVal"),
    clauses =
      update("targetVal = srcVal", "s.key = 1") :: update("*", "s.key = 2") ::
      delete("s.key = 3") :: delete("s.key = 4") ::
      insert("*", "s.key = 6") :: insert("*", "s.key = 7") :: Nil,
    expected =
      ((1, "t", null) :: (2, "b", "u") :: (5, "e", null) ::
        (6, null, "x") :: (7, null, "y") :: Nil)
        .asInstanceOf[List[(Integer, String, String)]].toDF("key", "targetVal", "srcVal"),
    // The UPDATE * clause won't resolve without evolution because the source and target columns
    // don't match.
    expectErrorWithoutEvolutionContains = "cannot resolve targetVal"
  )

  testEvolution("array of struct should work with containsNull as false")(
    targetData = Seq(500000).toDF("key"),
    sourceData = Seq(500000, 100000).toDF("key")
      .withColumn("generalDeduction",
        struct(
          lit("2024-11-08").cast(DateType).as("date"),
          array(struct(lit(0d).as("data"))))),
    clauses = update("*") :: insert("*") :: Nil,
    expected = Seq(500000, 100000).toDF("key")
      .withColumn("generalDeduction",
        struct(
          lit("2024-11-08").cast(DateType).as("date"),
          array(struct(lit(0d).as("data"))))),
    expectedWithoutEvolution = Seq(500000, 100000).toDF("key")
  )

  testEvolution("test array_union with schema evolution")(
    targetData = Seq(1).toDF("key")
      .withColumn("openings",
        array(
          (2010 to 2019).map { i =>
            struct(
              lit(s"$i-01-19T09:29:00.000+0000").as("opened_at"),
              lit(null).cast(StringType).as("opened_with"),
              lit(s"$i").as("location")
            )
          }: _*)),
    sourceData = Seq(1).toDF("key")
      .withColumn("openings",
        array(
          (2020 to 8020).map { i =>
            struct(
              lit(null).cast(StringType).as("opened_with"),
              lit(s"$i-01-19T09:29:00.000+0000").as("opened_at")
            )
          }: _*)),
    clauses = update(set = "openings = array_union(s.openings, s.openings)") :: insert("*") :: Nil,
    expected = Seq(1).toDF("key")
      .withColumn("openings",
        array(
          (2020 to 8020).map { i =>
            struct(
              lit(s"$i-01-19T09:29:00.000+0000").as("opened_at"),
              lit(null).cast(StringType).as("opened_with"),
              lit(null).cast(StringType).as("location")
            )
          }: _*)),
    expectErrorWithoutEvolutionContains = "All nested columns must match"
  )

  testEvolution("test array_intersect with schema evolution")(
    targetData = Seq(1).toDF("key")
      .withColumn("openings",
        array(
          (2010 to 2019).map { i =>
            struct(
              lit(s"$i-01-19T09:29:00.000+0000").as("opened_at"),
              lit(null).cast(StringType).as("opened_with"),
              lit(s"$i").as("location")
            )
          }: _*)),
    sourceData = Seq(1).toDF("key")
      .withColumn("openings",
        array(
          (2020 to 8020).map { i =>
            struct(
              lit(null).cast(StringType).as("opened_with"),
              lit(s"$i-01-19T09:29:00.000+0000").as("opened_at")
            )
          }: _*)),
    clauses =
      update(set = "openings = array_intersect(s.openings, s.openings)") :: insert("*") :: Nil,
    expected = Seq(1).toDF("key")
      .withColumn("openings",
        array(
          (2020 to 8020).map { i =>
            struct(
              lit(s"$i-01-19T09:29:00.000+0000").as("opened_at"),
              lit(null).cast(StringType).as("opened_with"),
              lit(null).cast(StringType).as("location")
            )
          }: _*)),
    expectErrorWithoutEvolutionContains = "All nested columns must match"
  )

  testEvolution("test array_except with schema evolution")(
    targetData = Seq(1).toDF("key")
      .withColumn("openings",
        array(
          (2010 to 2020).map { i =>
            struct(
              lit(s"$i-01-19T09:29:00.000+0000").as("opened_at"),
              lit(null).cast(StringType).as("opened_with"),
              lit(s"$i").as("location")
            )
          }: _*)),
    sourceData = Seq(1).toDF("key")
      .withColumn("openings",
        array(
          (2020 to 8020).map { i =>
            struct(
              lit(null).cast(StringType).as("opened_with"),
              lit(s"$i-01-19T09:29:00.000+0000").as("opened_at")
            )
          }: _*)),
    clauses =
      update(set = "openings = array_except(s.openings, s.openings)") :: insert("*") :: Nil,
    expected = Seq(1).toDF("key")
      .withColumn(
        "openings",
        array().cast(
          new ArrayType(
            new StructType()
              .add("opened_at", StringType)
              .add("opened_with", StringType)
              .add("location", StringType),
            true
          )
        )
      ),
    expectErrorWithoutEvolutionContains = "All nested columns must match"
  )

  testEvolution("test array_remove with schema evolution")(
    targetData = Seq(1).toDF("key")
      .withColumn("openings",
        array(
          (2010 to 2019).map { i =>
            struct(
              lit(s"$i-01-19T09:29:00.000+0000").as("opened_at"),
              lit(null).cast(StringType).as("opened_with"),
              lit(s"$i").as("location")
            )
          }: _*)),
    sourceData = Seq(1).toDF("key")
      .withColumn("openings",
        array(
          (2020 to 8020).map { i =>
            struct(
              lit(null).cast(StringType).as("opened_with"),
              lit(s"$i-01-19T09:29:00.000+0000").as("opened_at")
            )
          }: _*)),
    clauses = update(
      set = "openings = array_remove(s.openings," +
        "named_struct('opened_with', cast(null as string)," +
        "'opened_at', '2020-01-19T09:29:00.000+0000'))") :: insert("*") :: Nil,
    expected = Seq(1).toDF("key")
      .withColumn(
        "openings",
        array((2021 to 8020).map { i =>
          struct(
            lit(s"$i-01-19T09:29:00.000+0000").as("opened_at"),
            lit(null).cast(StringType).as("opened_with"),
            lit(null).cast(StringType).as("location")
          )
        }: _*)),
    expectErrorWithoutEvolutionContains = "All nested columns must match"
  )

  testEvolution("test array_distinct with schema evolution")(
    targetData = Seq(1).toDF("key")
      .withColumn("openings",
        array(
          (2010 to 2019).map { i =>
            struct(
              lit(s"$i-01-19T09:29:00.000+0000").as("opened_at"),
              lit(null).cast(StringType).as("opened_with"),
              lit(s"$i").as("location")
            )
          }: _*
        )),
    sourceData = Seq(1).toDF("key")
      .withColumn("openings",
        array(
          ((2020 to 8020) ++ (2020 to 8020)).map { i =>
            struct(
              lit(null).cast(StringType).as("opened_with"),
              lit(s"$i-01-19T09:29:00.000+0000").as("opened_at")
            )
          }: _*
        )),
    clauses = update(set = "openings = array_distinct(s.openings)") :: insert("*") :: Nil,
    expected = Seq(1).toDF("key")
      .withColumn(
        "openings",
        array((2020 to 8020).map { i =>
          struct(
            lit(s"$i-01-19T09:29:00.000+0000").as("opened_at"),
            lit(null).cast(StringType).as("opened_with"),
            lit(null).cast(StringType).as("location")
          )
        }: _*)),
    expectErrorWithoutEvolutionContains = "All nested columns must match"
  )

  testEvolution("void columns are not allowed")(
    targetData = Seq((1, 1)).toDF("key", "value"),
    sourceData = Seq((1, 100, null), (2, 200, null)).toDF("key", "value", "extra"),
    clauses = update("*") :: insert("*") :: Nil,
    expectErrorContains = "Cannot add column `extra` with type VOID",
    expectedWithoutEvolution = Seq((1, 100), (2, 200)).toDF("key", "value")
  )

  testEvolution("top-level column assignment qualified with source alias")(
    targetData = Seq((0, 0), (1, 10), (3, 30)).toDF("key", "value"),
    sourceData = Seq((1, 1, "extra1"), (2, 2, "extra2")).toDF("key", "value", "extra"),
    clauses = update(set = "s.value = s.value") :: Nil,
    // Assigning to the source is just wrong and should fail.
    expected = ((0, 0) +: (3, 30) +: (1, 1) +: Nil)
      .toDF("key", "value"),
    expectErrorWithoutEvolutionContains = "cannot resolve s.value in UPDATE clause")

  test("schema evolution enabled for the current command") {
    withSQLConf(DeltaSQLConf.DELTA_SCHEMA_AUTO_MIGRATE.key -> "false") {
      withTable("target", "source") {
        Seq((0, 0), (1, 10), (3, 30)).toDF("key", "value")
          .write.format("delta").saveAsTable("target")
        Seq((1, 1, 1), (2, 2, 2)).toDF("key", "value", "extra")
          .write.format("delta").saveAsTable("source")

        // Should fail without schema evolution
        val e = intercept[org.apache.spark.sql.AnalysisException] {
          executeMerge(
            "target",
            "source",
            "target.key = source.key",
            update("extra = -1"), insert("*"))
        }
        assert(e.getErrorClass === "DELTA_MERGE_UNRESOLVED_EXPRESSION")
        assert(e.getMessage.contains("resolve extra in UPDATE clause"))

        // Should succeed with schema evolution
        executeMergeWithSchemaEvolution(
          "target",
          "source",
          "target.key = source.key",
          update("extra = -1"), insert("*"))
        checkAnswer(
          spark.table("target"),
          Seq[(Integer, Integer, Integer)]((0, 0, null), (1, 10, -1), (2, 2, 2), (3, 30, null))
            .toDF("key", "value", "extra"))
      }
    }
  }

  testEvolutionWithoutTableAliases("new top-level column assignment qualified with target name")(
    targetData = Seq((0, 1)).toDF("a", "nested_a")
        .selectExpr("a", "named_struct('a', nested_a) as target"),
    sourceData = Seq((2, 3, 4, 5)).toDF("a", "b", "nested_a", "nested_b")
        .selectExpr("a", "b", "named_struct('a', nested_a, 'b', nested_b) as target"),
    clauses = update("target.b = source.b"))(
    expected = Seq(Row(0, Row(1, 3))),
    expectErrorWithoutEvolutionContains = "No such struct field `b` in `a")

  testEvolutionWithoutTableAliases("new nested field assignment qualified with target name")(
    targetData = Seq((0, 1)).toDF("a", "nested_a")
        .selectExpr("a", "named_struct('a', nested_a) as target"),
    sourceData = Seq((2, 3, 4, 5)).toDF("a", "b", "nested_a", "nested_b")
        .selectExpr("a", "b", "named_struct('a', nested_a, 'b', nested_b) as target"),
    clauses = update("target.target.b = source.target.b"))(
    // target.target.b gets resolved to source struct target, accessing nested field target.target.b
    // which doesn't exist.
    expectErrorContains = "No such struct field `target` in `a`, `b`",
    // target.target.b: target.target gets resolved to target table 'target' column with nested
    // field b which doesn't exist.
    expectErrorWithoutEvolutionContains = "No such struct field `b` in `a`")
}

/**
 * Trait collecting all base and existing column tests for schema evolution.
 */
trait MergeIntoSchemaEvolutionBaseExistingColumnTests extends MergeIntoSchemaEvolutionMixin {
  self: MergeIntoTestUtils
    with SharedSparkSession =>

  import testImplicits._

  // No schema evolution
  testEvolution("old column updated from new column")(
    targetData = Seq((0, 0), (1, 10), (3, 30)).toDF("key", "value"),
    sourceData = Seq((1, 1, -1), (2, 2, -2))
      .toDF("key", "value", "extra"),
    clauses = update(set = "value = s.extra") :: Nil,
    expected = ((0, 0) +: (1, -1) +: (3, 30) +: Nil).toDF("key", "value"),
    expectedWithoutEvolution = ((0, 0) +: (1, -1) +: (3, 30) +: Nil).toDF("key", "value"))

  testEvolution("old column inserted from new column")(
    targetData = Seq((0, 0), (1, 10), (3, 30)).toDF("key", "value"),
    sourceData = Seq((1, 1, -1), (2, 2, -2))
      .toDF("key", "value", "extra"),
    clauses = insert(values = "(key) VALUES (s.extra)") :: Nil,
    expected = ((0, 0) +: (1, 10) +: (3, 30) +: (-2, null) +: Nil)
      .asInstanceOf[List[(Integer, Integer)]]
      .toDF("key", "value"),
    expectedWithoutEvolution = ((0, 0) +: (1, 10) +: (3, 30) +: (-2, null) +: Nil)
      .asInstanceOf[List[(Integer, Integer)]]
      .toDF("key", "value"))

  // Column doesn't exist with UPDATE/INSERT alone.
  testEvolution("update set nonexistent column")(
    targetData = Seq((0, 0), (1, 10), (3, 30)).toDF("key", "value"),
    sourceData = Seq((1, 1, "extra1"), (2, 2, "extra2")).toDF("key", "value", "extra"),
    clauses = update(set = "nonexistent = s.extra") :: Nil,
    expectErrorContains = "cannot resolve nonexistent in UPDATE clause",
    expectErrorWithoutEvolutionContains = "cannot resolve nonexistent in UPDATE clause")

  testEvolution("insert values nonexistent column")(
    targetData = Seq((0, 0), (1, 10), (3, 30)).toDF("key", "value"),
    sourceData = Seq((1, 1, "extra1"), (2, 2, "extra2")).toDF("key", "value", "extra"),
    clauses = insert(values = "(nonexistent) VALUES (s.extra)") :: Nil,
    expectErrorContains = "cannot resolve nonexistent in INSERT clause",
    expectErrorWithoutEvolutionContains = "cannot resolve nonexistent in INSERT clause")

  testEvolution("update * with column not in source")(
    targetData = Seq((0, 0, 0), (1, 10, 10), (3, 30, 30)).toDF("key", "value", "extra"),
    sourceData = Seq((1, 1), (2, 2)).toDF("key", "value"),
    clauses = update("*") :: Nil,
    // update went through even though `extra` wasn't there
    expected = ((0, 0, 0) +: (1, 1, 10) +: (3, 30, 30) +: Nil).toDF("key", "value", "extra"),
    expectErrorWithoutEvolutionContains = "cannot resolve extra in UPDATE clause"
  )

  testEvolution("insert * with column not in source")(
    targetData = Seq((0, 0, 0), (1, 10, 10), (3, 30, 30)).toDF("key", "value", "extra"),
    sourceData = Seq((1, 1), (2, 2)).toDF("key", "value"),
    clauses = insert("*") :: Nil,
    // insert went through even though `extra` wasn't there
    expected = ((0, 0, 0) +: (1, 10, 10) +: (2, 2, null) +: (3, 30, 30) +: Nil)
      .asInstanceOf[List[(Integer, Integer, Integer)]]
      .toDF("key", "value", "extra"),
    expectErrorWithoutEvolutionContains = "cannot resolve extra in INSERT clause"
  )

  testEvolution("explicitly insert subset of columns")(
    targetData = Seq((0, 0, 0), (1, 10, 10), (3, 30, 30)).toDF("key", "value", "extra"),
    sourceData = Seq((1, 1, 1), (2, 2, 2)).toDF("key", "value", "extra"),
    clauses = insert("(key, value) VALUES (s.key, s.value)") :: Nil,
    // 2 should have extra = null, since extra wasn't in the insert spec.
    expected = ((0, 0, 0) +: (1, 10, 10) +: (2, 2, null) +: (3, 30, 30) +: Nil)
      .asInstanceOf[List[(Integer, Integer, Integer)]]
      .toDF("key", "value", "extra"),
    expectedWithoutEvolution = ((0, 0, 0) +: (1, 10, 10) +: (2, 2, null) +: (3, 30, 30) +: Nil)
      .asInstanceOf[List[(Integer, Integer, Integer)]]
      .toDF("key", "value", "extra")
  )

  testEvolution("explicitly update one column")(
    targetData = Seq((0, 0), (1, 10), (3, 30)).toDF("key", "value"),
    sourceData = Seq((1, 1, 1), (2, 2, 2)).toDF("key", "value", "extra"),
    clauses = update("value = s.value") :: Nil,
    // Both results should be the same - we're checking that no evolution logic triggers
    // even though there's an extra source column.
    expected = ((0, 0) +: (1, 1) +: (3, 30) +: Nil).toDF("key", "value"),
    expectedWithoutEvolution = ((0, 0) +: (1, 1) +: (3, 30) +: Nil).toDF("key", "value")
  )

  testEvolution(s"case-insensitive insert")(
    targetData = Seq((0, 0), (1, 10), (3, 30)).toDF("key", "value"),
    sourceData = Seq((1, 1), (2, 2)).toDF("key", "VALUE"),
    clauses = insert("(key, value, VALUE) VALUES (s.key, s.value, s.VALUE)") :: Nil,
    expected = ((0, 0) +: (1, 10) +: (3, 30) +: (2, 2) +: Nil).toDF("key", "value"),
    expectedWithoutEvolution = ((0, 0) +: (1, 10) +: (3, 30) +: (2, 2) +: Nil).toDF("key", "value"),
    confs = Seq(SQLConf.CASE_SENSITIVE.key -> "false")
  )

  testEvolution("case-sensitive insert, column not in target")(
    targetData = Seq((0, 0), (1, 10), (3, 30)).toDF("key", "value"),
    sourceData = Seq((1, 1, "extra1"), (2, 2, "extra2")).toDF("key", "value", "EXTRA"),
    clauses = insert("(key, value, EXTRA) VALUES (s.key, s.value, s.EXTRA)") :: Nil,
    // In case-sensitive mode, EXTRA is a new column distinct from any lowercase variant.
    expected = ((0, 0, null) +: (1, 10, null) +: (3, 30, null) +: (2, 2, "extra2") +: Nil)
      .asInstanceOf[List[(Integer, Integer, String)]]
      .toDF("key", "value", "EXTRA"),
    expectErrorWithoutEvolutionContains = "cannot resolve EXTRA in INSERT clause",
    confs = Seq(SQLConf.CASE_SENSITIVE.key -> "true")
  )

  testEvolution("case-sensitive insert, column not in source")(
    targetData = Seq((0, 0), (1, 10), (3, 30)).toDF("key", "value"),
    sourceData = Seq((1, 1), (2, 2)).toDF("key", "VALUE"),
    clauses = insert("(key, value) VALUES (s.key, s.value)") :: Nil,
    expectErrorContains = "Cannot resolve s.value in INSERT clause",
    expectErrorWithoutEvolutionContains = "Cannot resolve s.value in INSERT clause",
    confs = Seq(SQLConf.CASE_SENSITIVE.key -> "true")
  )

  // Note that incompatible types are those where a cast to the target type can't resolve - any
  // valid cast will be permitted.
  testEvolution("incompatible types in update *")(
    targetData = Seq((0, 0), (1, 10), (3, 30)).toDF("key", "value"),
    sourceData = Seq((1, Array[Byte](1)), (2, Array[Byte](2))).toDF("key", "value"),
    clauses = update("*") :: Nil,
    expectErrorContains =
      "Failed to merge incompatible data types IntegerType and BinaryType",
    expectErrorWithoutEvolutionContains = "cannot cast"
  )

  testEvolution("incompatible types in insert *")(
    targetData = Seq((0, 0), (1, 10), (3, 30)).toDF("key", "value"),
    sourceData = Seq((1, Array[Byte](1)), (2, Array[Byte](2))).toDF("key", "value"),
    clauses = insert("*") :: Nil,
    expectErrorContains = "Failed to merge incompatible data types IntegerType and BinaryType",
    expectErrorWithoutEvolutionContains = "cannot cast"
  )

  // All integral types other than long can be upcasted to integer.
  testEvolution("upcast numeric source types into integer target")(
    targetData = Seq((0, 0), (1, 10), (3, 30)).toDF("key", "value"),
    sourceData = Seq((1.toByte, 1.toShort), (2.toByte, 2.toShort)).toDF("key", "value"),
    clauses = update("*") :: insert("*") :: Nil,
    expected = Seq((0, 0), (1, 1), (2, 2), (3, 30)).toDF("key", "value"),
    expectedWithoutEvolution = Seq((0, 0), (1, 1), (2, 2), (3, 30)).toDF("key", "value")
  )

  // Delta's automatic schema evolution allows converting table columns with a numeric type narrower
  // than integer to integer, because in the underlying Parquet they're all stored as ints.
  testEvolution("upcast numeric target types from integer source")(
    targetData = Seq((0.toByte, 0.toShort), (1.toByte, 10.toShort)).toDF("key", "value"),
    sourceData = Seq((1, 1), (2, 2)).toDF("key", "value"),
    clauses = update("*") :: insert("*") :: Nil,
    expected =
      ((0.toByte, 0.toShort) +:
        (1.toByte, 1.toShort) +:
        (2.toByte, 2.toShort) +: Nil
        ).toDF("key", "value"),
    expectedWithoutEvolution =
      ((0.toByte, 0.toShort) +:
        (1.toByte, 1.toShort) +:
        (2.toByte, 2.toShort) +: Nil
        ).toDF("key", "value")
  )

  testEvolution("upcast int source type into long target")(
    targetData = Seq((0, 0L), (1, 10L), (3, 30L)).toDF("key", "value"),
    sourceData = Seq((1, 1), (2, 2)).toDF("key", "value"),
    clauses = update("*") :: insert("*") :: Nil,
    expected = ((0, 0L) +: (1, 1L) +: (2, 2L) +: (3, 30L) +: Nil).toDF("key", "value"),
    expectedWithoutEvolution =
      ((0, 0L) +: (1, 1L) +: (2, 2L) +: (3, 30L) +: Nil).toDF("key", "value")
  )

  testEvolution("write string into int column")(
    targetData = Seq((0, 0), (1, 10), (3, 30)).toDF("key", "value"),
    sourceData = Seq((1, "1"), (2, "2"), (5, "notANumber")).toDF("key", "value"),
    clauses = insert("*") :: Nil,
    expected = ((0, 0) +: (1, 10) +: (2, 2) +: (3, 30) +: (5, null) +: Nil)
      .asInstanceOf[List[(Integer, Integer)]].toDF("key", "value"),
    expectedWithoutEvolution =
      ((0, 0) +: (1, 10) +: (2, 2) +: (3, 30) +: (5, null) +: Nil)
        .asInstanceOf[List[(Integer, Integer)]].toDF("key", "value"),
    // Disable ANSI as this test needs to cast string "notANumber" to int
    confs = Seq(SQLConf.STORE_ASSIGNMENT_POLICY.key -> "LEGACY")
  )

  // This is kinda bug-for-bug compatibility. It doesn't really make sense that infinity is casted
  // to int as Int.MaxValue, but that's the behavior.
  testEvolution("write double into int column")(
    targetData = Seq((0, 0), (1, 10), (3, 30)).toDF("key", "value"),
    sourceData = Seq((1, 1.1), (2, 2.2), (5, Double.PositiveInfinity)).toDF("key", "value"),
    clauses = insert("*") :: Nil,
    expected =
      ((0, 0) +: (1, 10) +: (2, 2) +: (3, 30) +: (5, Int.MaxValue) +: Nil)
        .asInstanceOf[List[(Integer, Integer)]].toDF("key", "value"),
    expectedWithoutEvolution =
      ((0, 0) +: (1, 10) +: (2, 2) +: (3, 30) +: (5, Int.MaxValue) +: Nil)
        .asInstanceOf[List[(Integer, Integer)]].toDF("key", "value"),
    // Disable ANSI as this test needs to cast Double.PositiveInfinity to int
    confs = Seq(SQLConf.STORE_ASSIGNMENT_POLICY.key -> "LEGACY")
  )

  testEvolution("missing nested column in source - insert")(
    targetData = Seq((1, (1, 2, 3))).toDF("key", "x"),
    sourceData = Seq((2, (2, 3))).toDF("key", "x"),
    clauses = insert("*") :: Nil,
    expected = ((1, (1, 2, 3)) +: (2, (2, 3, null)) +: Nil)
      .asInstanceOf[List[(Integer, (Integer, Integer, Integer))]].toDF("key", "x"),
    expectErrorWithoutEvolutionContains = "Cannot cast"
  )

  testEvolution("missing nested column resolved by name - insert")(
    targetData = Seq((1, 1, 2, 3)).toDF("key", "a", "b", "c")
      .selectExpr("key", "named_struct('a', a, 'b', b, 'c', c) as x"),
    sourceData = Seq((2, 2, 4)).toDF("key", "a", "c")
      .selectExpr("key", "named_struct('a', a, 'c', c) as x"),
    clauses = insert("*") :: Nil,
    expected = ((1, (1, 2, 3)) +: (2, (2, null, 4)) +: Nil)
      .asInstanceOf[List[(Integer, (Integer, Integer, Integer))]].toDF("key", "x")
      .selectExpr("key", "named_struct('a', x._1, 'b', x._2, 'c', x._3) as x"),
    expectErrorWithoutEvolutionContains = "Cannot cast"
  )

  testEvolutionWithoutTableAliases(
    "existing top-level column assignment qualified with target name")(
    targetData = Seq((0, 1)).toDF("a", "nested_a")
        .selectExpr("a", "named_struct('a', nested_a) as target"),
    sourceData = Seq((2, 3)).toDF("a", "nested_a")
        .selectExpr("a", "named_struct('a', nested_a) as target"),
    clauses = update("target.a = source.a"))(
    expected = Seq(Row(2, Row(1))))

  testEvolutionWithoutTableAliases("existing nested field assignment qualified with target name")(
    targetData = Seq((0, 1)).toDF("a", "nested_a")
        .selectExpr("a", "named_struct('a', nested_a) as target"),
    sourceData = Seq((2, 3)).toDF("a", "nested_a")
        .selectExpr("a", "named_struct('a', nested_a) as target"),
    clauses = update("target.target.a = source.target.a"))(
    expected = Seq(Row(0, Row(3))))
}

trait MergeIntoSchemaEvoStoreAssignmentPolicyTests extends MergeIntoSchemaEvolutionMixin {
  self: MergeIntoTestUtils with SharedSparkSession =>
  import testImplicits._

  // Upcasting is always allowed.
  for (storeAssignmentPolicy <- StoreAssignmentPolicy.values)
  testEvolution("upcast int source type into long target, storeAssignmentPolicy = " +
    s"$storeAssignmentPolicy")(
    targetData = Seq((0, 0L), (1, 1L), (3, 3L)).toDF("key", "value"),
    sourceData = Seq((1, 1), (2, 2)).toDF("key", "value"),
    clauses = update("*") :: insert("*") :: Nil,
    expected =
      ((0, 0L) +: (1, 1L) +: (2, 2L) +: (3, 3L) +: Nil).toDF("key", "value"),
    expectedWithoutEvolution =
      ((0, 0L) +: (1, 1L) +: (2, 2L) +: (3, 3L) +: Nil).toDF("key", "value"),
    confs = Seq(
      SQLConf.STORE_ASSIGNMENT_POLICY.key -> storeAssignmentPolicy.toString,
      DeltaSQLConf.UPDATE_AND_MERGE_CASTING_FOLLOWS_ANSI_ENABLED_FLAG.key -> "false")
  )

  // Casts that are not valid implicit casts (e.g. string -> boolean) are never allowed with
  // schema evolution enabled and allowed only when storeAssignmentPolicy is LEGACY or ANSI when
  // schema evolution is disabled.
  for (storeAssignmentPolicy <- StoreAssignmentPolicy.values - StoreAssignmentPolicy.STRICT)
  testEvolution("invalid implicit cast string source type into boolean target, " +
    s"storeAssignmentPolicy = $storeAssignmentPolicy")(
    targetData = Seq((0, true), (1, false), (3, true)).toDF("key", "value"),
    sourceData = Seq((1, "true"), (2, "false")).toDF("key", "value"),
    clauses = update("*") :: insert("*") :: Nil,
    expectErrorContains = "Failed to merge incompatible data types BooleanType and StringType",
    expectedWithoutEvolution = ((0, true) +: (1, true) +: (2, false) +: (3, true) +: Nil)
      .toDF("key", "value"),
    confs = Seq(
      SQLConf.STORE_ASSIGNMENT_POLICY.key -> storeAssignmentPolicy.toString,
      DeltaSQLConf.UPDATE_AND_MERGE_CASTING_FOLLOWS_ANSI_ENABLED_FLAG.key -> "false")
  )

  // Casts that are not valid implicit casts (e.g. string -> boolean) are not allowed with
  // storeAssignmentPolicy = STRICT.
  testEvolution("invalid implicit cast string source type into boolean target, " +
   s"storeAssignmentPolicy = ${StoreAssignmentPolicy.STRICT}")(
    targetData = Seq((0, true), (1, false), (3, true)).toDF("key", "value"),
    sourceData = Seq((1, "true"), (2, "false")).toDF("key", "value"),
    clauses = update("*") :: insert("*") :: Nil,
    expectErrorContains = "Failed to merge incompatible data types BooleanType and StringType",
    expectErrorWithoutEvolutionContains = "cannot up cast s.value from \"string\" to \"boolean\"",
    confs = Seq(
      SQLConf.STORE_ASSIGNMENT_POLICY.key -> StoreAssignmentPolicy.STRICT.toString,
      DeltaSQLConf.UPDATE_AND_MERGE_CASTING_FOLLOWS_ANSI_ENABLED_FLAG.key -> "false")
  )

  // Valid implicit casts that are not upcasts (e.g. string -> int) are allowed with
  // storeAssignmentPolicy = LEGACY or ANSI.
  for (storeAssignmentPolicy <- StoreAssignmentPolicy.values - StoreAssignmentPolicy.STRICT)
  testEvolution("valid implicit cast string source type into int target, " +
   s"storeAssignmentPolicy = ${storeAssignmentPolicy}")(
    targetData = Seq((0, 0), (1, 1), (3, 3)).toDF("key", "value"),
    sourceData = Seq((1, "1"), (2, "2")).toDF("key", "value"),
    clauses = update("*") :: insert("*") :: Nil,
    expected = ((0, 0)+: (1, 1) +: (2, 2) +: (3, 3)  +: Nil).toDF("key", "value"),
    expectedWithoutEvolution = ((0, 0) +: (1, 1) +: (2, 2) +: (3, 3) +: Nil).toDF("key", "value"),
    confs = Seq(
      SQLConf.STORE_ASSIGNMENT_POLICY.key -> storeAssignmentPolicy.toString,
      DeltaSQLConf.UPDATE_AND_MERGE_CASTING_FOLLOWS_ANSI_ENABLED_FLAG.key -> "false")
  )

  for (storeAssignmentPolicy <- StoreAssignmentPolicy.values - StoreAssignmentPolicy.STRICT)
  testEvolution("valid implicit cast long source type into int target, " +
   s"storeAssignmentPolicy = $storeAssignmentPolicy")(
    targetData = Seq((0, 0), (1, 1), (3, 3)).toDF("key", "value"),
    sourceData = Seq((1, 1L), (2, 2L)).toDF("key", "value"),
    clauses = update("*") :: insert("*") :: Nil,
    expected = ((0, 0)+: (1, 1) +: (2, 2) +: (3, 3)  +: Nil).toDF("key", "value"),
    expectedWithoutEvolution = ((0, 0) +: (1, 1) +: (2, 2) +: (3, 3) +: Nil).toDF("key", "value"),
    confs = Seq(
      SQLConf.STORE_ASSIGNMENT_POLICY.key -> storeAssignmentPolicy.toString,
      DeltaSQLConf.UPDATE_AND_MERGE_CASTING_FOLLOWS_ANSI_ENABLED_FLAG.key -> "false")
  )

  // Valid implicit casts that are not upcasts (e.g. string -> int) are rejected with
  // storeAssignmentPolicy = STRICT.
  testEvolution("valid implicit cast string source type into int target, " +
   s"storeAssignmentPolicy = ${StoreAssignmentPolicy.STRICT}")(
    targetData = Seq((0, 0), (1, 1), (3, 3)).toDF("key", "value"),
    sourceData = Seq((1, "1"), (2, "2")).toDF("key", "value"),
    clauses = update("*") :: insert("*") :: Nil,
    expectErrorContains = "cannot up cast s.value from \"string\" to \"int\"",
    expectErrorWithoutEvolutionContains = "cannot up cast s.value from \"string\" to \"int\"",
    confs = Seq(
      SQLConf.STORE_ASSIGNMENT_POLICY.key -> StoreAssignmentPolicy.STRICT.toString,
      DeltaSQLConf.UPDATE_AND_MERGE_CASTING_FOLLOWS_ANSI_ENABLED_FLAG.key -> "false")
  )

  testEvolution("multiple casts with storeAssignmentPolicy = STRICT")(
    targetData = Seq((0L, "0"), (1L, "10"), (3L, "30")).toDF("key", "value"),
    sourceData = Seq((1, 1L), (2, 2L)).toDF("key", "value"),
    clauses = update("*") :: insert("*") :: Nil,
    expected =
      ((0L, "0") +: (1L, "1") +: (2L, "2") +: (3L, "30") +: Nil).toDF("key", "value"),
    expectedWithoutEvolution =
      ((0L, "0") +: (1L, "1") +: (2L, "2") +: (3L, "30") +: Nil).toDF("key", "value"),
    confs = Seq(
      SQLConf.STORE_ASSIGNMENT_POLICY.key -> StoreAssignmentPolicy.STRICT.toString,
      DeltaSQLConf.UPDATE_AND_MERGE_CASTING_FOLLOWS_ANSI_ENABLED_FLAG.key -> "false"))

  testEvolution("new column with storeAssignmentPolicy = STRICT")(
    targetData = Seq((0, 0), (1, 10), (3, 30)).toDF("key", "value"),
    sourceData = Seq((1, 1, "one"), (2, 2, "two")).toDF("key", "value", "extra"),
    clauses = update("value = CAST(s.value AS short)") :: insert("*") :: Nil,
    expected =
      ((0, 0, null) +: (1, 1, null) +: (2, 2, "two") +: (3, 30, null) +: Nil)
        .toDF("key", "value", "extra"),
    expectedWithoutEvolution =
      ((0, 0) +: (1, 1) +: (2, 2) +: (3, 30) +: Nil).toDF("key", "value"),
    confs = Seq(
      SQLConf.STORE_ASSIGNMENT_POLICY.key -> StoreAssignmentPolicy.STRICT.toString,
      DeltaSQLConf.UPDATE_AND_MERGE_CASTING_FOLLOWS_ANSI_ENABLED_FLAG.key -> "false"))

}

/**
 * Trait collecting tests for schema evolution with a NOT MATCHED BY SOURCE clause.
 */
trait MergeIntoSchemaEvolutionNotMatchedBySourceTests extends MergeIntoSchemaEvolutionMixin {
  self: MergeIntoTestUtils with SharedSparkSession =>

  import testImplicits._

  // Test schema evolution with NOT MATCHED BY SOURCE clauses.
  testEvolution("new column with insert * and conditional update not matched by source")(
    targetData = Seq((0, 0), (1, 10), (3, 30)).toDF("key", "value"),
    sourceData = Seq((1, 1, "extra1"), (2, 2, "extra2")).toDF("key", "value", "extra"),
    clauses = insert("*") ::
      updateNotMatched(condition = "key > 0", set = "value = value + 1") :: Nil,
    expected = Seq(
      (0, 0, null), // Not matched by source, no change
      (1, 10, null), // Matched, no change
      (2, 2, "extra2"), // Not matched by target, inserted
      (3, 31, null) // Not matched by source, updated
    ).toDF("key", "value", "extra"),
    expectedWithoutEvolution = Seq((0, 0), (1, 10), (2, 2), (3, 31)).toDF("key", "value"))

  testEvolution("new column not inserted and conditional update not matched by source")(
    targetData = Seq((0, 0), (1, 10), (3, 30)).toDF("key", "value"),
    sourceData = Seq((1, 1, "extra1"), (2, 2, "extra2")).toDF("key", "value", "extra"),
    clauses = updateNotMatched(condition = "key > 0", set = "value = value + 1") :: Nil,
    expected = Seq(
      (0, 0), // Not matched by source, no change
      (1, 10), // Matched, no change
      (3, 31) // Not matched by source, updated
    ).toDF("key", "value"),
    expectedWithoutEvolution = Seq((0, 0), (1, 10), (3, 31)).toDF("key", "value"))

  testEvolution("new column referenced in matched condition but not inserted")(
    targetData = Seq((0, 0), (1, 10), (3, 30)).toDF("key", "value"),
    sourceData = Seq((1, 1, "extra1"), (2, 2, "extra2")).toDF("key", "value", "extra"),
    clauses = delete(condition = "extra = 'extra1'") ::
      updateNotMatched(condition = "key > 0", set = "value = value + 1") :: Nil,
    expected = Seq(
      (0, 0), // Not matched by source, no change
      // (1, 10), Matched, deleted
      (3, 31) // Not matched by source, updated
    ).toDF("key", "value"),
    expectedWithoutEvolution = Seq((0, 0), (3, 31)).toDF("key", "value"))

  testEvolution("matched update * and conditional update not matched by source")(
    targetData = Seq((0, 0), (1, 10), (3, 30)).toDF("key", "value"),
    sourceData = Seq((1, 1, "extra1"), (2, 2, "extra2")).toDF("key", "value", "extra"),
    clauses = update("*") ::
      updateNotMatched(condition = "key > 0", set = "value = value + 1") :: Nil,
    expected = Seq(
      (0, 0, null), // Not matched by source, no change
      (1, 1, "extra1"), // Matched, updated
      (3, 31, null) // Not matched by source, updated
    ).toDF("key", "value", "extra"),
    expectedWithoutEvolution = Seq((0, 0), (1, 1), (3, 31)).toDF("key", "value"))

  // Migrating new column via WHEN NOT MATCHED BY SOURCE is not allowed.
  testEvolution("update new column with not matched by source fails")(
    targetData = Seq((0, 0), (1, 10), (3, 30)).toDF("key", "value"),
    sourceData = Seq((1, 1, "extra3"), (2, 2, "extra2")).toDF("key", "value", "extra"),
    clauses = updateNotMatched("extra = s.extra") :: Nil,
    expectErrorContains = "cannot resolve extra in UPDATE clause",
    expectErrorWithoutEvolutionContains = "cannot resolve extra in UPDATE clause")
}

/**
 * Trait collecting all tests for nested struct evolution.
 */
trait MergeIntoNestedStructInMapEvolutionTests extends MergeIntoSchemaEvolutionMixin {
  self: MergeIntoTestUtils with SharedSparkSession =>

  import testImplicits._

  // scalastyle:off line.size.limit
  // Struct evolution inside of map values.
  testNestedStructsEvolution("new source column in map struct value")(
    target =
      """{ "key": "A", "map": { "key": { "a": 1 } } }
         { "key": "C", "map": { "key": { "a": 3 } } }""",
    source =
      """{ "key": "A", "map": { "key": { "a": 2, "b": 2 } } }
         { "key": "B", "map": { "key": { "a": 1, "b": 2 } } }""",
    targetSchema = new StructType()
      .add("key", StringType)
      .add("map", MapType(
          StringType,
          new StructType().add("a", IntegerType))),
    sourceSchema = new StructType()
      .add("key", StringType)
      .add("map", MapType(
          StringType,
          new StructType().add("a", IntegerType).add("b", IntegerType))),
    resultSchema = new StructType()
      .add("key", StringType)
      .add("map", MapType(
          StringType,
          new StructType().add("a", IntegerType).add("b", IntegerType))),
    clauses = update("*") :: insert("*") :: Nil,
    result =
      """{ "key": "A", "map": { "key": { "a": 2, "b": 2 } } }
         { "key": "B", "map": { "key": { "a": 1, "b": 2 } } }
         { "key": "C", "map": { "key": { "a": 3, "b": null } } }""",
    expectErrorWithoutEvolutionContains = "Cannot cast")

  testNestedStructsEvolution("new source column in nested map struct value")(
    target =
      """{"key": "A", "map": { "key": { "innerKey": { "a": 1 } } } }
         {"key": "C", "map": { "key": { "innerKey": { "a": 3 } } } }""",
    source =
      """{"key": "A", "map": { "key": { "innerKey": { "a": 2, "b": 3 } } } }
         {"key": "B", "map": { "key": { "innerKey": { "a": 2, "b": 3 } } } }""",
    targetSchema = new StructType()
      .add("key", StringType)
      .add("map", MapType(
          StringType,
          MapType(StringType, new StructType().add("a", IntegerType)))),
    sourceSchema = new StructType()
      .add("key", StringType)
      .add("map", MapType(
          StringType,
          MapType(StringType, new StructType().add("a", IntegerType).add("b", IntegerType)))),
    resultSchema = new StructType()
      .add("key", StringType)
      .add("map", MapType(
          StringType,
          MapType(StringType, new StructType().add("a", IntegerType).add("b", IntegerType)))),
    clauses = update("*") :: insert("*") :: Nil,
    result =
      """{"key": "A", "map": { "key": { "innerKey": { "a": 2, "b": 3 } } } }
         {"key": "B", "map": { "key": { "innerKey": { "a": 2, "b": 3 } } } }
         {"key": "C", "map": { "key": { "innerKey": { "a": 3, "b": null } } } }""",
    expectErrorWithoutEvolutionContains = "Cannot cast")

  testNestedStructsEvolution("source map struct value contains less columns than target")(
    target =
      """{ "key": "A", "map": { "key": { "a": 1, "b": 1 } } }
         { "key": "C", "map": { "key": { "a": 3, "b": 1 } } }""",
    source =
      """{ "key": "A", "map": { "key": { "a": 2 } } }
         { "key": "B", "map": { "key": { "a": 1 } } }""",
    targetSchema = new StructType()
      .add("key", StringType)
      .add("map", MapType(
          StringType,
          new StructType().add("a", IntegerType).add("b", IntegerType))),
    sourceSchema = new StructType()
      .add("key", StringType)
      .add("map", MapType(
          StringType,
          new StructType().add("a", IntegerType))),
    resultSchema = new StructType()
      .add("key", StringType)
      .add("map", MapType(
          StringType,
          new StructType().add("a", IntegerType).add("b", IntegerType))),
    clauses = update("*") :: insert("*") :: Nil,
    result =
      """{ "key": "A", "map": { "key": { "a": 2, "b": null } } }
         { "key": "B", "map": { "key": { "a": 1, "b": null } } }
         { "key": "C", "map": { "key": { "a": 3, "b": 1 } } }""",
    expectErrorWithoutEvolutionContains = "Cannot cast")

  testNestedStructsEvolution("source nested map struct value contains less columns than target")(
    target =
      """{"key": "A", "map": { "key": { "innerKey": { "a": 1, "b": 1 } } } }
         {"key": "C", "map": { "key": { "innerKey": { "a": 3, "b": 1 } } } }""",
    source =
      """{"key": "A", "map": { "key": { "innerKey": { "a": 2 } } } }
         {"key": "B", "map": { "key": { "innerKey": { "a": 2 } } } }""",
    targetSchema = new StructType()
      .add("key", StringType)
      .add("map", MapType(
          StringType,
          MapType(StringType, new StructType().add("a", IntegerType).add("b", IntegerType)))),
    sourceSchema = new StructType()
      .add("key", StringType)
      .add("map", MapType(
          StringType,
          MapType(StringType, new StructType().add("a", IntegerType)))),
    resultSchema = new StructType()
      .add("key", StringType)
      .add("map", MapType(
          StringType,
          MapType(StringType, new StructType().add("a", IntegerType).add("b", IntegerType)))),
    clauses = update("*") :: insert("*") :: Nil,
    result =
      """{"key": "A", "map": { "key": { "innerKey": { "a": 2, "b": null } } } }
         {"key": "B", "map": { "key": { "innerKey": { "a": 2, "b": null } } } }
         {"key": "C", "map": { "key": { "innerKey": { "a": 3, "b": 1 } } } }""",
    expectErrorWithoutEvolutionContains = "Cannot cast")

  testNestedStructsEvolution("source nested map struct value contains different type than target")(
    target =
      """{"key": "A", "map": { "key": { "a": 1, "b" : 1 } } }
         {"key": "C", "map": { "key": { "a": 3, "b" : 1 } } }""",
    source =
      """{"key": "A", "map": { "key": { "a": 1, "b" : "2" } } }
         {"key": "B", "map": { "key": { "a": 2, "b" : "2" } } }""",
    targetSchema = new StructType()
      .add("key", StringType)
      .add("map", MapType(
          StringType,
          new StructType().add("a", IntegerType).add("b", IntegerType))),
    sourceSchema = new StructType()
      .add("key", StringType)
      .add("map", MapType(
          StringType,
          new StructType().add("a", IntegerType).add("b", StringType))),
    resultSchema = new StructType()
      .add("key", StringType)
      .add("map", MapType(
          StringType,
          new StructType().add("a", IntegerType).add("b", IntegerType))),
    clauses = update("*") :: insert("*") :: Nil,
    result =
      """{"key": "A", "map": { "key": { "a": 1, "b" : 2 } } }
         {"key": "B", "map": { "key": { "a": 2, "b" : 2 } } }
         {"key": "C", "map": { "key": { "a": 3, "b" : 1 } } }""",
    resultWithoutEvolution =
      """{"key": "A", "map": { "key": { "a": 1, "b" : 2 } } }
         {"key": "B", "map": { "key": { "a": 2, "b" : 2 } } }
         {"key": "C", "map": { "key": { "a": 3, "b" : 1 } } }""")

  testNestedStructsEvolution("source nested map struct value in different order")(
    target =
      """{"key": "A", "map": { "key": { "a" : 1, "b" : 1 } } }
         {"key": "C", "map": { "key": { "a" : 3, "b" : 1 } } }""",
    source =
      """{"key": "A", "map": { "key": { "b" : 2, "a" : 1, "c" : 3 } } }
         {"key": "B", "map": { "key": { "b" : 2, "a" : 2, "c" : 4 } } }""",
    targetSchema = new StructType()
      .add("key", StringType)
      .add("map", MapType(
          StringType,
          new StructType().add("a", IntegerType).add("b", IntegerType))),
    sourceSchema = new StructType()
      .add("key", StringType)
      .add("map", MapType(
          StringType,
          new StructType().add("b", IntegerType).add("a", IntegerType).add("c", IntegerType))),
    resultSchema = new StructType()
      .add("key", StringType)
      .add("map", MapType(
          StringType,
          new StructType().add("a", IntegerType).add("b", IntegerType).add("c", IntegerType))),
    clauses = update("*") :: insert("*") :: Nil,
    result =
      """{"key": "A", "map": { "key": { "a": 1, "b" : 2, "c" : 3 } } }
         {"key": "B", "map": { "key": { "a": 2, "b" : 2, "c" : 4 } } }
         {"key": "C", "map": { "key": { "a": 3, "b" : 1, "c" : null } } }""",
    expectErrorWithoutEvolutionContains = "Cannot cast")

  testNestedStructsEvolution("source map struct value to map array value")(
    target =
      """{ "key": "A", "map": { "key": [ 1, 2 ] } }
         { "key": "C", "map": { "key": [ 3, 4 ] } }""",
    source =
      """{ "key": "A", "map": { "key": { "a": 2 } } }
         { "key": "B", "map": { "key": { "a": 1 } } }""",
    targetSchema = new StructType()
      .add("key", StringType)
      .add("map", MapType(
          StringType,
          ArrayType(IntegerType))),
    sourceSchema = new StructType()
      .add("key", StringType)
      .add("map", MapType(
          StringType,
          new StructType().add("a", IntegerType))),
    clauses = update("*") :: insert("*") :: Nil,
    expectErrorContains = "Failed to merge incompatible data types",
    expectErrorWithoutEvolutionContains = "Cannot cast")

  testNestedStructsEvolution("source struct nested in map array values contains more columns in different order")(
    target =
      """{ "key": "A", "map": { "key": [ { "a": 1, "b": 2 } ] } }
         { "key": "C", "map": { "key": [ { "a": 3, "b": 4 } ] } }""",
    source =
      """{ "key": "A", "map": { "key": [ { "b": 6, "c": 7, "a": 5 } ] } }
         { "key": "B", "map": { "key": [ { "b": 9, "c": 10, "a": 8 } ] } }""",
    targetSchema = new StructType()
      .add("key", StringType)
      .add("map", MapType(
          StringType,
          ArrayType(
            new StructType().add("a", IntegerType).add("b", IntegerType)))),
    sourceSchema = new StructType()
      .add("key", StringType)
      .add("map", MapType(
          StringType,
          ArrayType(
          new StructType().add("b", IntegerType).add("c", IntegerType).add("a", IntegerType)))),
    resultSchema = new StructType()
      .add("key", StringType)
      .add("map", MapType(
          StringType,
          ArrayType(
          new StructType().add("a", IntegerType).add("b", IntegerType).add("c", IntegerType)))),
    clauses = update("*") :: insert("*") :: Nil,
    result =
      """{ "key": "A", "map": { "key": [ { "a": 5, "b": 6, "c": 7 } ] } }
         { "key": "B", "map": { "key": [ { "a": 8, "b": 9, "c": 10 } ] } }
         { "key": "C", "map": { "key": [ { "a": 3, "b": 4, "c": null } ] } }""",
    expectErrorWithoutEvolutionContains = "Cannot cast")

  // Struct evolution inside of map keys.
  testEvolution("new source column in map struct key")(
    targetData = Seq((1, 2, 3, 4), (3, 5, 6, 7)).toDF("key", "a", "b", "value")
      .selectExpr("key", "map(named_struct('a', a, 'b', b), value) as x"),
    sourceData = Seq((1, 10, 30, 50, 1), (2, 20, 40, 60, 2)).toDF("key", "a", "b", "c", "value")
      .selectExpr("key", "map(named_struct('a', a, 'b', b, 'c', c), value) as x"),
    clauses = update("*") :: insert("*") :: Nil,
    expected = Seq((1, 10, 30, 50, 1), (2, 20, 40, 60, 2), (3, 5, 6, null, 7))
      .asInstanceOf[List[(Integer, Integer, Integer, Integer, Integer)]]
      .toDF("key", "a", "b", "c", "value")
      .selectExpr("key", "map(named_struct('a', a, 'b', b, 'c', c), value) as x"),
    expectErrorWithoutEvolutionContains = "Cannot cast"
  )

  testEvolution("source nested map struct key contains less columns than target")(
    targetData = Seq((1, 2, 3, 4, 5), (3, 6, 7, 8, 9)).toDF("key", "a", "b", "c", "value")
      .selectExpr("key", "map(named_struct('a', a, 'b', b, 'c', c), value) as x"),
    sourceData = Seq((1, 10, 50, 1), (2, 20, 60, 2)).toDF("key", "a", "c", "value")
      .selectExpr("key", "map(named_struct('a', a, 'c', c), value) as x"),
    clauses = update("*") :: insert("*") :: Nil,
    expected = Seq((1, 10, null, 50, 1), (2, 20, null, 60, 2), (3, 6, 7, 8, 9))
      .asInstanceOf[List[(Integer, Integer, Integer, Integer, Integer)]]
      .toDF("key", "a", "b", "c", "value")
      .selectExpr("key", "map(named_struct('a', a, 'b', b, 'c', c), value) as x"),
    expectErrorWithoutEvolutionContains = "Cannot cast"
  )

  testEvolution("source nested map struct key contains different type than target")(
    targetData = Seq((1, 2, 3, 4), (3, 5, 6, 7)).toDF("key", "a", "b", "value")
      .selectExpr("key", "map(named_struct('a', a, 'b', b), value) as x"),
    sourceData = Seq((1, 10, "30", 1), (2, 20, "40", 2)).toDF("key", "a", "b", "value")
      .selectExpr("key", "map(named_struct('a', a, 'b', b), value) as x"),
    clauses = update("*") :: insert("*") :: Nil,
    expected = Seq((1, 10, 30, 1), (2, 20, 40, 2), (3, 5, 6, 7))
      .asInstanceOf[List[(Integer, Integer, Integer, Integer)]]
      .toDF("key", "a", "b", "value")
      .selectExpr("key", "map(named_struct('a', a, 'b', b), value) as x"),
    expectedWithoutEvolution = Seq((1, 10, 30, 1), (2, 20, 40, 2), (3, 5, 6, 7))
      .asInstanceOf[List[(Integer, Integer, Integer, Integer)]]
      .toDF("key", "a", "b", "value")
      .selectExpr("key", "map(named_struct('a', a, 'b', b), value) as x")
  )

  testEvolution("source nested map struct key in different order")(
    targetData = Seq((1, 2, 3, 4), (3, 5, 6, 7)).toDF("key", "a", "b", "value")
      .selectExpr("key", "map(named_struct('a', a, 'b', b), value) as x"),
    sourceData = Seq((1, 10, 30, 1), (2, 20, 40, 2)).toDF("key", "a", "b", "value")
      .selectExpr("key", "map(named_struct('b', b, 'a', a), value) as x"),
    clauses = update("*") :: insert("*") :: Nil,
    expected = Seq((1, 10, 30, 1), (2, 20, 40, 2), (3, 5, 6, 7))
      .asInstanceOf[List[(Integer, Integer, Integer, Integer)]]
      .toDF("key", "a", "b", "value")
      .selectExpr("key", "map(named_struct('a', a, 'b', b), value) as x"),
    expectedWithoutEvolution = Seq((1, 10, 30, 1), (2, 20, 40, 2), (3, 5, 6, 7))
      .asInstanceOf[List[(Integer, Integer, Integer, Integer)]]
      .toDF("key", "a", "b", "value")
      .selectExpr("key", "map(named_struct('a', a, 'b', b), value) as x")
  )

  testEvolution("struct nested in map array keys contains more columns")(
    targetData = Seq((1, 2, 3, 4), (3, 5, 6, 7)).toDF("key", "a", "b", "value")
      .selectExpr("key", "map(array(named_struct('a', a, 'b', b)), value) as x"),
    sourceData = Seq((1, 10, 30, 50, 1), (2, 20, 40, 60, 2)).toDF("key", "a", "b", "c", "value")
      .selectExpr("key", "map(array(named_struct('a', a, 'b', b, 'c', c)), value) as x"),
    clauses = update("*") :: insert("*") :: Nil,
    expected = Seq((1, 10, 30, 50, 1), (2, 20, 40, 60, 2), (3, 5, 6, null, 7))
      .asInstanceOf[List[(Integer, Integer, Integer, Integer, Integer)]]
      .toDF("key", "a", "b", "c", "value")
      .selectExpr("key", "map(array(named_struct('a', a, 'b', b, 'c', c)), value) as x"),
    expectErrorWithoutEvolutionContains = "cannot cast"
  )

  testEvolution("update-only struct nested in map array keys contains more columns")(
    targetData = Seq((1, 2, 3, 4), (3, 5, 6, 7)).toDF("key", "a", "b", "value")
      .selectExpr("key", "map(array(named_struct('a', a, 'b', b)), value) as x"),
    sourceData = Seq((1, 10, 30, 50, 1), (2, 20, 40, 60, 2)).toDF("key", "a", "b", "c", "value")
      .selectExpr("key", "map(array(named_struct('a', a, 'b', b, 'c', c)), value) as x"),
    clauses = update("*") :: Nil,
    expected = Seq((1, 10, 30, 50, 1), (3, 5, 6, null, 7))
      .asInstanceOf[List[(Integer, Integer, Integer, Integer, Integer)]]
      .toDF("key", "a", "b", "c", "value")
      .selectExpr("key", "map(array(named_struct('a', a, 'b', b, 'c', c)), value) as x"),
    expectErrorWithoutEvolutionContains = "cannot cast"
  )

  testEvolution("struct evolution in both map keys and values")(
    targetData = Seq((1, 2, 3, 4, 5), (3, 6, 7, 8, 9)).toDF("key", "a", "b", "d", "e")
      .selectExpr("key", "map(named_struct('a', a, 'b', b), named_struct('d', d, 'e', e)) as x"),
    sourceData = Seq((1, 10, 30, 50, 70, 90, 110), (2, 20, 40, 60, 80, 100, 120))
      .toDF("key", "a", "b", "c", "d", "e", "f")
      .selectExpr("key", "map(named_struct('a', a, 'b', b, 'c', c), named_struct('d', d, 'e', e, 'f', f)) as x"),
    clauses = update("*") :: insert("*") :: Nil,
    expected = Seq((1, 10, 30, 50, 70, 90, 110), (2, 20, 40, 60, 80, 100, 120), (3, 6, 7, null, 8, 9, null))
      .asInstanceOf[List[(Integer, Integer, Integer, Integer, Integer, Integer, Integer)]]
      .toDF("key", "a", "b", "c", "d", "e", "f")
      .selectExpr("key", "map(named_struct('a', a, 'b', b, 'c', c), named_struct('d', d, 'e', e, 'f', f)) as x"),
    expectErrorWithoutEvolutionContains = "cannot cast"
  )

  testEvolution("update only struct evolution in both map keys and values")(
    targetData = Seq((1, 2, 3, 4, 5), (3, 6, 7, 8, 9)).toDF("key", "a", "b", "d", "e")
      .selectExpr("key", "map(named_struct('a', a, 'b', b), named_struct('d', d, 'e', e)) as x"),
    sourceData = Seq((1, 10, 30, 50, 70, 90, 110), (2, 20, 40, 60, 80, 100, 120))
      .toDF("key", "a", "b", "c", "d", "e", "f")
      .selectExpr("key", "map(named_struct('a', a, 'b', b, 'c', c), named_struct('d', d, 'e', e, 'f', f)) as x"),
    clauses = update("*") :: Nil,
    expected = Seq((1, 10, 30, 50, 70, 90, 110), (3, 6, 7, null, 8, 9, null))
      .asInstanceOf[List[(Integer, Integer, Integer, Integer, Integer, Integer, Integer)]]
      .toDF("key", "a", "b", "c", "d", "e", "f")
      .selectExpr("key", "map(named_struct('a', a, 'b', b, 'c', c), named_struct('d', d, 'e', e, 'f', f)) as x"),
    expectErrorWithoutEvolutionContains = "cannot cast"
  )
  // scalastyle:on line.size.limit
}

trait MergeIntoNestedStructEvolutionInsertTests extends MergeIntoSchemaEvolutionMixin {
  self: MergeIntoTestUtils with SharedSparkSession =>

  import testImplicits._

  // Nested Schema evolution with INSERT alone
  testNestedStructsEvolution("new nested source field added when inserting top-level column")(
    target = """{ "key": "A", "value": { "a": 1 } }""",
    source = """{ "key": "B", "value": { "a": 2, "b": 3 } }""",
    targetSchema = new StructType()
      .add("key", StringType)
      .add("value", new StructType()
        .add("a", IntegerType)),
    sourceSchema = new StructType()
      .add("key", StringType)
      .add("value", new StructType()
        .add("a", IntegerType)
        .add("b", IntegerType)),
    clauses = insert("(value) VALUES (s.value)") :: Nil,
    result =
      """{ "key": "A", "value": { "a": 1, "b": null } }
       { "key": null, "value": { "a": 2, "b": 3 } }""".stripMargin,
    resultSchema = new StructType()
      .add("key", StringType)
      .add("value", new StructType()
        .add("a", IntegerType)
        .add("b", IntegerType)),
    expectErrorWithoutEvolutionContains = "Cannot cast")

  testNestedStructsEvolution("insert new nested source field not supported")(
    target = """{ "key": "A", "value": { "a": 1 } }""",
    source = """{ "key": "A", "value": { "a": 2, "b": 3, "c": 4 } }""",
    targetSchema = new StructType()
      .add("key", StringType)
      .add("value", new StructType()
        .add("a", IntegerType)),
    sourceSchema = new StructType()
      .add("key", StringType)
      .add("value", new StructType()
        .add("a", IntegerType)
        .add("b", IntegerType)
        .add("c", IntegerType)),
    clauses = insert("(value.b) VALUES (s.value.b)") :: Nil,
    expectErrorContains = "Nested field is not supported in the INSERT clause of MERGE operation",
    expectErrorWithoutEvolutionContains = "No such struct field")

  // scalastyle:off line.size.limit
  testNestedStructsEvolution("new nested column with update non-* and insert * - array of struct - longer source")(
    target = """{ "key": "A", "value": [ { "a": { "x": 1, "y": 2 }, "b": 1, "c": 2 } ] }""",
    source =
      """{ "key": "A", "value": [ { "b": "2", "a": { "y": 20, "x": 10 } }, { "b": "3", "a": { "y": 30, "x": 20 } }, { "b": "4", "a": { "y": 30, "x": 20 } } ] }
           { "key": "B", "value": [ { "b": "3", "a": { "y": 30, "x": 40 } }, { "b": "4", "a": { "y": 30, "x": 40 } } ] }""",
    targetSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("a", new StructType()
            .add("x", IntegerType)
            .add("y", IntegerType))
          .add("b", IntegerType)
          .add("c", IntegerType))),
    sourceSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("b", StringType)
          .add("a", new StructType().add("y", IntegerType).add("x", IntegerType)))),
    clauses = update("value = s.value") :: insert("*") :: Nil,
    result =
      """{ "key": "A", "value": [ { "a": { "x": 10, "y": 20 }, "b": 2, "c": null}, { "a": { "x": 20, "y": 30}, "b": 3, "c": null }, { "a": { "x": 20, "y": 30}, "b": 4, "c": null } ] }
           { "key": "B", "value": [ { "a": { "x": 40, "y": 30 }, "b": 3, "c": null }, { "a": { "x": 40, "y": 30}, "b": 4, "c": null } ] }""".stripMargin,
    expectErrorWithoutEvolutionContains = "Cannot cast")

  testNestedStructsEvolution("new nested column with update non-* and insert * - array of struct - longer target")(
    target = """{ "key": "A", "value": [ { "a": { "x": 1, "y": 2 }, "b": 1, "c": 2 }, { "a": { "x": 3, "y": 2 }, "b": 2, "c": 2 } ] }""",
    source =
      """{ "key": "A", "value": [ { "b": "2", "a": { "y": 20, "x": 10 } } ] }
           { "key": "B", "value": [ { "b": "3", "a": { "y": 30, "x": 40 } } ] }""",
    targetSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("a", new StructType()
            .add("x", IntegerType)
            .add("y", IntegerType))
          .add("b", IntegerType)
          .add("c", IntegerType))),
    sourceSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("b", StringType)
          .add("a", new StructType().add("y", IntegerType).add("x", IntegerType)))),
    clauses = update("value = s.value") :: insert("*") :: Nil,
    result =
      """{ "key": "A", "value": [ { "a": { "x": 10, "y": 20}, "b": 2, "c": null } ] }
           { "key": "B", "value": [ { "a": { "x": 40, "y": 30}, "b": 3, "c": null } ] }""".stripMargin,
    expectErrorWithoutEvolutionContains = "Cannot cast")

  testNestedStructsEvolution("new nested column with update non-* and insert * - nested array of struct - longer source")(
    target = """{ "key": "A", "value": [ { "a": { "y": 2, "x": [ { "c": 1, "d": 3 } ] }, "b": 1, "c": 4 } ] }""",
    source =
      """{ "key": "A", "value": [ { "b": "2", "a": {"x": [ { "d": "30", "c": 10 }, { "d": "20", "c": 10 }, { "d": "20", "c": 10 } ], "y": 20 } } ] }
          { "key": "B", "value": [ { "b": "3", "a": {"x": [ { "d": "50", "c": 20 }, { "d": "20", "c": 10 } ], "y": 60 } } ] }""",
    targetSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("a", new StructType()
            .add("y", IntegerType)
            .add("x", ArrayType(
              new StructType()
                .add("c", IntegerType)
                .add("d", IntegerType)
            )))
          .add("b", IntegerType)
          .add("c", IntegerType))),
    sourceSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("b", StringType)
          .add("a", new StructType()
            .add("x", ArrayType(
              new StructType()
                .add("d", StringType)
                .add("c", IntegerType)
            ))
            .add("y", IntegerType)))),
    clauses = update("value = s.value") :: insert("*") :: Nil,
    result =
      """{ "key": "A", "value": [ { "a": { "y": 20, "x": [ { "c": 10, "d": 30 }, { "c": 10, "d": 20 }, { "c": 10, "d": 20 } ] }, "b": 2, "c": null}] }
          { "key": "B", "value": [ { "a": { "y": 60, "x": [ { "c": 20, "d": 50 }, { "c": 10, "d": 20 } ] }, "b": 3, "c": null } ] }""",
    expectErrorWithoutEvolutionContains = "Cannot cast")

  testNestedStructsEvolution("new nested column with update non-* and insert * - nested array of struct - longer target")(
    target = """{ "key": "A", "value": [ { "a": { "y": 2, "x": [ { "c": 1, "d": 3}, { "c": 2, "d": 3 } ] }, "b": 1, "c": 4 } ] }""",
    source =
      """{ "key": "A", "value": [ { "b": "2", "a": {"x": [ { "d": "30", "c": 10 } ], "y": 20 } } ] }
          { "key": "B", "value": [ { "b": "3", "a": {"x": [ { "d": "50", "c": 20 } ], "y": 60 } } ] }""",
    targetSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("a", new StructType()
            .add("y", IntegerType)
            .add("x", ArrayType(
              new StructType()
                .add("c", IntegerType)
                .add("d", IntegerType)
            )))
          .add("b", IntegerType)
          .add("c", IntegerType))),
    sourceSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("b", StringType)
          .add("a", new StructType()
            .add("x", ArrayType(
              new StructType()
                .add("d", StringType)
                .add("c", IntegerType)
            ))
            .add("y", IntegerType)))),
    clauses = update("value = s.value") :: insert("*") :: Nil,
    result =
      """{ "key": "A", "value": [ { "a": { "y": 20, "x": [ { "c": 10, "d": 30} ] }, "b": 2, "c": null}] }
          { "key": "B", "value": [ { "a": { "y": 60, "x": [ { "c": 20, "d": 50 } ] }, "b": 3, "c": null } ] }""",
    expectErrorWithoutEvolutionContains = "Cannot cast")
  // scalastyle:on line.size.limit

  testEvolution("new nested-nested column with update non-* and insert *")(
    targetData = Seq((1, 1, 2, 3)).toDF("key", "a", "b", "c")
      .selectExpr("key", "named_struct('y', named_struct('a', a, 'b', b, 'c', c)) as x"),
    sourceData = Seq((1, 10, 30), (2, 20, 40)).toDF("key", "a", "c")
      .selectExpr("key", "named_struct('y', named_struct('a', a, 'c', c)) as x"),
    clauses = update("x.y.a = s.x.y.a") :: insert("*") :: Nil,
    expected = Seq((1, 10, 2, 3), (2, 20, null, 40))
      .asInstanceOf[List[(Integer, Integer, Integer, Integer)]]
      .toDF("key", "a", "b", "c")
      .selectExpr("key", "named_struct('y', named_struct('a', a, 'b', b, 'c', c)) as x"),
    expectErrorWithoutEvolutionContains = "Cannot cast"
  )

  // scalastyle:off line.size.limit
  testNestedStructsEvolution("new nested-nested column with update non-* and insert * - array of struct - longer source")(
    target = """{ "key": "A", "value": [ { "a": { "x": 1, "y": 2, "z": 3 }, "b": 1 } ] }""",
    source =
      """{ "key": "A", "value": [ { "b": "2", "a": { "y": 20, "x": 10 } }, { "b": "3", "a": { "y": 20, "x": 30 } }, { "b": "4", "a": { "y": 20, "x": 30 } } ] }
           { "key": "B", "value": [ { "b": "3", "a": { "y": 30, "x": 40 } }, { "b": "4", "a": { "y": 30, "x": 40 } } ] }""",
    targetSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("a", new StructType()
            .add("x", IntegerType)
            .add("y", IntegerType)
            .add("z", IntegerType))
          .add("b", IntegerType))),
    sourceSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("b", StringType)
          .add("a", new StructType().add("y", IntegerType).add("x", IntegerType)))),
    clauses = update("value = s.value") :: insert("*") :: Nil,
    result =
      """{ "key": "A", "value": [ { "a": { "x": 10, "y": 20, "z": null }, "b": 2 }, { "a": { "x": 30, "y": 20, "z": null }, "b": 3}, { "a": { "x": 30, "y": 20, "z": null }, "b": 4 } ] }
           { "key": "B", "value": [ { "a": { "x": 40, "y": 30, "z": null }, "b": 3 }, { "a": { "x": 40, "y": 30, "z": null }, "b": 4 } ] }""".stripMargin,
    expectErrorWithoutEvolutionContains = "Cannot cast")

  testNestedStructsEvolution("new nested-nested column with update non-* and insert * - array of struct - longer target")(
    target = """{ "key": "A", "value": [ { "a": { "x": 1, "y": 2, "z": 3 }, "b": 1 }, { "a": { "x": 2, "y": 3, "z": 4 }, "b": 1 } ] }""",
    source =
      """{ "key": "A", "value": [ { "b": "2", "a": { "y": 20, "x": 10 } } ] }
           { "key": "B", "value": [ { "b": "3", "a": { "y": 30, "x": 40 } } ] }""",
    targetSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("a", new StructType()
            .add("x", IntegerType)
            .add("y", IntegerType)
            .add("z", IntegerType))
          .add("b", IntegerType))),
    sourceSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("b", StringType)
          .add("a", new StructType().add("y", IntegerType).add("x", IntegerType)))),
    clauses = update("value = s.value") :: insert("*") :: Nil,
    result =
      """{ "key": "A", "value": [{ "a": { "x": 10, "y": 20, "z": null }, "b": 2 }] }
           { "key": "B", "value": [{ "a": { "x": 40, "y": 30, "z": null }, "b": 3 }] }""".stripMargin,
    expectErrorWithoutEvolutionContains = "Cannot cast")

  testNestedStructsEvolution("new nested-nested column with update non-* and insert * - nested array of struct - longer source")(
    target = """{ "key": "A", "value": [ { "a": { "y": 2, "x": [ { "c": 1, "d": 3, "e": 1 } ] }, "b": 1 } ] }""",
    source =
      """{ "key": "A", "value": [ { "b": "2", "a": {"x": [ { "d": "30", "c": 10 }, { "d": "30", "c": 40 }, { "d": "30", "c": 50 } ], "y": 20 } } ] }
          { "key": "B", "value": [ { "b": "3", "a": {"x": [ { "d": "50", "c": 20 }, { "d": "50", "c": 30 } ], "y": 60 } } ] }""",
    targetSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("a", new StructType()
            .add("y", IntegerType)
            .add("x", ArrayType(
              new StructType()
                .add("c", IntegerType)
                .add("d", IntegerType)
                .add("e", IntegerType)
            )))
          .add("b", IntegerType))),
    sourceSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("b", StringType)
          .add("a", new StructType()
            .add("x", ArrayType(
              new StructType()
                .add("d", StringType)
                .add("c", IntegerType)
            ))
            .add("y", IntegerType)))),
    clauses = update("value = s.value") :: insert("*") :: Nil,
    result =
      """{ "key": "A", "value": [ { "a": { "y": 20, "x": [ { "c": 10, "d": 30, "e": null }, { "c": 40, "d": 30, "e": null }, { "c": 50, "d": 30, "e": null } ] }, "b": 2 } ] }
          { "key": "B", "value": [ { "a": { "y": 60, "x": [ { "c": 20, "d": 50, "e": null }, { "c": 30, "d": 50, "e": null } ] }, "b": 3 } ] }""",
    expectErrorWithoutEvolutionContains = "Cannot cast")

  testNestedStructsEvolution("new nested-nested column with update non-* and insert * - nested array of struct - longer target")(
    target = """{ "key": "A", "value": [ { "a": { "y": 2, "x": [ { "c": 1, "d": 3, "e": 1 }, { "c": 2, "d": 3, "e": 4 } ] }, "b": 1 } ] }""",
    source =
      """{ "key": "A", "value": [ { "b": "2", "a": { "x": [ { "d": "30", "c": 10 } ], "y": 20 } } ] }
          { "key": "B", "value": [ { "b": "3", "a": { "x": [ { "d": "50", "c": 20 } ], "y": 60 } } ] }""",
    targetSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("a", new StructType()
            .add("y", IntegerType)
            .add("x", ArrayType(
              new StructType()
                .add("c", IntegerType)
                .add("d", IntegerType)
                .add("e", IntegerType)
            )))
          .add("b", IntegerType))),
    sourceSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("b", StringType)
          .add("a", new StructType()
            .add("x", ArrayType(
              new StructType()
                .add("d", StringType)
                .add("c", IntegerType)
            ))
            .add("y", IntegerType)))),
    clauses = update("value = s.value") :: insert("*") :: Nil,
    result =
      """{ "key": "A", "value": [ { "a": { "y": 20, "x": [ { "c": 10, "d": 30, "e": null } ] }, "b": 2 } ] }
          { "key": "B", "value": [ { "a": { "y": 60, "x": [ { "c": 20, "d": 50, "e": null } ] }, "b": 3 } ] }""",
    expectErrorWithoutEvolutionContains = "Cannot cast")

  // scalastyle:on line.size.limit

  // Note that the obvious dual of this test, "update * and insert non-*", doesn't exist
  // because nested columns can't be explicitly INSERTed to.
  testEvolution("new nested column with update non-* and insert *")(
    targetData = Seq((1, 1, 2, 3)).toDF("key", "a", "b", "c")
      .selectExpr("key", "named_struct('a', a, 'b', b, 'c', c) as x"),
    sourceData = Seq((1, 10, 30), (2, 20, 40)).toDF("key", "a", "c")
      .selectExpr("key", "named_struct('a', a, 'c', c) as x"),
    clauses = update("x.a = s.x.a") :: insert("*") :: Nil,
    expected = Seq((1, 10, 2, 3), (2, 20, null, 40))
      .asInstanceOf[List[(Integer, Integer, Integer, Integer)]]
      .toDF("key", "a", "b", "c")
      .selectExpr("key", "named_struct('a', a, 'b', b, 'c', c) as x"),
    expectErrorWithoutEvolutionContains = "Cannot cast"
  )

  // scalastyle:off line.size.limit
  testNestedStructsEvolution("missing nested column resolved by name - insert - array of struct")(
    target = """{ "key": "A", "value": [ { "a": { "x": 1, "y": 2, "z": 1 }, "b": 1 } ] }""",
    source =
      """{ "key": "A", "value": [ { "a": { "x": 10, "z": 20 }, "b": "2" } ] }
           { "key": "B", "value": [ { "a": { "x": 40, "z": 30 }, "b": "3" }, { "a": { "x": 40, "z": 30 }, "b": "4" }, { "a": { "x": 40, "z": 30 }, "b": "5" } ] }""",
    targetSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("a", new StructType()
            .add("x", IntegerType)
            .add("y", IntegerType)
            .add("z", IntegerType))
          .add("b", IntegerType))),
    sourceSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("a", new StructType()
            .add("x", IntegerType)
            .add("z", IntegerType))
          .add("b", StringType))),
    clauses = insert("*") :: Nil,
    result =
      """{ "key": "A", "value": [ { "a": { "x": 1, "y": 2, "z": 1 }, "b": 1 } ] }
           { "key": "B", "value": [ { "a": { "x": 40, "y": null, "z": 30 }, "b": 3 }, { "a": { "x": 40, "y": null, "z": 30 }, "b": 4 }, { "a": { "x": 40, "y": null, "z": 30 }, "b": 5 } ] }""",
    expectErrorWithoutEvolutionContains = "Cannot cast")

  testNestedStructsEvolution("missing nested column resolved by name - insert - nested array of struct")(
    target = """{ "key": "A", "value": [ { "a": { "y": 2, "x": [ { "c": 1, "d": 3, "e": 1 } ] }, "b": 1 } ] }""",
    source =
      """{ "key": "A", "value": [ { "a": { "y": 20, "x": [ { "c": 10, "e": "30" } ] }, "b": "2" } ] }
          { "key": "B", "value": [ {"a": {"y": 60, "x": [ { "c": 20, "e": "50" }, { "c": 20, "e": "60" }, { "c": 20, "e": "80" } ] }, "b": "3" } ] }""",
    targetSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("a", new StructType()
            .add("y", IntegerType)
            .add("x", ArrayType(
              new StructType()
                .add("c", IntegerType)
                .add("d", IntegerType)
                .add("e", IntegerType)
            )))
          .add("b", IntegerType))),
    sourceSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("a", new StructType()
            .add("y", IntegerType)
            .add("x", ArrayType(
              new StructType()
                .add("c", IntegerType)
                .add("e", StringType)
            )))
          .add("b", StringType))),
    clauses = insert("*") :: Nil,
    result =
      """{ "key": "A", "value": [ { "a": { "y": 2, "x": [ { "c": 1, "d": 3, "e": 1 } ] }, "b": 1 } ] }
          { "key": "B", "value": [ { "a": { "y": 60, "x": [ { "c": 20, "d": null, "e": 50 }, { "c": 20, "d": null, "e": 60 }, { "c": 20, "d": null, "e": 80 } ] }, "b": 3 } ] }""",
    expectErrorWithoutEvolutionContains = "Cannot cast")
  // scalastyle:on line.size.limit

  testEvolution("additional nested column in source resolved by name - insert")(
    targetData = Seq((1, 10, 30)).toDF("key", "a", "c")
      .selectExpr("key", "named_struct('a', a, 'c', c) as x"),
    sourceData = Seq((2, 20, 30, 40)).toDF("key", "a", "b", "c")
      .selectExpr("key", "named_struct('a', a, 'b', b, 'c', c) as x"),
    clauses = insert("*") :: Nil,
    expected = ((1, (10, null, 30)) +: ((2, (20, 30, 40)) +: Nil))
      .asInstanceOf[List[(Integer, (Integer, Integer, Integer))]].toDF("key", "x")
      .selectExpr("key", "named_struct('a', x._1, 'c', x._3, 'b', x._2) as x"),
    expectErrorWithoutEvolutionContains = "Cannot cast"
  )

  // scalastyle:off line.size.limit
  testNestedStructsEvolution("additional nested column in source resolved by name - insert - array of struct")(
    target = """{ "key": "A", "value": [ { "a": { "x": 1, "z": 2 }, "b": 1 } ] }""",
    source =
      """{ "key": "A", "value": [ { "a": { "x": 10, "y": 20, "z": 2 }, "b": "2 "} ] }
           { "key": "B", "value": [ {"a": { "x": 40, "y": 30, "z": 3 }, "b": "3" }, {"a": { "x": 40, "y": 30, "z": 3 }, "b": "4" } ] }""",
    targetSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("a", new StructType()
            .add("x", IntegerType)
            .add("z", IntegerType))
          .add("b", IntegerType))),
    sourceSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("a", new StructType()
            .add("x", IntegerType).add("y", IntegerType).add("z", IntegerType))
          .add("b", StringType))),
    clauses = insert("*") :: Nil,
    result =
      """{ "key": "A", "value": [ { "a": { "x": 1, "z": 2, "y": null }, "b": 1 } ] }
           { "key": "B", "value": [ { "a": { "x": 40, "z": 3, "y": 30 }, "b": 3 }, { "a": { "x": 40, "z": 3, "y": 30 }, "b": 4 } ] }""",
    resultSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("a", new StructType()
            .add("x", IntegerType)
            .add("z", IntegerType)
            .add("y", IntegerType))
          .add("b", IntegerType))),
    expectErrorWithoutEvolutionContains = "Cannot cast")

  testNestedStructsEvolution("additional nested column in source resolved by name - insert - nested array of struct")(
    target = """{ "key": "A", "value": [ { "a": { "y": 2, "x": [ { "c": 1, "e": 3 } ] }, "b": 1 } ] }""",
    source =
      """{ "key": "A", "value": [ { "a": { "y": 20, "x": [ { "c": 10, "d": "30", "e": 1 } ] }, "b": "2" } ] }
          { "key": "B", "value": [ { "a": { "y": 60, "x": [ { "c": 20, "d": "50", "e": 2 }, { "c": 20, "d": "50", "e": 3 } ] }, "b": "3" } ] }""",
    targetSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("a", new StructType()
            .add("y", IntegerType)
            .add("x", ArrayType(
              new StructType()
                .add("c", IntegerType)
                .add("e", IntegerType)
            )))
          .add("b", IntegerType))),
    sourceSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("a", new StructType()
            .add("y", IntegerType)
            .add("x", ArrayType(
              new StructType()
                .add("c", IntegerType)
                .add("d", StringType)
                .add("e", IntegerType)
            )))
          .add("b", StringType))),
    resultSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("a", new StructType()
            .add("y", IntegerType)
            .add("x", ArrayType(
              new StructType()
                .add("c", IntegerType)
                .add("e", IntegerType)
                .add("d", StringType)
            )))
          .add("b", IntegerType))),
    clauses = insert("*") :: Nil,
    result =
      """{ "key": "A", "value": [ { "a": { "y": 2, "x": [ { "c": 1, "e": 3, "d": null } ] }, "b": 1 } ] }
          { "key": "B", "value": [ { "a": { "y": 60, "x": [ { "c": 20, "e": 2, "d": "50" }, { "c": 20, "e": 3, "d": "50" } ] }, "b": 3 } ] }""",
    expectErrorWithoutEvolutionContains = "Cannot cast")
  // scalastyle:on line.size.limit

  testEvolution("nested columns resolved by name with same column count but different names")(
    targetData = Seq((1, 1, 2, 3)).toDF("key", "a", "b", "c")
      .selectExpr("key", "struct(a, b, c) as x"),
    sourceData = Seq((1, 10, 20, 30), (2, 20, 30, 40)).toDF("key", "a", "b", "d")
      .selectExpr("key", "struct(a, b, d) as x"),
    clauses = update("*") :: insert("*") :: Nil,
    // We evolve to the schema (key, x.{a, b, c, d}).
    expected = ((1, (10, 20, 3, 30)) +: (2, (20, 30, null, 40)) +: Nil)
      .asInstanceOf[List[(Integer, (Integer, Integer, Integer, Integer))]]
      .toDF("key", "x")
      .selectExpr("key", "named_struct('a', x._1, 'b', x._2, 'c', x._3, 'd', x._4) as x"),
    expectErrorWithoutEvolutionContains = "All nested columns must match."
  )

  // scalastyle:off line.size.limit
  testNestedStructsEvolution("nested columns resolved by name with same column count but different names - array of struct - longer source")(
    target = """{ "key": "A", "value": [ { "a": { "x": 1, "y": 2, "o": 4 }, "b": 1 } ] }""",
    source =
      """{ "key": "A", "value": [ { "a": { "x": 10, "y": 20, "z": 2 }, "b": "2" }, { "a": { "x": 10, "y": 20, "z": 2 }, "b": "3" }, { "a": { "x": 10, "y": 20, "z": 2 }, "b": "4" } ] }
           { "key": "B", "value": [ {"a": { "x": 40, "y": 30, "z": 3 }, "b": "3" }, {"a": { "x": 40, "y": 30, "z": 3 }, "b": "4" } ] }""",
    targetSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("a", new StructType()
            .add("x", IntegerType)
            .add("y", IntegerType)
            .add("o", IntegerType))
          .add("b", IntegerType))),
    sourceSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("a", new StructType()
            .add("x", IntegerType).add("y", IntegerType).add("z", IntegerType))
          .add("b", StringType))),
    clauses = update("*") :: insert("*") :: Nil,
    result =
      """{ "key": "A", "value": [ { "a": { "x": 10, "y": 20, "o": null, "z": 2 }, "b": 2 }, { "a": { "x": 10, "y": 20, "o": null, "z": 2 }, "b": 3 }, { "a": { "x": 10, "y": 20, "o": null, "z": 2 }, "b": 4 } ] }
           { "key": "B", "value": [ {"a": { "x": 40, "y": 30, "o": null, "z": 3 }, "b": 3 }, {"a": { "x": 40, "y": 30, "o": null, "z": 3 }, "b": 4 } ] }""",
    resultSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("a", new StructType()
            .add("x", IntegerType)
            .add("y", IntegerType)
            .add("o", IntegerType)
            .add("z", IntegerType))
          .add("b", IntegerType))),
    expectErrorWithoutEvolutionContains = "Cannot cast")

  testNestedStructsEvolution("nested columns resolved by name with same column count but different names - array of struct - longer target")(
    target = """{ "key": "A", "value": [ { "a": { "x": 1, "y": 2, "o": 4 }, "b": 1 }, { "a": { "x": 1, "y": 2, "o": 4 }, "b": 2 } ] }""",
    source =
      """{ "key": "A", "value": [ { "a": { "x": 10, "y": 20, "z": 2 }, "b": "2" } ] }
           { "key": "B", "value": [ {"a": { "x": 40, "y": 30, "z": 3 }, "b": "3" } ] }""",
    targetSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("a", new StructType()
            .add("x", IntegerType)
            .add("y", IntegerType)
            .add("o", IntegerType))
          .add("b", IntegerType))),
    sourceSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("a", new StructType()
            .add("x", IntegerType).add("y", IntegerType).add("z", IntegerType))
          .add("b", StringType))),
    clauses = update("*") :: insert("*") :: Nil,
    result =
      """{ "key": "A", "value": [ { "a": { "x": 10, "y": 20, "o": null, "z": 2 }, "b": 2 } ] }
           { "key": "B", "value": [ {"a": { "x": 40, "y": 30, "o": null, "z": 3 }, "b": 3 } ] }""",
    resultSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("a", new StructType()
            .add("x", IntegerType)
            .add("y", IntegerType)
            .add("o", IntegerType)
            .add("z", IntegerType))
          .add("b", IntegerType))),
    expectErrorWithoutEvolutionContains = "Cannot cast")

  testNestedStructsEvolution("nested columns resolved by name with same column count but different names - nested array of struct - longer source")(
    target = """{ "key": "A", "value": [ { "a": { "y": 2, "x": [ { "c": 1, "d": 3, "f": 4 } ] }, "b": 1 } ] }""",
    source =
      """{ "key": "A", "value": [ { "a": { "y": 20, "x": [ { "c": 10, "d": "30", "e": 1 }, { "c": 10, "d": "30", "e": 2 }, { "c": 10, "d": "30", "e": 3 } ] }, "b": "2" } ] }
          { "key": "B", "value": [ { "a": { "y": 60, "x": [ { "c": 20, "d": "50", "e": 2 }, { "c": 20, "d": "50", "e": 3 } ] }, "b": "3" } ] }""",
    targetSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("a", new StructType()
            .add("y", IntegerType)
            .add("x", ArrayType(
              new StructType()
                .add("c", IntegerType)
                .add("d", IntegerType)
                .add("f", IntegerType)
            )))
          .add("b", IntegerType))),
    sourceSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("a", new StructType()
            .add("y", IntegerType)
            .add("x", ArrayType(
              new StructType()
                .add("c", IntegerType)
                .add("d", StringType)
                .add("e", IntegerType)
            )))
          .add("b", StringType))),
    resultSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("a", new StructType()
            .add("y", IntegerType)
            .add("x", ArrayType(
              new StructType()
                .add("c", IntegerType)
                .add("d", IntegerType)
                .add("f", IntegerType)
                .add("e", IntegerType)
            )))
          .add("b", IntegerType))),
    clauses = update("*") :: insert("*") :: Nil,
    result =
      """{ "key": "A", "value": [ { "a": { "y": 20, "x": [ { "c": 10, "d": 30, "f": null, "e": 1 }, { "c": 10, "d": 30, "f": null, "e": 2 }, { "c": 10, "d": 30, "f": null, "e": 3 } ] }, "b": 2 } ] }
          { "key": "B", "value": [ { "a": { "y": 60, "x": [ { "c": 20, "d": 50, "f": null, "e": 2 }, { "c": 20, "d": 50, "f": null, "e": 3 } ] }, "b": 3} ] }""",
    expectErrorWithoutEvolutionContains = "Cannot cast")

  testNestedStructsEvolution("nested columns resolved by name with same column count but different names - nested array of struct - longer target")(
    target = """{ "key": "A", "value": [ { "a": { "y": 2, "x": [ { "c": 1, "d": 3, "f": 4 }, { "c": 1, "d": 3, "f": 4 } ] }, "b": 1 } ] }""",
    source =
      """{ "key": "A", "value": [ { "a": { "y": 20, "x": [ { "c": 10, "d": "30", "e": 1 } ] }, "b": "2" } ] }
          { "key": "B", "value": [ { "a": { "y": 60, "x": [ { "c": 20, "d": "50", "e": 2 } ] }, "b": "3" } ] }""",
    targetSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("a", new StructType()
            .add("y", IntegerType)
            .add("x", ArrayType(
              new StructType()
                .add("c", IntegerType)
                .add("d", IntegerType)
                .add("f", IntegerType)
            )))
          .add("b", IntegerType))),
    sourceSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("a", new StructType()
            .add("y", IntegerType)
            .add("x", ArrayType(
              new StructType()
                .add("c", IntegerType)
                .add("d", StringType)
                .add("e", IntegerType)
            )))
          .add("b", StringType))),
    resultSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("a", new StructType()
            .add("y", IntegerType)
            .add("x", ArrayType(
              new StructType()
                .add("c", IntegerType)
                .add("d", IntegerType)
                .add("f", IntegerType)
                .add("e", IntegerType)
            )))
          .add("b", IntegerType))),
    clauses = update("*") :: insert("*") :: Nil,
    result =
      """{ "key": "A", "value": [ { "a": { "y": 20, "x": [ { "c": 10, "d": 30, "f": null, "e": 1 } ] }, "b": 2 } ] }
          { "key": "B", "value": [ { "a": { "y": 60, "x": [ { "c": 20, "d": 50, "f": null, "e": 2 } ] }, "b": 3} ] }""",
    expectErrorWithoutEvolutionContains = "Cannot cast")
  // scalastyle:on line.size.limit

  testEvolution("nested columns resolved by position with same column count but different names")(
    targetData = Seq((1, 1, 2, 3)).toDF("key", "a", "b", "c")
      .selectExpr("key", "struct(a, b, c) as x"),
    sourceData = Seq((1, 10, 20, 30), (2, 20, 30, 40)).toDF("key", "a", "b", "d")
      .selectExpr("key", "struct(a, b, d) as x"),
    clauses = update("*") :: insert("*") :: Nil,
    expectErrorContains = "cannot cast",
    expectedWithoutEvolution = ((1, (10, 20, 30)) +: (2, (20, 30, 40)) +: Nil)
      .asInstanceOf[List[(Integer, (Integer, Integer, Integer))]]
      .toDF("key", "x")
      .selectExpr("key", "named_struct('a', x._1, 'b', x._2, 'c', x._3) as x"),
    confs = (DeltaSQLConf.DELTA_RESOLVE_MERGE_UPDATE_STRUCTS_BY_NAME.key, "false") +: Nil
  )

  // scalastyle:off line.size.limit
  testNestedStructsEvolution("nested columns resolved by position with same column count but different names - array of struct - longer source")(
    target = """{ "key": "A", "value": [{ "a": { "x": 1, "y": 2, "o": 4 }, "b": 1 } ] }""",
    source =
      """{ "key": "A", "value": [ { "a": { "x": 10, "y": 20, "z": 2 }, "b": "2" }, { "a": { "x": 10, "y": 20, "z": 3 }, "b": "2" }, { "a": { "x": 10, "y": 20, "z": 3 }, "b": "3" } ] }
           { "key": "B", "value": [ { "a": { "x": 40, "y": 30, "z": 3 }, "b": "3" }, { "a": { "x": 40, "y": 30, "z": 3 }, "b": "4" } ] }""",
    targetSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("a", new StructType()
            .add("x", IntegerType)
            .add("y", IntegerType)
            .add("o", IntegerType))
          .add("b", IntegerType))),
    sourceSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("a", new StructType()
            .add("x", IntegerType).add("y", IntegerType).add("z", IntegerType))
          .add("b", StringType))),
    clauses = update("*") :: insert("*") :: Nil,
    resultWithoutEvolution =
      """{ "key": "A", "value": [ { "a": { "x": 10, "y": 20, "o": 2 }, "b": 2 }, { "a": { "x": 10, "y": 20, "o": 3 }, "b": 2 }, { "a": { "x": 10, "y": 20, "o": 3 }, "b": 3 } ] }
           { "key": "B", "value": [ {"a": { "x": 40, "y": 30, "o": 3 }, "b": 3 }, {"a": { "x": 40, "y": 30, "o": 3 }, "b": 4 } ] }""",
    expectErrorContains = "cannot cast",
    confs = (DeltaSQLConf.DELTA_RESOLVE_MERGE_UPDATE_STRUCTS_BY_NAME.key, "false") +: Nil)

  testNestedStructsEvolution("nested columns resolved by position with same column count but different names - array of struct - longer target")(
    target = """{ "key": "A", "value": [{ "a": { "x": 1, "y": 2, "o": 4 }, "b": 1}, { "a": { "x": 1, "y": 2, "o": 4 }, "b": 2}] }""",
    source =
      """{ "key": "A", "value": [{ "a": { "x": 10, "y": 20, "z": 2 }, "b": "2" } ] }
           { "key": "B", "value": [{"a": { "x": 40, "y": 30, "z": 3 }, "b": "3" } ] }""",
    targetSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("a", new StructType()
            .add("x", IntegerType)
            .add("y", IntegerType)
            .add("o", IntegerType))
          .add("b", IntegerType))),
    sourceSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("a", new StructType()
            .add("x", IntegerType).add("y", IntegerType).add("z", IntegerType))
          .add("b", StringType))),
    clauses = update("*") :: insert("*") :: Nil,
    resultWithoutEvolution =
      """{ "key": "A", "value": [{ "a": { "x": 10, "y": 20, "o": 2}, "b": 2}] }
           { "key": "B", "value": [{"a": { "x": 40, "y": 30, "o": 3}, "b": 3}] }""",
    expectErrorContains = "cannot cast",
    confs = (DeltaSQLConf.DELTA_RESOLVE_MERGE_UPDATE_STRUCTS_BY_NAME.key, "false") +: Nil)

  testNestedStructsEvolution("nested columns resolved by position with same column count but different names - nested array of struct - longer source")(
    target = """{ "key": "A", "value": [ { "a": { "y": 2, "x": [ { "c": 1, "d": 3, "f": 4 } ] }, "b": 1 } ] }""",
    source =
      """{ "key": "A", "value": [ { "a": { "y": 20, "x": [ { "c": 10, "d": "30", "e": 1 }, { "c": 10, "d": "30", "e": 2 }, { "c": 10, "d": "30", "e": 3} ] }, "b": "2" } ] }
          { "key": "B", "value": [ { "a": { "y": 60, "x": [ { "c": 20, "d": "50", "e": 2 }, { "c": 20, "d": "50", "e": 3 } ] }, "b": "3" } ] }""",
    targetSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("a", new StructType()
            .add("y", IntegerType)
            .add("x", ArrayType(
              new StructType()
                .add("c", IntegerType)
                .add("d", IntegerType)
                .add("f", IntegerType)
            )))
          .add("b", IntegerType))),
    sourceSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("a", new StructType()
            .add("y", IntegerType)
            .add("x", ArrayType(
              new StructType()
                .add("c", IntegerType)
                .add("d", StringType)
                .add("e", IntegerType)
            )))
          .add("b", StringType))),
    clauses = update("*") :: insert("*") :: Nil,
    resultWithoutEvolution =
      """{ "key": "A", "value": [ { "a": {"y": 20, "x": [ { "c": 10, "d": 30, "f": 1 }, { "c": 10, "d": 30, "f": 2 }, { "c": 10, "d": 30, "f": 3 } ] }, "b": 2 } ] }
          { "key": "B", "value": [ { "a": {"y": 60, "x": [ { "c": 20, "d": 50, "f": 2 }, { "c": 20, "d": 50, "f": 3 } ] }, "b": 3}]}""",
    expectErrorContains = "cannot cast",
    confs = (DeltaSQLConf.DELTA_RESOLVE_MERGE_UPDATE_STRUCTS_BY_NAME.key, "false") +: Nil)

  testNestedStructsEvolution("nested columns resolved by position with same column count but different names - nested array of struct - longer target")(
    target = """{ "key": "A", "value": [{ "a": { "y": 2, "x": [ { "c": 1, "d": 3, "f": 5 }, { "c": 1, "d": 3, "f": 6 } ] }, "b": 1}] }""",
    source =
      """{ "key": "A", "value": [ { "a": { "y": 20, "x": [ { "c": 10, "d": "30", "e": 1 } ] }, "b": "2" } ] }
          { "key": "B", "value": [ { "a": { "y": 60, "x": [ { "c": 20, "d": "50", "e": 2 } ] }, "b": "3" } ] }""",
    targetSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("a", new StructType()
            .add("y", IntegerType)
            .add("x", ArrayType(
              new StructType()
                .add("c", IntegerType)
                .add("d", IntegerType)
                .add("f", IntegerType)
            )))
          .add("b", IntegerType))),
    sourceSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("a", new StructType()
            .add("y", IntegerType)
            .add("x", ArrayType(
              new StructType()
                .add("c", IntegerType)
                .add("d", StringType)
                .add("e", IntegerType)
            )))
          .add("b", StringType))),
    clauses = update("*") :: insert("*") :: Nil,
    resultWithoutEvolution =
      """{ "key": "A", "value": [ { "a": {"y": 20, "x": [ { "c": 10, "d": 30, "f": 1 } ] }, "b": 2 } ] }
          { "key": "B", "value": [ { "a": {"y": 60, "x": [ { "c": 20, "d": 50, "f": 2 } ] }, "b": 3 } ] }""",
    expectErrorContains = "cannot cast",
    confs = (DeltaSQLConf.DELTA_RESOLVE_MERGE_UPDATE_STRUCTS_BY_NAME.key, "false") +: Nil)
  // scalastyle:on line.size.limit

  testEvolution("struct in different order")(
    targetData = Seq((1, (1, 10, 100)), (2, (2, 20, 200))).toDF("key", "x")
      .selectExpr("key", "named_struct('a', x._1, 'b', x._2, 'c', x._3) as x"),
    sourceData = Seq((1, (1111, 111, 11)), (3, (3333, 333, 33))).toDF("key", "x")
      .selectExpr("key", "named_struct('c', x._1, 'b', x._2, 'a', x._3) as x"),
    clauses = update("*") :: insert("*") :: Nil,
    expected = ((1, (11, 111, 1111)) :: (2, (2, 20, 200)) :: (3, (33, 333, 3333)) :: Nil)
      .toDF("key", "x")
      .selectExpr("key", "named_struct('a', x._1, 'b', x._2, 'c', x._3) as x"),
    expectedWithoutEvolution =
      ((1, (11, 111, 1111)) :: (2, (2, 20, 200)) :: (3, (33, 333, 3333)) :: Nil)
      .toDF("key", "x")
      .selectExpr("key", "named_struct('a', x._1, 'b', x._2, 'c', x._3) as x")
  )

  // scalastyle:off line.size.limit
  testNestedStructsEvolution("struct in different order - array of struct")(
    target = """{ "key": "A", "value": [{ "a": { "x": 1, "y": 2 }, "b": 1 }] }""",
    source = """{ "key": "A", "value": [{ "b": "2", "a": { "y": 20, "x": 10}}, { "b": "3", "a": { "y": 30, "x": 40}}] }""",
    targetSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("a", new StructType().add("x", IntegerType).add("y", IntegerType))
          .add("b", IntegerType))),
    sourceSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("b", StringType)
          .add("a", new StructType().add("y", IntegerType).add("x", IntegerType)))),
    clauses = update("*") :: insert("*") :: Nil,
    result = """{ "key": "A", "value": [{ "a": { "x": 10, "y": 20 }, "b": 2 }, { "a": { "y": 30, "x": 40}, "b": 3 }] }""",
    resultWithoutEvolution = """{ "key": "A", "value": [{ "a": { "x": 10, "y": 20 }, "b": 2 }, { "a": { "y": 30, "x": 40}, "b": 3 }] }""")

  testNestedStructsEvolution("struct in different order - nested array of struct")(
    target = """{ "key": "A", "value": [{ "a": { "y": 2, "x": [{ "c": 1, "d": 3}]}, "b": 1 }] }""",
    source = """{ "key": "A", "value": [{ "b": "2", "a": {"x": [{ "d": "30", "c": 10}, { "d": "40", "c": 3}], "y": 20}}]}""",
    targetSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("a", new StructType()
            .add("y", IntegerType)
            .add("x", ArrayType(
              new StructType()
                .add("c", IntegerType)
                .add("d", IntegerType))))
          .add("b", IntegerType))),
    sourceSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("b", StringType)
          .add("a", new StructType()
            .add("x", ArrayType(
              new StructType()
                .add("d", StringType)
                .add("c", IntegerType)
            ))
            .add("y", IntegerType)))),
    clauses = update("*") :: insert("*") :: Nil,
    result = """{ "key": "A", "value": [{ "a": { "y": 20, "x": [{ "c": 10, "d": 30}, { "c": 3, "d": 40}]}, "b": 2 }]}""",
    resultWithoutEvolution = """{ "key": "A", "value": [{ "a": { "y": 20, "x": [{ "c": 10, "d": 30}, { "c": 3, "d": 40}]}, "b": 2 }]}""")
  // scalastyle:on line.size.limit

  testNestedStructsEvolution("array of struct with same columns but in different order" +
    " which can be casted implicitly - by name")(
    target = """{ "key": "A", "value": [ { "a": 1, "b": 2 } ] }""",
    source =
      """{ "key": "A", "value": [ { "b": 4, "a": 3 } ] }
          { "key": "B", "value": [ { "b": 2, "a": 5 } ] }""".stripMargin,
    targetSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("a", IntegerType)
          .add("b", IntegerType))),
    sourceSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("b", IntegerType)
          .add("a", IntegerType))),
    clauses = update("*") :: insert("*") :: Nil,
    result = """{ "key": "A", "value": [ { "a": 3, "b": 4 } ] }
                { "key": "B", "value": [ { "a": 5, "b": 2 } ] }""".stripMargin,
    resultWithoutEvolution = """{ "key": "A", "value": [ { "a": 3, "b": 4 } ] }
                { "key": "B", "value": [ { "a": 5, "b": 2 } ] }""".stripMargin)

  testNestedStructsEvolution("array of struct with same columns but in different order" +
    " which can be casted implicitly - by position")(
    target = """{ "key": "A", "value": [ { "a": 1, "b": 2 } ] }""",
    source = """{ "key": "A", "value": [ { "b": 4, "a": 3 } ] }
                  { "key": "B", "value": [ { "b": 2, "a": 5 } ] }""".stripMargin,
    targetSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("a", IntegerType)
          .add("b", IntegerType))),
    sourceSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("b", IntegerType)
          .add("a", IntegerType))),
    clauses = update("*") :: insert("*") :: Nil,
    result =
      """{ "key": "A", "value": [ { "a": 4, "b": 3 } ] }
          { "key": "B", "value": [ { "a": 2, "b": 5 } ] }""".stripMargin,
    resultWithoutEvolution =
      """{ "key": "A", "value": [ { "a": 4, "b": 3 } ] }
          { "key": "B", "value": [ { "a": 2, "b": 5 } ] }""".stripMargin,
    confs = (DeltaSQLConf.DELTA_RESOLVE_MERGE_UPDATE_STRUCTS_BY_NAME.key, "false") +: Nil)

  testNestedStructsEvolution("array of struct with same column count but all different names" +
    " - by name")(
    target = """{ "key": "A", "value": [ { "a": 1, "b": 2 } ] }""",
    source =
      """{ "key": "A", "value": [ { "c": 4, "d": 3 } ] }
          { "key": "B", "value": [ { "c": 2, "d": 5 } ] }""".stripMargin,
    targetSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("a", IntegerType)
          .add("b", IntegerType))),
    sourceSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("c", IntegerType)
          .add("d", IntegerType))),
    clauses = update("*") :: insert("*") :: Nil,
    resultSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("a", IntegerType)
          .add("b", IntegerType)
          .add("c", IntegerType)
          .add("d", IntegerType))),
    result =
      """{ "key": "A", "value": [ { "a": null, "b": null, "c": 4, "d": 3 } ] }
          { "key": "B", "value": [ { "a": null, "b": null, "c": 2, "d": 5 } ] }""".stripMargin,
    expectErrorWithoutEvolutionContains = "Cannot cast")

  testNestedStructsEvolution("array of struct with same column count but all different names" +
    " - by position")(
    target = """{ "key": "A", "value": [ { "a": 1, "b": 2 } ] }""",
    source = """{ "key": "A", "value": [ { "c": 4, "d": 3 } ] }
                  { "key": "B", "value": [ { "c": 2, "d": 5 } ] }""".stripMargin,
    targetSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("a", IntegerType)
          .add("b", IntegerType))),
    sourceSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("c", IntegerType)
          .add("d", IntegerType))),
    clauses = update("*") :: insert("*") :: Nil,
    resultWithoutEvolution =
      """{ "key": "A", "value": [ { "a": 4, "b": 3 } ] }
          { "key": "B", "value": [ { "a": 2, "b": 5 } ] }""".stripMargin,
    expectErrorContains = " cannot cast",
    confs = (DeltaSQLConf.DELTA_RESOLVE_MERGE_UPDATE_STRUCTS_BY_NAME.key, "false") +: Nil)

  testNestedStructsEvolution("array of struct with same columns but in different order" +
    " which cannot be casted implicitly - by name")(
    target = """{ "key": "A", "value": [ { "a": {"c" : 1}, "b": 2 } ] }""",
    source = """{ "key": "A", "value": [ { "b": 4, "a": {"c" : 3 } } ] }""",
    targetSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("a", new StructType().add("c", IntegerType))
          .add("b", IntegerType))),
    sourceSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("b", IntegerType)
          .add("a", new StructType().add("c", IntegerType)))),
    clauses = update("*") :: insert("*") :: Nil,
    result = """{ "key": "A", "value": [ { "a": { "c" : 3 }, "b": 4 } ] }""",
    resultWithoutEvolution = """{ "key": "A", "value": [ { "a": { "c" : 3 }, "b": 4 } ] }""")

  testNestedStructsEvolution("array of struct with same columns but in different order" +
    " which cannot be casted implicitly - by position")(
    target = """{ "key": "A", "value": [ { "a": {"c" : 1}, "b": 2 } ] }""",
    source = """{ "key": "A", "value": [ { "b": 4, "a": {"c" : 3 } } ] }""",
    targetSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("a", new StructType().add("c", IntegerType))
          .add("b", IntegerType))),
    sourceSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("b", IntegerType)
          .add("a", new StructType().add("c", IntegerType)))),
    clauses = update("*") :: insert("*") :: Nil,
    expectErrorContains = " cannot cast",
    expectErrorWithoutEvolutionContains = " cannot cast",
    confs = (DeltaSQLConf.DELTA_RESOLVE_MERGE_UPDATE_STRUCTS_BY_NAME.key, "false") +: Nil)

  testNestedStructsEvolution("array of struct with additional column in target - by name")(
    target = """{ "key": "A", "value": [ { "a": 1, "b": 2, "c": 3 } ] }""",
    source = """{ "key": "A", "value": [ { "b": 4, "a": 3 } ] }""",
    targetSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("a", IntegerType)
          .add("b", IntegerType)
          .add("c", IntegerType))),
    sourceSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("b", IntegerType)
          .add("a", IntegerType))),
    clauses = update("*") :: insert("*") :: Nil,
    result = """{ "key": "A", "value": [ { "a": 3, "b": 4, "c": null } ] }""",
    expectErrorWithoutEvolutionContains = "Cannot cast")

  testNestedStructsEvolution("array of struct with additional column in target - by position")(
    target = """{ "key": "A", "value": [ { "a": 1, "b": 2, "c": 3 } ] }""",
    source = """{ "key": "A", "value": [ { "b": 4, "a": 3 } ] }""",
    targetSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("a", IntegerType)
          .add("b", IntegerType)
          .add("c", IntegerType))),
    sourceSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("b", IntegerType)
          .add("a", IntegerType))),
    clauses = update("*") :: insert("*") :: Nil,
    expectErrorContains = " cannot cast",
    expectErrorWithoutEvolutionContains = "cannot cast",
    confs = (DeltaSQLConf.DELTA_RESOLVE_MERGE_UPDATE_STRUCTS_BY_NAME.key, "false") +: Nil)

  testNestedStructsEvolution("struct with extra source column not used in update, without fix")(
    target = """{ "key": 1, "value": { "a": 10 } }""",
    source =
      """{ "key": 1, "value": { "a": 11, "b": 21 } }
       { "key": 2, "value": { "a": 12, "b": 22 } }""".stripMargin,
    targetSchema = new StructType()
      .add("key", IntegerType)
      .add("value", new StructType()
        .add("a", IntegerType)),
    sourceSchema = new StructType()
      .add("key", IntegerType)
      .add("value", new StructType()
        .add("a", IntegerType)
        .add("b", IntegerType)),
    cond = "t.key = s.key",
    clauses = update(set = "key = 0") :: insert("*") :: Nil,
    expectErrorContains = "data type mismatch",
    expectErrorWithoutEvolutionContains = "cannot cast",
    confs = Seq(
      DeltaSQLConf.DELTA_MERGE_SCHEMA_EVOLUTION_FIX_NESTED_STRUCT_ALIGNMENT.key -> "false")
  )

  testNestedStructsEvolution("struct with extra source column not used in update")(
    target = """{ "key": 1, "value": { "a": 10 } }""",
    source =
    """{ "key": 1, "value": { "a": 11, "b": 21 } }
       { "key": 2, "value": { "a": 12, "b": 22 } }""".stripMargin,
    targetSchema = new StructType()
      .add("key", IntegerType)
      .add("value", new StructType()
        .add("a", IntegerType)),
    sourceSchema = new StructType()
      .add("key", IntegerType)
      .add("value", new StructType()
        .add("a", IntegerType)
        .add("b", IntegerType)),
    cond = "t.key = s.key",
    clauses = update(set = "key = 0") :: insert("*") :: Nil,
    result =
    """{ "key": 0, "value": { "a": 10, "b": null } }
       { "key": 2, "value": { "a": 12, "b": 22 } }""".stripMargin,
    resultSchema = new StructType()
      .add("key", IntegerType)
      .add("value", new StructType()
        .add("a", IntegerType)
        .add("b", IntegerType)),
    expectErrorWithoutEvolutionContains = "cannot cast",
    confs = Seq(
      DeltaSQLConf.DELTA_MERGE_SCHEMA_EVOLUTION_FIX_NESTED_STRUCT_ALIGNMENT.key -> "true")
  )

  testNestedStructsEvolution("array struct with extra source column not used in update")(
    target = """{ "key": 1, "value": [ { "a": 10 } ] }""",
    source =
      """{ "key": 1, "value": [ { "a": 11, "b": 21 } ] }
       { "key": 2, "value": [ { "a": 12, "b": 22 } ] }""".stripMargin,
    targetSchema = new StructType()
      .add("key", IntegerType)
      .add("value", ArrayType(
        new StructType()
          .add("a", IntegerType))),
    sourceSchema = new StructType()
      .add("key", IntegerType)
      .add("value", ArrayType(
        new StructType()
          .add("a", IntegerType)
          .add("b", IntegerType))),
    cond = "t.key = s.key",
    clauses = update(set = "key = 0") :: insert("*") :: Nil,
    result =
      """{ "key": 0, "value": [ { "a": 10, "b": null } ] }
       { "key": 2, "value": [ { "a": 12, "b": 22 } ] }""".stripMargin,
    resultSchema = new StructType()
      .add("key", IntegerType)
      .add("value", ArrayType(
        new StructType()
          .add("a", IntegerType)
          .add("b", IntegerType))),
    expectErrorWithoutEvolutionContains = "cannot cast",
    confs = Seq(
      DeltaSQLConf.DELTA_MERGE_SCHEMA_EVOLUTION_FIX_NESTED_STRUCT_ALIGNMENT.key -> "true")
  )

  testNestedStructsEvolution("nested struct with extra source column not used in update")(
    target = """{ "key": 1, "value": { "a": { "aa": 1 } } }""",
    source =
      """{ "key": 1, "value": { "a": { "aa": 11, "bb": 31 }, "b": 21 } }
       { "key": 2, "value": { "a": { "aa": 12, "bb": 32 }, "b": 22 } }""".stripMargin,
    targetSchema = new StructType()
      .add("key", IntegerType)
      .add("value", new StructType()
        .add("a", new StructType()
        .add("aa", IntegerType))),
    sourceSchema = new StructType()
      .add("key", IntegerType)
      .add("value", new StructType()
        .add("a", new StructType()
        .add("aa", IntegerType)
        .add("bb", IntegerType))
        .add("b", IntegerType)),
    cond = "t.key = s.key",
    clauses = update(set = "key = 0") :: insert("*") :: Nil,
    result =
      """{ "key": 0, "value": { "a": { "aa": 1, "bb": null }, "b": null } }
       { "key": 2, "value": { "a": { "aa": 12, "bb": 32 }, "b": 22 } }""".stripMargin,
    resultSchema = new StructType()
      .add("key", IntegerType)
      .add("value", new StructType()
        .add("a", new StructType()
        .add("aa", IntegerType)
        .add("bb", IntegerType))
        .add("b", IntegerType)),
    expectErrorWithoutEvolutionContains = "cannot cast",
    confs = Seq(
      DeltaSQLConf.DELTA_MERGE_SCHEMA_EVOLUTION_FIX_NESTED_STRUCT_ALIGNMENT.key -> "true")
  )
}

trait MergeIntoNestedStructEvolutionUpdateOnlyTests extends MergeIntoSchemaEvolutionMixin {
  self: MergeIntoTestUtils with SharedSparkSession =>

  import testImplicits._

  // Nested Schema evolution with UPDATE alone
  testNestedStructsEvolution("new nested source field not in update is ignored")(
    target = """{ "key": "A", "value": { "a": 1 } }""",
    source = """{ "key": "A", "value": { "a": 2, "b": 3 } }""",
    targetSchema = new StructType()
      .add("key", StringType)
      .add("value", new StructType()
        .add("a", IntegerType)),
    sourceSchema = new StructType()
      .add("key", StringType)
      .add("value", new StructType()
        .add("a", IntegerType)
        .add("b", IntegerType)),
    clauses = update("value.a = s.value.a") :: Nil,
    result = """{ "key": "A", "value": { "a": 2 } }""",
    resultWithoutEvolution = """{ "key": "A", "value": { "a": 2 } }""")

  testNestedStructsEvolution("two new nested source fields with update: one added, one ignored")(
    target = """{ "key": "A", "value": { "a": 1 } }""",
    source = """{ "key": "A", "value": { "a": 2, "b": 3, "c": 4 } }""",
    targetSchema = new StructType()
      .add("key", StringType)
      .add("value", new StructType()
        .add("a", IntegerType)),
    sourceSchema = new StructType()
      .add("key", StringType)
      .add("value", new StructType()
        .add("a", IntegerType)
        .add("b", IntegerType)
        .add("c", IntegerType)),
    clauses = update("value.b = s.value.b") :: Nil,
    result = """{ "key": "A", "value": { "a": 1, "b": 3 } }""",
    resultSchema = new StructType()
      .add("key", StringType)
      .add("value", new StructType()
        .add("a", IntegerType)
        .add("b", IntegerType)),
    expectErrorWithoutEvolutionContains = "No such struct field")

  testNestedStructsEvolution("nested void columns are not allowed")(
    target = """{ "key": "A", "value": { "a": { "x": 1 }, "b": 1 } }""",
    source = """{ "key": "A", "value": { "a": { "x": 2, "z": null } }""",
    targetSchema = new StructType()
      .add("key", StringType)
      .add("value",
        new StructType()
          .add("a", new StructType().add("x", IntegerType))
          .add("b", IntegerType)),
    sourceSchema = new StructType()
      .add("key", StringType)
      .add("value",
        new StructType()
          .add("a", new StructType().add("x", IntegerType).add("z", NullType))),
    clauses = update("*") :: Nil,
    expectErrorContains = "Cannot add column `value`.`a`.`z` with type VOID",
    expectErrorWithoutEvolutionContains = "All nested columns must match")
  for (isPartitioned <- BOOLEAN_DOMAIN)
    testEvolution(s"extra nested column in source - update, isPartitioned=$isPartitioned")(
      targetData = Seq((1, (1, 10)), (2, (2, 2000))).toDF("key", "x")
        .selectExpr("key", "named_struct('a', x._1, 'c', x._2) as x"),
      sourceData = Seq((1, (10, 100, 1000))).toDF("key", "x")
        .selectExpr("key", "named_struct('a', x._1, 'b', x._2, 'c', x._3) as x"),
      clauses = update("*") :: Nil,
      expected = ((1, (10, 100, 1000)) +: (2, (2, null, 2000)) +: Nil)
        .asInstanceOf[List[(Integer, (Integer, Integer, Integer))]].toDF("key", "x")
        .selectExpr("key", "named_struct('a', x._1, 'c', x._3, 'b', x._2) as x"),
      expectErrorWithoutEvolutionContains = "Cannot cast",
      partitionCols = if (isPartitioned) Seq("key") else Seq.empty
    )

  testEvolution("extra nested column in source - update, partition on unused column")(
    targetData = Seq((1, 2, (1, 10)), (2, 2, (2, 2000))).toDF("key", "part", "x")
      .selectExpr("part", "key", "named_struct('a', x._1, 'c', x._2) as x"),
    sourceData = Seq((1, 2, (10, 100, 1000))).toDF("key", "part", "x")
      .selectExpr("key", "part", "named_struct('a', x._1, 'b', x._2, 'c', x._3) as x"),
    clauses = update("*") :: Nil,
    expected = ((1, 2, (10, 100, 1000)) +: (2, 2, (2, null, 2000)) +: Nil)
      .asInstanceOf[List[(Integer, Integer, (Integer, Integer, Integer))]].toDF("key", "part", "x")
      .selectExpr("part", "key", "named_struct('a', x._1, 'c', x._3, 'b', x._2) as x"),
    expectErrorWithoutEvolutionContains = "Cannot cast",
    partitionCols = Seq("part")
  )

  // scalastyle:off line.size.limit
  testNestedStructsEvolution("extra nested column in source - update - array of struct - longer source")(
    target =
      """{ "key": "A", "value": [ { "a": { "x": 1, "y": 2 }, "b": 1 } ] }
         { "key": "B", "value": [ { "a": { "x": 40, "y": 30 }, "b": 3 } ] }""",
    source =
      """{ "key": "A", "value": [ { "a": { "x": 10, "y": 20, "z": 2 }, "b": "2" }, { "a": { "x": 10, "y": 20, "z": 2 }, "b": "3" }, { "a": { "x": 10, "y": 20, "z": 2 }, "b": "4" } ] }""",
    targetSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("a", new StructType()
            .add("x", IntegerType)
            .add("y", IntegerType))
          .add("b", IntegerType))),
    sourceSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("a", new StructType()
            .add("x", IntegerType).add("y", IntegerType).add("z", IntegerType))
          .add("b", StringType))),
    clauses = update("*") :: Nil,
    result =
      """{ "key": "A", "value": [ { "a": { "x": 10, "y": 20, "z": 2 }, "b": 2 }, { "a": { "x": 10, "y": 20, "z": 2 }, "b": 3 }, { "a": { "x": 10, "y": 20, "z": 2 }, "b": 4 } ] }
         { "key": "B", "value": [ { "a": { "x": 40, "y": 30, "z": null }, "b": 3 } ] }""",
    resultSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("a", new StructType()
            .add("x", IntegerType)
            .add("y", IntegerType)
            .add("z", IntegerType))
          .add("b", IntegerType))),
    expectErrorWithoutEvolutionContains = "Cannot cast")

  testNestedStructsEvolution("extra nested column in source - update - array of struct - longer target")(
    target =
      """{ "key": "A", "value": [ { "a": { "x": 1, "y": 2 }, "b": 1 }, { "a": { "x": 1, "y": 2 }, "b": 2 } ] }
         { "key": "B", "value": [ { "a": { "x": 40, "y": 30 }, "b": 3 }, { "a": { "x": 40, "y": 30 }, "b": 4 }, { "a": { "x": 40, "y": 30 }, "b": 5 } ] }""",
    source =
      """{ "key": "A", "value": [ { "a": { "x": 10, "y": 20, "z": 2 }, "b": "2" } ] }""",
    targetSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("a", new StructType()
            .add("x", IntegerType)
            .add("y", IntegerType))
          .add("b", IntegerType))),
    sourceSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("a", new StructType()
            .add("x", IntegerType).add("y", IntegerType).add("z", IntegerType))
          .add("b", StringType))),
    clauses = update("*") :: Nil,
    result =
      """{ "key": "A", "value": [ { "a": { "x": 10, "y": 20, "z": 2 }, "b": 2 } ] }
         { "key": "B", "value": [ { "a": { "x": 40, "y": 30, "z": null }, "b": 3 }, { "a": { "x": 40, "y": 30, "z": null }, "b": 4 }, { "a": { "x": 40, "y": 30, "z": null }, "b": 5 } ] }""".stripMargin,
    resultSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("a", new StructType()
            .add("x", IntegerType)
            .add("y", IntegerType)
            .add("z", IntegerType))
          .add("b", IntegerType))),
    expectErrorWithoutEvolutionContains = "Cannot cast")

  testNestedStructsEvolution("extra nested column in source - update - nested array of struct - longer source")(
    target =
      """{ "key": "A", "value": [ { "a": { "y": 2, "x": [ { "c": 1, "d": 3 } ] }, "b": 1 } ] }
         { "key": "B", "value": [ { "a": { "y": 60, "x": [ { "c": 20, "d": 50 } ] }, "b": 3 } ] }""",
    source =
      """{ "key": "A", "value": [ { "a": { "y": 20, "x": [ { "c": 10, "d": "30", "e": 1 }, { "c": 10, "d": "30", "e": 2 }, { "c": 10, "d": "30", "e": 3 } ] }, "b": 2 } ] }""",
    targetSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("a", new StructType()
            .add("y", IntegerType)
            .add("x", ArrayType(
              new StructType()
                .add("c", IntegerType)
                .add("d", IntegerType)
            )))
          .add("b", IntegerType))),
    sourceSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("a", new StructType()
            .add("y", IntegerType)
            .add("x", ArrayType(
              new StructType()
                .add("c", IntegerType)
                .add("d", StringType)
                .add("e", IntegerType)
            )))
          .add("b", StringType))),
    resultSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("a", new StructType()
            .add("y", IntegerType)
            .add("x", ArrayType(
              new StructType()
                .add("c", IntegerType)
                .add("d", IntegerType)
                .add("e", IntegerType)
            )))
          .add("b", IntegerType))),
    clauses = update("*") :: Nil,
    result =
      """{ "key": "A", "value": [ { "a": { "y": 20, "x": [ { "c": 10, "d": 30, "e": 1 }, { "c": 10, "d": 30, "e": 2 }, { "c": 10, "d": 30, "e": 3 } ] }, "b": 2 } ] }
         { "key": "B", "value": [ { "a": { "y": 60, "x": [ { "c": 20, "d": 50, "e": null } ] }, "b": 3 } ] }""",
    expectErrorWithoutEvolutionContains = "Cannot cast")

  testNestedStructsEvolution("extra nested column in source - update - nested array of struct - longer target")(
    target =
      """{ "key": "A", "value": [ { "a": { "y": 2, "x": [ { "c": 1, "d": 3 }, { "c": 1, "d": 2 } ] }, "b": 1 } ] }
         { "key": "B", "value": [ { "a": { "y": 60, "x": [ { "c": 20, "d": 50 }, { "c": 20, "d": 40 }, { "c": 20, "d": 60 } ] }, "b": 3 } ] }""",
    source =
      """{ "key": "A", "value": [ { "a": { "y": 20, "x": [ { "c": 10, "d": "30", "e": 1 } ] }, "b": "2" } ] }""",
    targetSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("a", new StructType()
            .add("y", IntegerType)
            .add("x", ArrayType(
              new StructType()
                .add("c", IntegerType)
                .add("d", IntegerType)
            )))
          .add("b", IntegerType))),
    sourceSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("a", new StructType()
            .add("y", IntegerType)
            .add("x", ArrayType(
              new StructType()
                .add("c", IntegerType)
                .add("d", StringType)
                .add("e", IntegerType)
            )))
          .add("b", StringType))),
    resultSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("a", new StructType()
            .add("y", IntegerType)
            .add("x", ArrayType(
              new StructType()
                .add("c", IntegerType)
                .add("d", IntegerType)
                .add("e", IntegerType)
            )))
          .add("b", IntegerType))),
    clauses = update("*") :: Nil,
    result =
      """{ "key": "A", "value": [ { "a": { "y": 20, "x": [ { "c": 10, "d": 30, "e": 1 } ] }, "b": 2 } ] }
         { "key": "B", "value": [ { "a": { "y": 60, "x": [ { "c": 20, "d": 50, "e": null }, { "c": 20, "d": 40, "e": null }, { "c": 20, "d": 60, "e": null } ] }, "b": 3 } ] }""",
    expectErrorWithoutEvolutionContains = "Cannot cast")
  // scalastyle:on line.size.limit

  testEvolution("missing nested column in source - update")(
    targetData = Seq((1, (1, 10, 100)), (2, (2, 20, 200))).toDF("key", "x")
      .selectExpr("key", "named_struct('a', x._1, 'b', x._2, 'c', x._3) as x"),
    sourceData = Seq((1, (0, 0))).toDF("key", "x")
      .selectExpr("key", "named_struct('a', x._1, 'c', x._2) as x"),
    clauses = update("*") :: Nil,
    expected = ((1, (0, 10, 0)) +: (2, (2, 20, 200)) +: Nil).toDF("key", "x")
      .selectExpr("key", "named_struct('a', x._1, 'b', x._2, 'c', x._3) as x"),
    expectErrorWithoutEvolutionContains = "Cannot cast"
  )

  // scalastyle:off line.size.limit
  testNestedStructsEvolution("missing nested column in source - update - array of struct - longer source")(
    target = """{ "key": "A", "value": [ { "a": { "x": 1, "y": 2, "z": 3 }, "b": 1 } ] }""",
    source =
      """{ "key": "A", "value": [ { "a": { "x": 10, "z": 2 }, "b": "2" }, { "a": { "x": 10, "z": 2 }, "b": "3" }, { "a": { "x": 10, "z": 2 }, "b": "4" } ] }
           { "key": "B", "value": [ { "a": { "x": 40, "z": 3 }, "b": "3" } ] }""",
    targetSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("a", new StructType()
            .add("x", IntegerType)
            .add("y", IntegerType)
            .add("z", IntegerType))
          .add("b", IntegerType))),
    sourceSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("a", new StructType()
            .add("x", IntegerType).add("z", IntegerType))
          .add("b", StringType))),
    clauses = update("*") :: Nil,
    result =
      """{ "key": "A", "value": [ { "a": { "x": 10, "y": null, "z": 2 }, "b": 2 }, { "a": { "x": 10, "y": null, "z": 2 }, "b": 3 }, { "a": { "x": 10, "y": null, "z": 2 }, "b": 4 } ] }""",
    resultSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("a", new StructType()
            .add("x", IntegerType)
            .add("y", IntegerType)
            .add("z", IntegerType))
          .add("b", IntegerType))),
    expectErrorWithoutEvolutionContains = "Cannot cast")

  // scalastyle:off line.size.limit
  testNestedStructsEvolution("missing nested column in source - update - array of struct - longer target")(
    target = """{ "key": "A", "value": [ { "a": { "x": 1, "y": 2, "z": 3 }, "b": 1 }, { "a": { "x": 1, "y": 2, "z": 3 }, "b": 2 } ] }""",
    source =
      """{ "key": "A", "value": [ { "a": { "x": 10, "z": 2 }, "b": "2" } ] }
           { "key": "B", "value": [ { "a": { "x": 40, "z": 3 }, "b": "3" } ] }""",
    targetSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("a", new StructType()
            .add("x", IntegerType)
            .add("y", IntegerType)
            .add("z", IntegerType))
          .add("b", IntegerType))),
    sourceSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("a", new StructType()
            .add("x", IntegerType).add("z", IntegerType))
          .add("b", StringType))),
    clauses = update("*") :: Nil,
    result =
      """{ "key": "A", "value": [ { "a": { "x": 10, "y": null, "z": 2 }, "b": 2 } ] }""",
    resultSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("a", new StructType()
            .add("x", IntegerType)
            .add("y", IntegerType)
            .add("z", IntegerType))
          .add("b", IntegerType))),
    expectErrorWithoutEvolutionContains = "Cannot cast")

  testNestedStructsEvolution("missing nested column in source - update - nested array of struct - longer source")(
    target = """{ "key": "A", "value": [ { "a": { "y": 2, "x": [ { "c": 1, "d": 3, "e": 4 } ] }, "b": 1 } ] }""",
    source =
      """{ "key": "A", "value": [ { "a": {"y": 20, "x": [ { "c": 10, "e": 1 }, { "c": 10, "e": 2 }, { "c": 10, "e": 3 } ] }, "b": "2" } ] }
          { "key": "B", "value": [ { "a": {"y": 60, "x":  [{ "c": 20, "e": 2 } ] }, "b": "3" } ] }""",
    targetSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("a", new StructType()
            .add("y", IntegerType)
            .add("x", ArrayType(
              new StructType()
                .add("c", IntegerType)
                .add("d", IntegerType)
                .add("e", IntegerType)
            )))
          .add("b", IntegerType))),
    sourceSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("a", new StructType()
            .add("y", IntegerType)
            .add("x", ArrayType(
              new StructType()
                .add("c", IntegerType)
                .add("e", IntegerType)
            )))
          .add("b", StringType))),
    resultSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("a", new StructType()
            .add("y", IntegerType)
            .add("x", ArrayType(
              new StructType()
                .add("c", IntegerType)
                .add("d", IntegerType)
                .add("e", IntegerType)
            )))
          .add("b", IntegerType))),
    clauses = update("*") :: Nil,
    result =
      """{ "key": "A", "value": [ { "a": { "y": 20, "x": [ { "c": 10, "d": null, "e": 1 }, { "c": 10, "d": null, "e": 2 }, { "c": 10, "d": null, "e": 3} ] }, "b": 2 } ] }""",
    expectErrorWithoutEvolutionContains = "Cannot cast")

  testNestedStructsEvolution("missing nested column in source - update - nested array of struct - longer target")(
    target = """{ "key": "A", "value": [ { "a": { "y": 2, "x": [ { "c": 1, "d": 3, "e": 4 }, { "c": 1, "d": 3, "e": 5 } ] }, "b": 1 } ] }""",
    source =
      """{ "key": "A", "value": [ { "a": { "y": 20, "x": [ { "c": 10, "e": 1 } ] }, "b": "2" } ] }
          { "key": "B", "value": [ { "a": { "y": 60, "x": [ { "c": 20, "e": 2 } ] }, "b": "3" } ] }""",
    targetSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("a", new StructType()
            .add("y", IntegerType)
            .add("x", ArrayType(
              new StructType()
                .add("c", IntegerType)
                .add("d", IntegerType)
                .add("e", IntegerType)
            )))
          .add("b", IntegerType))),
    sourceSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("a", new StructType()
            .add("y", IntegerType)
            .add("x", ArrayType(
              new StructType()
                .add("c", IntegerType)
                .add("e", IntegerType)
            )))
          .add("b", StringType))),
    resultSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("a", new StructType()
            .add("y", IntegerType)
            .add("x", ArrayType(
              new StructType()
                .add("c", IntegerType)
                .add("d", IntegerType)
                .add("e", IntegerType)
            )))
          .add("b", IntegerType))),
    clauses = update("*") :: Nil,
    result =
      """{ "key": "A", "value": [ { "a": { "y": 20, "x": [ { "c": 10, "d": null, "e": 1 } ] }, "b": 2 } ] }""",
    expectErrorWithoutEvolutionContains = "Cannot cast")
  // scalastyle:on line.size.limit

  testNestedStructsEvolution("add non-nullable struct field to target schema")(
    target = """{ "key": "A" }""",
    source = """{ "key": "B", "value": 4}""",
    targetSchema = new StructType()
      .add("key", StringType),
    sourceSchema = new StructType()
      .add("key", StringType)
      .add("value", IntegerType, nullable = false),
    clauses = update("*") :: Nil,
    result = """{ "key": "A", "value": null }""".stripMargin,
    // Even though `value` is non-nullable in the source, it must be nullable in the target as
    // existing rows will contain null values.
    resultSchema = new StructType()
      .add("key", StringType)
      .add("value", IntegerType, nullable = true),
    resultWithoutEvolution = """{ "key": "A" }""")

  testNestedStructsEvolution("struct in array with storeAssignmentPolicy = STRICT")(
    target = """{ "key": "A", "value": [ { "a": 1 } ] }""",
    source = """{ "key": "A", "value": [ { "a": 2 } ] }""",
    targetSchema = new StructType()
      .add("key", StringType)
      .add("value",
        ArrayType(new StructType()
          .add("a", LongType))),
    sourceSchema = new StructType()
      .add("key", StringType)
      .add("value",
        ArrayType(new StructType()
          .add("a", IntegerType))),
    clauses = update("*") :: Nil,
    result = """{ "key": "A", "value": [ { "a": 2 } ] }""",
    resultWithoutEvolution = """{ "key": "A", "value": [ { "a": 2 } ] }""",
    confs = Seq(
      SQLConf.STORE_ASSIGNMENT_POLICY.key -> StoreAssignmentPolicy.STRICT.toString,
      DeltaSQLConf.UPDATE_AND_MERGE_CASTING_FOLLOWS_ANSI_ENABLED_FLAG.key -> "false"))

  testNestedStructsEvolution("nested field assignment qualified with source alias")(
    target = Seq("""{ "a": 1, "t": { "a": 2 } }"""),
    source = Seq("""{ "a": 3, "t": { "a": 5 } }"""),
    targetSchema = new StructType()
      .add("a", IntegerType)
      .add("t", new StructType()
        .add("a", IntegerType)),
    sourceSchema = new StructType()
      .add("a", IntegerType)
      .add("t", new StructType()
        .add("a", IntegerType)),
    cond = "1 = 1",
    clauses = update("s.t.a = s.t.a") :: Nil,
    // Assigning to the source is just wrong and should fail.
    result = Seq("""{ "a": 1, "t": { "a": 5 } }"""),
    resultSchema = new StructType()
      .add("a", IntegerType)
      .add("t", new StructType()
        .add("a", IntegerType)),
    expectErrorWithoutEvolutionContains = "cannot resolve s.t.a in UPDATE")

  testNestedStructsEvolution("existing top-level column assignment qualified with target alias")(
    target = Seq("""{ "a": 1, "t": { "a": 2 } }"""),
    source = Seq("""{ "a": 3, "t": { "a": 5 } }"""),
    targetSchema = new StructType()
      .add("a", IntegerType)
      .add("t", new StructType()
        .add("a", IntegerType)),
    sourceSchema = new StructType()
      .add("a", IntegerType)
      .add("t", new StructType()
        .add("a", IntegerType)),
    cond = "1 = 1",
    // This succeeds and updates 'a': the fully qualified column name 't.a' gets precedence over
    // the unqualified struct field name '(t.)t.a' to resolve the ambiguity.
    clauses = update("t.a = s.a") :: Nil,
    result = Seq("""{ "a": 3, "t": { "a": 2 } }"""),
    resultSchema = new StructType()
      .add("a", IntegerType)
      .add("t", new StructType()
        .add("a", IntegerType)),
    resultWithoutEvolution = Seq("""{ "a": 3, "t": { "a": 2 } }"""))

  testNestedStructsEvolution("existing nested field assignment qualified with target alias")(
    target = Seq("""{ "a": 1, "t": { "a": 2 } }"""),
    source = Seq("""{ "a": 3, "t": { "a": 5 } }"""),
    targetSchema = new StructType()
      .add("a", IntegerType)
      .add("t", new StructType()
        .add("a", IntegerType)),
    sourceSchema = new StructType()
      .add("a", IntegerType)
      .add("t", new StructType()
        .add("a", IntegerType)),
    cond = "1 = 1",
    // This is unambiguous: and resolves to the struct field 't.t.a' during resolution
    clauses = update("t.t.a = s.t.a") :: Nil,
    result = Seq("""{ "a": 1, "t": { "a": 5 } }"""),
    resultSchema = new StructType()
      .add("a", IntegerType)
      .add("t", new StructType()
        .add("a", IntegerType)),
    resultWithoutEvolution = Seq("""{ "a": 1, "t": { "a": 5 } }"""))

  testNestedStructsEvolution("new top-level column assignment qualified with target alias")(
    target = Seq("""{ "a": 1, "t": { "a": 2 } }"""),
    source = Seq("""{ "a": 3, "b": 4, "t": { "a": 5, "b": 6 } }"""),
    targetSchema = new StructType()
      .add("a", IntegerType)
      .add("t", new StructType()
        .add("a", IntegerType)),
    sourceSchema = new StructType()
      .add("a", IntegerType)
      .add("b", IntegerType)
      .add("t", new StructType()
        .add("a", IntegerType)
        .add("b", IntegerType)),
    cond = "1 = 1",
    clauses = update("t.b = s.b") :: Nil,
    result = Seq("""{ "a": 1, "t": { "a": 2, "b": 4 } }"""),
    resultSchema = new StructType()
      .add("a", IntegerType)
      .add("t", new StructType()
        .add("a", IntegerType)
        .add("b", IntegerType)),
    expectErrorWithoutEvolutionContains = "No such struct field `b` in `a`")

  testNestedStructsEvolution("new nested field assignment qualified with target alias")(
    target = Seq("""{ "a": 1, "t": { "a": 2 } }"""),
    source = Seq("""{ "a": 3, "b": 4, "t": { "a": 5, "b": 6 } }"""),
    targetSchema = new StructType()
      .add("a", IntegerType)
      .add("t", new StructType()
        .add("a", IntegerType)),
    sourceSchema = new StructType()
      .add("a", IntegerType)
      .add("b", IntegerType)
      .add("t", new StructType()
        .add("a", IntegerType)
        .add("b", IntegerType)),
    cond = "1 = 1",
    clauses = update("t.t.b = s.t.b") :: Nil,
    // t.t.b gets resolved to source struct t, accessing nested field t.t.b which doesn't exist.
    expectErrorContains = "No such struct field `t` in `a`, `b`",
    resultSchema = new StructType()
      .add("a", IntegerType)
      .add("t", new StructType()
        .add("a", IntegerType)
        .add("b", IntegerType)),
    // t.t.b: t.t gets resolved to target t with nested field b which doesn't exist in fields (a)
    expectErrorWithoutEvolutionContains = "No such struct field `b` in `a`")
}
