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

package org.apache.spark.sql.delta.typewidening

import org.apache.spark.sql.delta._
import org.apache.spark.sql.delta.catalog.DeltaTableV2
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.hadoop.fs.Path

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Dataset, QueryTest, Row, SaveMode}
import org.apache.spark.sql.catalyst.plans.logical.{AppendData, LogicalPlan}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.StoreAssignmentPolicy
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap


/**
 * Suite covering widening columns and fields type as part of automatic schema evolution in INSERT
 * when the type widening table feature is supported.
 */
class TypeWideningInsertSchemaEvolutionSuite
    extends QueryTest
    with DeltaDMLTestUtils
    with TypeWideningTestMixin
    with TypeWideningInsertSchemaEvolutionTests {

  protected override def sparkConf: SparkConf = {
    super.sparkConf
      .set(DeltaSQLConf.DELTA_SCHEMA_AUTO_MIGRATE.key, "true")
  }
}

/**
 * Tests covering type widening during schema evolution in INSERT.
 */
trait TypeWideningInsertSchemaEvolutionTests extends TypeWideningTestCases {
  self: QueryTest with TypeWideningTestMixin with DeltaDMLTestUtils =>

  import testImplicits._
  import scala.collection.JavaConverters._

  for {
    testCase <- supportedTestCases
  } {
    test(s"INSERT - automatic type widening ${testCase.fromType.sql} -> ${testCase.toType.sql}") {
      append(testCase.initialValuesDF)
      testCase.additionalValuesDF
        .write
        .mode("append")
        .insertInto(s"delta.`$tempPath`")

      assert(readDeltaTable(tempPath).schema("value").dataType === testCase.toType)
      checkAnswerWithTolerance(
        actualDf = readDeltaTable(tempPath).select("value"),
        expectedDf = testCase.expectedResult.select($"value".cast(testCase.toType)),
        toType = testCase.toType
      )
    }
  }

  for {
    testCase <- unsupportedTestCases ++ alterTableOnlySupportedTestCases
  } {
    test(s"INSERT - unsupported automatic type widening " +
      s"${testCase.fromType.sql} -> ${testCase.toType.sql}") {
      append(testCase.initialValuesDF)
      // Test cases for some of the unsupported type changes may overflow while others only have
      // values that can be implicitly cast to the narrower type - e.g. double ->float.
      // We set storeAssignmentPolicy to LEGACY to ignore overflows, this test only ensures
      // that the table schema didn't evolve.
      withSQLConf(SQLConf.STORE_ASSIGNMENT_POLICY.key -> StoreAssignmentPolicy.LEGACY.toString) {
        testCase.additionalValuesDF.write.mode("append")
          .insertInto(s"delta.`$tempPath`")
        assert(readDeltaTable(tempPath).schema("value").dataType === testCase.fromType)
      }
    }
  }

  test("INSERT - type widening isn't applied when schema evolution is disabled") {
    sql(s"CREATE TABLE delta.`$tempPath` (a short) USING DELTA")
    withSQLConf(DeltaSQLConf.DELTA_SCHEMA_AUTO_MIGRATE.key -> "false") {
      // Insert integer values. This should succeed and downcast the values to short.
      sql(s"INSERT INTO delta.`$tempPath` VALUES (1), (2)")
      assert(readDeltaTable(tempPath).schema("a").dataType === ShortType)
      checkAnswer(readDeltaTable(tempPath),
        Seq(1, 2).toDF("a").select($"a".cast(ShortType)))
    }

    // Check that we would actually widen if schema evolution was enabled.
    withSQLConf(DeltaSQLConf.DELTA_SCHEMA_AUTO_MIGRATE.key -> "true") {
      sql(s"INSERT INTO delta.`$tempPath` VALUES (3), (4)")
      assert(readDeltaTable(tempPath).schema("a").dataType === IntegerType)
      checkAnswer(readDeltaTable(tempPath), Seq(1, 2, 3, 4).toDF("a"))
    }
  }

  test("INSERT - type widening isn't applied when it's disabled") {
    sql(s"CREATE TABLE delta.`$tempPath` (a short) USING DELTA")
    enableTypeWidening(tempPath, enabled = false)
    sql(s"INSERT INTO delta.`$tempPath` VALUES (1), (2)")
    assert(readDeltaTable(tempPath).schema("a").dataType === ShortType)
    checkAnswer(readDeltaTable(tempPath),
      Seq(1, 2).toDF("a").select($"a".cast(ShortType)))
  }

  /**
   * Short-hand to create a logical plan to insert into the table. This captures the state of the
   * table at the time the method is called, e.p. the type widening property value that will be used
   * during analysis.
   */
  private def createInsertPlan(df: DataFrame): LogicalPlan = {
    val relation = DataSourceV2Relation.create(
      table = DeltaTableV2(spark, new Path(tempPath)),
      catalog = None,
      identifier = None,
      options = new CaseInsensitiveStringMap(Map.empty[String, String].asJava)
    )
    AppendData.byPosition(relation, df.queryExecution.logical)
  }

  test(s"INSERT - fail if type widening gets enabled by a concurrent transaction") {
    sql(s"CREATE TABLE delta.`$tempPath` (a short) USING DELTA")
    enableTypeWidening(tempPath, enabled = false)
    val insert = createInsertPlan(Seq(1).toDF("a"))
    // Enabling type widening after analysis doesn't impact the insert operation: the data is
    // already cast to conform to the current schema.
    enableTypeWidening(tempPath, enabled = true)
    Dataset.ofRows(spark, insert).collect()
    assert(readDeltaTable(tempPath).schema == new StructType().add("a", ShortType))
    checkAnswer(readDeltaTable(tempPath), Row(1))
  }

  test(s"INSERT - fail if type widening gets disabled by a concurrent transaction") {
    sql(s"CREATE TABLE delta.`$tempPath` (a short) USING DELTA")
    val insert = createInsertPlan(Seq(1).toDF("a"))
    // Disabling type widening after analysis results in inserting data with a wider type into the
    // table while type widening is actually disabled during execution. We do actually widen the
    // table schema in that case because `short` and `int` are both stored as INT32 in parquet.
    enableTypeWidening(tempPath, enabled = false)
    Dataset.ofRows(spark, insert).collect()
    assert(readDeltaTable(tempPath).schema == new StructType().add("a", IntegerType))
    checkAnswer(readDeltaTable(tempPath), Row(1))
  }


  /**
   * There are **many** different ways to run an insert:
   * - Using SQL or the dataframe v1 and v2 APIs.
   * - Append vs. Overwrite / Partition overwrite.
   * - Position-based vs. name-based resolution.
   *
   * Each take a unique path through analysis. The abstractions below captures these different
   * inserts to allow more easily running tests with all or a subset of them.
   *
   * @param mode Append or Overwrite. This dictates in particular what the expected result after the
   *             insert should be.
   * @param name A human-readable name for the insert type displayed in the test names.
   */
  trait Insert {
    val mode: SaveMode
    val name: String

    /**
     * The method that tests will call to run the insert. Each type of insert must implement its
     * sepcific way to run insert.
     */
    def runInsert(columns: Seq[String], whereCol: String, whereValue: Int): Unit

    /** SQL keyword for this type of insert.  */
    def intoOrOverwrite: String = if (mode == SaveMode.Append) "INTO" else "OVERWRITE"

    /** The expected content of the table after the insert. */
    def expectedResult(initialDF: DataFrame, insertedDF: DataFrame): DataFrame =
      if (mode == SaveMode.Overwrite) insertedDF
      else initialDF.unionByName(insertedDF, allowMissingColumns = true)
  }

  /** INSERT INTO/OVERWRITE */
  case class SQLInsertByPosition(mode: SaveMode) extends Insert {
    val name: String = s"INSERT $intoOrOverwrite"
    def runInsert(columns: Seq[String], whereCol: String, whereValue: Int): Unit =
      sql(s"INSERT $intoOrOverwrite target SELECT * FROM source")
  }

  /** INSERT INTO/OVERWRITE (a, b) */
  case class SQLInsertColList(mode: SaveMode) extends Insert {
    val name: String = s"INSERT $intoOrOverwrite (columns) - $mode"
    def runInsert(columns: Seq[String], whereCol: String, whereValue: Int): Unit = {
      val colList = columns.mkString(", ")
      sql(s"INSERT $intoOrOverwrite target ($colList) SELECT $colList FROM source")
    }
  }

  /** INSERT INTO/OVERWRITE BY NAME */
  case class SQLInsertByName(mode: SaveMode) extends Insert {
    val name: String = s"INSERT $intoOrOverwrite BY NAME - $mode"
    def runInsert(columns: Seq[String], whereCol: String, whereValue: Int): Unit =
      sql(s"INSERT $intoOrOverwrite target SELECT ${columns.mkString(", ")} FROM source")
  }

  /** INSERT INTO REPLACE WHERE */
  object SQLInsertOverwriteReplaceWhere extends Insert {
    val mode: SaveMode = SaveMode.Overwrite
    val name: String = s"INSERT INTO REPLACE WHERE"
    def runInsert(columns: Seq[String], whereCol: String, whereValue: Int): Unit =
      sql(s"INSERT INTO target REPLACE WHERE $whereCol = $whereValue " +
          s"SELECT ${columns.mkString(", ")} FROM source")
  }

  /** INSERT OVERWRITE PARTITION (part = 1) */
  object SQLInsertOverwritePartitionByPosition extends Insert {
    val mode: SaveMode = SaveMode.Overwrite
    val name: String = s"INSERT OVERWRITE PARTITION (partition)"
    def runInsert(columns: Seq[String], whereCol: String, whereValue: Int): Unit = {
      val assignments = columns.filterNot(_ == whereCol).mkString(", ")
      sql(s"INSERT OVERWRITE target PARTITION ($whereCol = $whereValue) " +
          s"SELECT $assignments FROM source")
    }
  }

  /** INSERT OVERWRITE PARTITION (part = 1) (a, b) */
  object SQLInsertOverwritePartitionColList extends Insert {
    val mode: SaveMode = SaveMode.Overwrite
    val name: String = s"INSERT OVERWRITE PARTITION (partition) (columns)"
    def runInsert(columns: Seq[String], whereCol: String, whereValue: Int): Unit = {
      val assignments = columns.filterNot(_ == whereCol).mkString(", ")
      sql(s"INSERT OVERWRITE target PARTITION ($whereCol = $whereValue) ($assignments) " +
          s"SELECT $assignments FROM source")
    }
  }

  /** df.write.mode(mode).insertInto() */
  case class DFv1InsertInto(mode: SaveMode) extends Insert {
    val name: String = s"DFv1 insertInto() - $mode"
    def runInsert(columns: Seq[String], whereCol: String, whereValue: Int): Unit =
      spark.read.table("source").write.mode(mode).insertInto("target")
  }

  /** df.write.mode(mode).saveAsTable() */
  case class DFv1SaveAsTable(mode: SaveMode) extends Insert {
    val name: String = s"DFv1 saveAsTable() - $mode"
    def runInsert(columns: Seq[String], whereCol: String, whereValue: Int): Unit = {
      spark.read.table("source").write.mode(mode).format("delta").saveAsTable("target")
    }
  }

  /** df.writeTo.append() */
  object DFv2Append extends Insert { self: Insert =>
    val mode: SaveMode = SaveMode.Append
    val name: String = "DFv2 append()"
    def runInsert(columns: Seq[String], whereCol: String, whereValue: Int): Unit = {
      spark.read.table("source").writeTo("target").append()
    }
  }

  /** df.writeTo.overwrite() */
  object DFv2Overwrite extends Insert { self: Insert =>
    val mode: SaveMode = SaveMode.Overwrite
    val name: String = s"DFv2 overwrite()"
    def runInsert(columns: Seq[String], whereCol: String, whereValue: Int): Unit = {
      spark.read.table("source").writeTo("target").overwrite(col(whereCol) === lit(whereValue))
    }
  }

  /** df.writeTo.overwritePartitions() */
  object DFv2OverwritePartition extends Insert { self: Insert =>
    override val mode: SaveMode = SaveMode.Overwrite
    val name: String = s"DFv2 overwritePartitions()"
    def runInsert(columns: Seq[String], whereCol: String, whereValue: Int): Unit = {
      spark.read.table("source").writeTo("target").overwritePartitions()
    }
  }

  /** Collects all the types of insert previously defined. */
  protected lazy val allInsertTypes: Seq[Insert] = Seq(
        SQLInsertOverwriteReplaceWhere,
        SQLInsertOverwritePartitionByPosition,
        SQLInsertOverwritePartitionColList,
        DFv2Append,
        DFv2Overwrite,
        DFv2OverwritePartition
  ) ++ (for {
      mode: SaveMode <- Seq(SaveMode.Append, SaveMode.Overwrite)
      insert: Insert <- Seq(
        SQLInsertByPosition(mode),
        SQLInsertColList(mode),
        SQLInsertByName(mode),
        DFv1InsertInto(mode),
        DFv1SaveAsTable(mode)
      )
    } yield insert)

  /**
   * Test runner for type evolution in INSERT.
   * @param name             Test name
   * @param initialSchemaDDL Initial schema of the table to be inserted into (as a DDL string).
   * @param initialJsonData  Initial data present in the table to be inserted into (as a JSON
   *                         string).
   * @param partitionBy      Partition columns for the initial table.
   * @param insertSchemaDDL  Schema of the data to be inserted (as a DDL string).
   * @param insertJsonData   Data to be inserted (as a JSON string)
   * @param overwriteWhere   Where clause for overwrite PARTITION / REPLACE WHERE (as
   *                         colName -> value)
   * @param expectedSchema   Expected schema of the table after the insert.
   * @param includeInserts   List of insert types to run the test with. Defaults to all inserts.
   * @param excludeInserts   List of insert types to exclude when running the test. Defaults to no
   *                         inserts excluded.
   */
  def testInsertTypeEvolution(name: String)(
      initialSchemaDDL: String,
      initialJsonData: Seq[String],
      partitionBy: Seq[String] = Seq.empty,
      insertSchemaDDL: String,
      insertJsonData: Seq[String],
      overwriteWhere: (String, Int),
      expectedSchema: StructType,
      includeInserts: Seq[Insert] = allInsertTypes,
      excludeInserts: Seq[Insert] = Seq.empty): Unit = {
    for (insert <- includeInserts.filterNot(excludeInserts.toSet)) {
      test(s"${insert.name} - $name") {
        withTable("source", "target") {
          val initialDF = readFromJSON(initialJsonData, StructType.fromDDL(initialSchemaDDL))
          val writer = initialDF.write.format("delta")
          if (partitionBy.nonEmpty) {
            writer.partitionBy(partitionBy: _*)
          }
          writer.saveAsTable("target")
          // Write the data to insert to a table so that we can use it in both SQL and dataframe
          // writer inserts.
          val insertDF = readFromJSON(insertJsonData, StructType.fromDDL(insertSchemaDDL))
          insertDF.write.format("delta").saveAsTable("source")

          insert.runInsert(
            columns = insertDF.schema.map(_.name),
            whereCol = overwriteWhere._1,
            whereValue = overwriteWhere._2
          )

          val target = spark.read.table("target")
          assert(target.schema === expectedSchema)
          checkAnswer(target, insert.expectedResult(initialDF, insertDF))
        }
      }
    }
  }

  testInsertTypeEvolution("top-level type evolution")(
    initialSchemaDDL = "a int, b short",
    initialJsonData = Seq("""{ "a": 1, "b": 2 }"""),
    partitionBy = Seq("a"),
    overwriteWhere = "a" -> 1,
    insertSchemaDDL = "a int, b int",
    insertJsonData = Seq("""{ "a": 1, "b": 4 }"""),
    expectedSchema = StructType(new StructType()
      .add("a", IntegerType)
      .add("b", IntegerType, nullable = true,
        metadata = typeWideningMetadata(version = 1, from = ShortType, to = IntegerType)))
  )

  testInsertTypeEvolution("top-level type evolution with column upcast")(
    initialSchemaDDL = "a int, b short, c int",
    initialJsonData = Seq("""{ "a": 1, "b": 2, "c": 3 }"""),
    partitionBy = Seq("a"),
    overwriteWhere = "a" -> 1,
    insertSchemaDDL = "a int, b int, c short",
    insertJsonData = Seq("""{ "a": 1, "b": 5, "c": 6 }"""),
    expectedSchema = new StructType()
      .add("a", IntegerType)
      .add("b", IntegerType, nullable = true,
        metadata = typeWideningMetadata(version = 1, from = ShortType, to = IntegerType))
      .add("c", IntegerType)
  )

  testInsertTypeEvolution("top-level type evolution with schema evolution")(
    initialSchemaDDL = "a int, b short",
    initialJsonData = Seq("""{ "a": 1, "b": 2 }"""),
    partitionBy = Seq("a"),
    overwriteWhere = "a" -> 1,
    insertSchemaDDL = "a int, b int, c int",
    insertJsonData = Seq("""{ "a": 1, "b": 4, "c": 5 }"""),
    expectedSchema = new StructType()
      .add("a", IntegerType)
      .add("b", IntegerType, nullable = true,
        metadata = typeWideningMetadata(version = 1, from = ShortType, to = IntegerType))
      .add("c", IntegerType),
    // INSERT INTO/OVERWRITE (a, b) VALUES doesn't support schema evolution.
    excludeInserts = Seq(
      SQLInsertColList(SaveMode.Append),
      SQLInsertColList(SaveMode.Overwrite),
      SQLInsertOverwritePartitionColList)
  )


  testInsertTypeEvolution("nested type evolution by position")(
    initialSchemaDDL =
      "key int, s struct<x: short, y: short>, m map<string, short>, a array<short>",
    initialJsonData = Seq("""{ "key": 1, "s": { "x": 1, "y": 2 }, "m": { "p": 3 }, "a": [4] }"""),
    partitionBy = Seq("key"),
    overwriteWhere = "key" -> 1,
    insertSchemaDDL = "key int, s struct<x: short, y: int>, m map<string, int>, a array<int>",
    insertJsonData = Seq("""{ "key": 1, "s": { "x": 4, "y": 5 }, "m": { "p": 6 }, "a": [7] }"""),
    expectedSchema = new StructType()
      .add("key", IntegerType)
      .add("s", new StructType()
        .add("x", ShortType)
        .add("y", IntegerType, nullable = true,
          metadata = typeWideningMetadata(version = 1, from = ShortType, to = IntegerType)))
      .add("m", MapType(StringType, IntegerType), nullable = true,
        metadata = typeWideningMetadata(
          version = 1,
          from = ShortType,
          to = IntegerType,
          path = Seq("value")))
      .add("a", ArrayType(IntegerType), nullable = true,
        metadata = typeWideningMetadata(
          version = 1,
          from = ShortType,
          to = IntegerType,
          path = Seq("element")))
  )


  testInsertTypeEvolution("nested type evolution with struct evolution by position")(
    initialSchemaDDL =
      "key int, s struct<x: short, y: short>, m map<string, short>, a array<short>",
    initialJsonData = Seq("""{ "key": 1, "s": { "x": 1, "y": 2 }, "m": { "p": 3 }, "a": [4] }"""),
    partitionBy = Seq("key"),
    overwriteWhere = "key" -> 1,
    insertSchemaDDL =
      "key int, s struct<x: short, y: int, z: int>, m map<string, int>, a array<int>",
    insertJsonData =
      Seq("""{ "key": 1, "s": { "x": 4, "y": 5, "z": 8 }, "m": { "p": 6 }, "a": [7] }"""),
    expectedSchema = new StructType()
      .add("key", IntegerType)
      .add("s", new StructType()
        .add("x", ShortType)
        .add("y", IntegerType, nullable = true,
          metadata = typeWideningMetadata(version = 1, from = ShortType, to = IntegerType))
        .add("z", IntegerType))
      .add("m", MapType(StringType, IntegerType), nullable = true,
        metadata = typeWideningMetadata(
          version = 1,
          from = ShortType,
          to = IntegerType,
          path = Seq("value")))
      .add("a", ArrayType(IntegerType), nullable = true,
        metadata = typeWideningMetadata(
          version = 1,
          from = ShortType,
          to = IntegerType,
          path = Seq("element")))
  )


  testInsertTypeEvolution("nested struct type evolution with field upcast")(
    initialSchemaDDL = "key int, s struct<x: int, y: short>",
    initialJsonData = Seq("""{ "key": 1, "s": { "x": 1, "y": 2 } }"""),
    partitionBy = Seq("key"),
    overwriteWhere = "key" -> 1,
    insertSchemaDDL = "key int, s struct<x: short, y: int>",
    insertJsonData = Seq("""{ "key": 1, "s": { "x": 4, "y": 5 } }"""),
    expectedSchema = new StructType()
      .add("key", IntegerType)
      .add("s", new StructType()
        .add("x", IntegerType)
        .add("y", IntegerType, nullable = true,
          metadata = typeWideningMetadata(version = 1, from = ShortType, to = IntegerType)))
  )

  // Interestingly, we introduced a special case to handle schema evolution / casting for structs
  // directly nested into an array. This doesn't always work with maps or with elements that
  // aren't a struct (see other tests).
  testInsertTypeEvolution("nested struct type evolution with field upcast in array")(
    initialSchemaDDL = "key int, a array<struct<x: int, y: short>>",
    initialJsonData = Seq("""{ "key": 1, "a": [ { "x": 1, "y": 2 } ] }"""),
    partitionBy = Seq("key"),
    overwriteWhere = "key" -> 1,
    insertSchemaDDL = "key int, a array<struct<x: short, y: int>>",
    insertJsonData = Seq("""{ "key": 1, "a": [ { "x": 3, "y": 4 } ] }"""),
    expectedSchema = new StructType()
      .add("key", IntegerType)
      .add("a", ArrayType(new StructType()
        .add("x", IntegerType)
        .add("y", IntegerType, nullable = true,
          metadata = typeWideningMetadata(version = 1, from = ShortType, to = IntegerType))))
  )

  // The next two tests document inconsistencies when handling maps. Using SQL doesn't allow type
  // evolution but using the dataframe API does.
  testInsertTypeEvolution("nested struct type evolution with field upcast in map")(
    initialSchemaDDL = "key int, m map<string, struct<x: int, y: short>>",
    initialJsonData = Seq("""{ "key": 1, "m": { "a": { "x": 1, "y": 2 } } }"""),
    partitionBy = Seq("key"),
    overwriteWhere = "key" -> 1,
    insertSchemaDDL = "key int, m map<string, struct<x: short, y: int>>",
    insertJsonData = Seq("""{ "key": 1, "m": { "a": { "x": 3, "y": 4 } } }"""),
    expectedSchema = new StructType()
      .add("key", IntegerType)
      // Type evolution wasn't applied in the map.
      .add("m", MapType(StringType, new StructType()
        .add("x", IntegerType)
        .add("y", ShortType))),
    excludeInserts = Seq(
      DFv1SaveAsTable(SaveMode.Append),
      DFv1SaveAsTable(SaveMode.Overwrite),
      DFv2Append,
      DFv2Overwrite,
      DFv2OverwritePartition
    )
  )

  testInsertTypeEvolution("nested struct type evolution with field upcast in map")(
    initialSchemaDDL = "key int, m map<string, struct<x: int, y: short>>",
    initialJsonData = Seq("""{ "key": 1, "m": { "a": { "x": 1, "y": 2 } } }"""),
    partitionBy = Seq("key"),
    overwriteWhere = "key" -> 1,
    insertSchemaDDL = "key int, m map<string, struct<x: short, y: int>>",
    insertJsonData = Seq("""{ "key": 1, "m": { "a": { "x": 3, "y": 4 } } }"""),
    expectedSchema = StructType(new StructType()
      .add("key", IntegerType)
      // Type evolution was applied in the map.
      .add("m", MapType(StringType, new StructType()
        .add("x", IntegerType)
        .add("y", IntegerType, nullable = true,
          metadata = typeWideningMetadata(version = 1, from = ShortType, to = IntegerType))))),
    includeInserts = Seq(
      DFv1SaveAsTable(SaveMode.Append),
      DFv1SaveAsTable(SaveMode.Overwrite),
      DFv2Append,
      DFv2Overwrite,
      DFv2OverwritePartition
    )
  )
}
