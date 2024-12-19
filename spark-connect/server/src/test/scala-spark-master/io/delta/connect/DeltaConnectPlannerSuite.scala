/*
 * Copyright (2024) The Delta Lake Project Authors.
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

package org.apache.spark.sql.connect.delta

import java.io.File
import java.text.SimpleDateFormat
import java.util.UUID

import org.apache.spark.sql.delta.commands.optimize.OptimizeMetrics
import com.google.protobuf
import io.delta.connect.proto
import io.delta.connect.spark.{proto => spark_proto}
import io.delta.tables.DeltaTable

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Dataset, QueryTest, Row, SparkSession}
import org.apache.spark.sql.catalyst.analysis.ResolvedTable
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation
import org.apache.spark.sql.connect.config.Connect
import org.apache.spark.sql.connect.delta.ImplicitProtoConversions._
import org.apache.spark.sql.connect.planner.{SparkConnectPlanner, SparkConnectPlanTest}
import org.apache.spark.sql.connect.service.{SessionHolder, SparkConnectService}
import org.apache.spark.sql.delta.{DeltaHistory, DeltaLog}
import org.apache.spark.sql.delta.commands.{DescribeDeltaDetailCommand, DescribeDeltaHistory}
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.delta.test.DeltaTestImplicits._
import org.apache.spark.sql.delta.util.FileNames
import org.apache.spark.sql.functions._

class DeltaConnectPlannerSuite
  extends QueryTest
  with DeltaSQLCommandTest
  with SparkConnectPlanTest {

  protected override def sparkConf: SparkConf = {
    super.sparkConf
      .set(Connect.CONNECT_EXTENSIONS_RELATION_CLASSES.key, classOf[DeltaRelationPlugin].getName)
  }

  private def createSparkRelation(relation: proto.DeltaRelation.Builder): spark_proto.Relation = {
    spark_proto.Relation.newBuilder().setExtension(protobuf.Any.pack(relation.build())).build()
  }

  def createDummySessionHolder(session: SparkSession): SessionHolder = {
    val ret = SessionHolder(
      userId = "testUser",
      sessionId = UUID.randomUUID().toString,
      session = session
    )
    SparkConnectService.sessionManager.putSessionForTesting(ret)
    ret
  }

  test("scan table by name") {
    withTable("table") {
      DeltaTable.create(spark).tableName(identifier = "table").execute()

      val input = createSparkRelation(
        proto.DeltaRelation
          .newBuilder()
          .setScan(
            proto.Scan
              .newBuilder()
              .setTable(proto.DeltaTable.newBuilder().setTableOrViewName("table"))
          )
      )

      val result = transform(input)
      val expected = DeltaTable.forName(spark, "table").toDF.queryExecution.analyzed
      comparePlans(result, expected)
    }
  }

  test("scan table by path") {
    withTempDir { dir =>
      DeltaTable.create(spark).location(dir.getAbsolutePath).execute()

      val input = createSparkRelation(
        proto.DeltaRelation
          .newBuilder()
          .setScan(
            proto.Scan
              .newBuilder()
              .setTable(
                proto.DeltaTable
                  .newBuilder()
                  .setPath(
                    proto.DeltaTable.Path.newBuilder().setPath(dir.getAbsolutePath)
                  )
              )
          )
      )

      val result = transform(input)
      val expected = DeltaTable.forPath(spark, dir.getAbsolutePath).toDF.queryExecution.analyzed
      comparePlans(result, expected)
    }
  }

  test("convert to delta") {
    withTempDir { dir =>
      spark.range(100).write.format("parquet").mode("overwrite").save(dir.getAbsolutePath)

      val input = createSparkRelation(
        proto.DeltaRelation
          .newBuilder()
          .setConvertToDelta(
            proto.ConvertToDelta
              .newBuilder()
              .setIdentifier(s"parquet.`${dir.getAbsolutePath}`")
          )
      )
      val plan = transform(input)
      val result = Dataset.ofRows(spark, plan).collect()

      assert(result.length === 1)
      val deltaTable = DeltaTable.forName(spark, result.head.getString(0))
      assert(!deltaTable.toDF.isEmpty)
    }
  }

  test("convert to delta with partitioning schema string") {
    withTempDir { dir =>
      spark.range(100)
        .select(col("id") % 10 as "part", col("id") as "value")
        .write
        .partitionBy("part")
        .format("parquet")
        .mode("overwrite")
        .save(dir.getAbsolutePath)

      val input = createSparkRelation(
        proto.DeltaRelation
          .newBuilder()
          .setConvertToDelta(
            proto.ConvertToDelta
              .newBuilder()
              .setIdentifier(s"parquet.`${dir.getAbsolutePath}`")
              .setPartitionSchemaString("part LONG")
          )
      )
      val plan = transform(input)
      val result = Dataset.ofRows(spark, plan).collect()

      assert(result.length === 1)
      val deltaTable = DeltaTable.forName(spark, result.head.getString(0))
      assert(!deltaTable.toDF.isEmpty)
    }
  }

  test("convert to delta with partitioning schema struct") {
    withTempDir { dir =>
      spark.range(100)
        .select(col("id") % 10 as "part", col("id") as "value")
        .write
        .partitionBy("part")
        .format("parquet")
        .mode("overwrite")
        .save(dir.getAbsolutePath)

      val input = createSparkRelation(
        proto.DeltaRelation
          .newBuilder()
          .setConvertToDelta(
            proto.ConvertToDelta
              .newBuilder()
              .setIdentifier(s"parquet.`${dir.getAbsolutePath}`")
              .setPartitionSchemaStruct(
                spark_proto.DataType
                  .newBuilder()
                  .setStruct(
                    spark_proto.DataType.Struct
                      .newBuilder()
                      .addFields(
                        spark_proto.DataType.StructField
                          .newBuilder()
                          .setName("part")
                          .setNullable(false)
                          .setDataType(
                            spark_proto.DataType
                              .newBuilder()
                              .setLong(spark_proto.DataType.Long.newBuilder())
                          )
                      )
                  )
              )
          )
      )
      val plan = transform(input)
      val result = Dataset.ofRows(spark, plan).collect()

      assert(result.length === 1)
      val deltaTable = DeltaTable.forName(spark, result.head.getString(0))
      assert(!deltaTable.toDF.isEmpty)
    }
  }

  test("history") {
    withTable("table") {
      DeltaTable.create(spark).tableName(identifier = "table").execute()

      val input = createSparkRelation(
        proto.DeltaRelation
          .newBuilder()
          .setDescribeHistory(
            proto.DescribeHistory.newBuilder()
              .setTable(proto.DeltaTable.newBuilder().setTableOrViewName("table"))
          )
      )

      val result = transform(input)
      result match {
        case lr: LocalRelation if lr.schema == ExpressionEncoder[DeltaHistory]().schema =>
        case other => fail(s"Unexpected plan: $other")
      }
    }
  }

  test("detail") {
    withTable("table") {
      DeltaTable.create(spark).tableName(identifier = "table").execute()

      val input = createSparkRelation(
        proto.DeltaRelation
          .newBuilder()
          .setDescribeDetail(
            proto.DescribeDetail.newBuilder()
              .setTable(proto.DeltaTable.newBuilder().setTableOrViewName("table"))
          )
      )

      val result = transform(input)
      val expected = DeltaTable.forName(spark, "table").detail().queryExecution.analyzed

      assert(result.isInstanceOf[DescribeDeltaDetailCommand])
      val childResult = result.asInstanceOf[DescribeDeltaDetailCommand].child
      val childExpected = expected.asInstanceOf[DescribeDeltaDetailCommand].child

      assert(childResult.asInstanceOf[ResolvedTable].identifier.name ===
        childExpected.asInstanceOf[ResolvedTable].identifier.name)
    }
  }

  private val expectedRestoreOutputColumns = Seq(
    "table_size_after_restore",
    "num_of_files_after_restore",
    "num_removed_files",
    "num_restored_files",
    "removed_files_size",
    "restored_files_size"
  )

  test("restore to version number") {
    withTable("table") {
      spark.range(start = 0, end = 1000, step = 1, numPartitions = 1)
        .write.format("delta").saveAsTable("table")
      spark.range(start = 0, end = 2000, step = 1, numPartitions = 2)
        .write.format("delta").mode("append").saveAsTable("table")

      val input = createSparkRelation(
        proto.DeltaRelation
          .newBuilder()
          .setRestoreTable(
            proto.RestoreTable.newBuilder()
              .setTable(proto.DeltaTable.newBuilder().setTableOrViewName("table"))
              .setVersion(0L)
          )
      )

      val plan = transform(input)
      assert(plan.columns.toSeq == expectedRestoreOutputColumns)
      val result = Dataset.ofRows(spark, plan).collect()
      assert(result.length === 1)
      assert(result.head.getLong(2) === 2) // Two files should have been removed.
      assert(spark.read.table("table").count() === 1000)
    }
  }

  test("restore to timestamp") {
    withTempDir { dir =>
      spark.range(start = 0, end = 1000, step = 1, numPartitions = 1)
        .write.format("delta").save(dir.getAbsolutePath)
      spark.range(start = 0, end = 2000, step = 1, numPartitions = 2)
        .write.format("delta").mode("append").save(dir.getAbsolutePath)

      val log = DeltaLog.forTable(spark, dir)
      val input = createSparkRelation(
        proto.DeltaRelation
          .newBuilder()
          .setRestoreTable(
            proto.RestoreTable.newBuilder()
              .setTable(
                proto.DeltaTable.newBuilder().setPath(
                  proto.DeltaTable.Path.newBuilder().setPath(dir.getAbsolutePath)
                )
              )
              .setTimestamp(getTimestampForVersion(log, version = 0))
          )
      )

      val plan = transform(input)
      assert(plan.columns.toSeq === expectedRestoreOutputColumns)
      val result = Dataset.ofRows(spark, plan).collect()
      assert(result.length === 1)
      assert(result.head.getLong(2) === 2) // Two files should have been removed.
      assert(spark.read.format("delta").load(dir.getAbsolutePath).count() === 1000)
    }
  }

  test("isDeltaTable - delta") {
    withTempDir { dir =>
      val path = dir.getAbsolutePath
      spark.range(end = 1000).write.format("delta").mode("overwrite").save(path)

      val input = createSparkRelation(
        proto.DeltaRelation.newBuilder()
          .setIsDeltaTable(proto.IsDeltaTable.newBuilder().setPath(path))
      )

      val plan = transform(input)
      assert(plan.schema.length === 1)
      val result = Dataset.ofRows(spark, plan).collect()
      assert(result.length === 1)
      assert(result.head.getBoolean(0))
    }
  }

  test("isDeltaTable - parquet") {
    withTempDir { dir =>
      val path = dir.getAbsolutePath
      spark.range(end = 1000).write.format("parquet").mode("overwrite").save(path)

      val input = createSparkRelation(
        proto.DeltaRelation.newBuilder()
          .setIsDeltaTable(proto.IsDeltaTable.newBuilder().setPath(path))
      )

      val plan = transform(input)
      assert(plan.schema.length === 1)
      val result = Dataset.ofRows(spark, plan).collect()
      assert(result.length === 1)
      assert(!result.head.getBoolean(0))
    }
  }

  test("delete without condition") {
    val tableName = "table"
    withTable(tableName) {
      spark.range(end = 1000).write.format("delta").saveAsTable(tableName)

      val input = createSparkRelation(
        proto.DeltaRelation.newBuilder()
          .setDeleteFromTable(
            proto.DeleteFromTable.newBuilder()
              .setTarget(createScan(tableName))
          )
      )

      val plan = transform(input)
      val result = Dataset.ofRows(spark, plan).collect()
      assert(result.length === 1)
      assert(result.head.getLong(0) === 1000)
      assert(spark.read.table(tableName).isEmpty)
    }
  }

  test("delete with condition") {
    val tableName = "table"
    withTable(tableName) {
      spark.range(end = 1000).write.format("delta").saveAsTable(tableName)

      val input = createSparkRelation(
        proto.DeltaRelation.newBuilder()
          .setDeleteFromTable(
            proto.DeleteFromTable.newBuilder()
              .setTarget(createScan(tableName))
              .setCondition(createExpression("id % 2 = 0"))
          )
      )

      val plan = transform(input)
      val result = Dataset.ofRows(spark, plan).collect()
      assert(result.length === 1)
      assert(result.head.getLong(0) === 500)
      assert(spark.read.table(tableName).count() === 500)
    }
  }

  test("update without condition") {
    val tableName = "target"
    withTable(tableName) {
      spark.range(end = 1000).select(col("id") as "key", col("id") as "value")
        .write.format("delta").saveAsTable(tableName)

      val input = createSparkRelation(
        proto.DeltaRelation.newBuilder()
          .setUpdateTable(
            proto.UpdateTable.newBuilder()
              .setTarget(createScan(tableName))
              .addAssignments(createAssignment(field = "value", value = "value + 1"))
          )
      )

      val plan = transform(input)
      val result = Dataset.ofRows(spark, plan).collect()
      assert(result.length === 1)
      assert(result.head.getLong(0) === 1000)
      checkAnswer(
        spark.read.table(tableName),
        Seq.tabulate(1000)(i => Row(i, i + 1))
      )
    }
  }

  test("update with condition") {
    val tableName = "target"
    withTable(tableName) {
      spark.range(end = 1000).select(col("id") as "key", col("id") as "value")
        .write.format("delta").saveAsTable(tableName)

      val input = createSparkRelation(
        proto.DeltaRelation.newBuilder()
          .setUpdateTable(
            proto.UpdateTable.newBuilder()
              .setTarget(createScan(tableName))
              .setCondition(createExpression("key % 2 = 0"))
              .addAssignments(createAssignment(field = "value", value = "value + 1"))
          )
      )

      val plan = transform(input)
      val result = Dataset.ofRows(spark, plan).collect()
      assert(result.length === 1)
      assert(result.head.getLong(0) === 500)
      checkAnswer(
        spark.read.table(tableName),
        Seq.tabulate(1000)(i => Row(i, if (i % 2 == 0) i + 1 else i))
      )
    }
  }

  test("merge - insert only") {
    val targetTableName = "target"
    val sourceTableName = "source"
    withTable(targetTableName, sourceTableName) {
      spark.range(end = 100).select(col("id") as "key", col("id") as "value")
        .write.format("delta").saveAsTable(targetTableName)

      spark.range(end = 100)
        .select(col("id") + 50 as "id")
        .select(col("id") as "key", col("id") + 1000 as "value")
        .write.format("delta").saveAsTable(sourceTableName)

      val input = createSparkRelation(
        proto.DeltaRelation.newBuilder()
          .setMergeIntoTable(
            proto.MergeIntoTable.newBuilder()
              .setTarget(createSubqueryAlias(createScan(targetTableName), alias = "t"))
              .setSource(createSubqueryAlias(createScan(sourceTableName), alias = "s"))
              .setCondition(createExpression("t.key = s.key"))
              .addNotMatchedActions(
                proto.MergeIntoTable.Action.newBuilder()
                  .setInsertAction(
                    proto.MergeIntoTable.Action.InsertAction.newBuilder()
                      .addAssignments(createAssignment(field = "t.key", value = "s.key"))
                      .addAssignments(createAssignment(field = "t.value", value = "s.value"))
                  )
              )
          )
      )

      val plan = transform(input)
      val result = Dataset.ofRows(spark, plan).collect()
      assert(result.length === 1)
      assert(result.head.getLong(0) === 50) // num_affected_rows
      assert(result.head.getLong(1) === 0) // num_updated_rows
      assert(result.head.getLong(2) === 0) // num_deleted_rows
      assert(result.head.getLong(3) === 50) // num_inserted_rows

      checkAnswer(
        spark.read.table(targetTableName),
        Seq.tabulate(100)(i => Row(i, i)) ++ Seq.tabulate(50)(i => Row(i + 100, i + 1100))
      )
    }
  }

  test("merge - update only") {
    val targetTableName = "target"
    val sourceTableName = "source"
    withTable(targetTableName, sourceTableName) {
      spark.range(end = 100).select(col("id") as "key", col("id") as "value")
        .write.format("delta").saveAsTable(targetTableName)

      spark.range(end = 100)
        .select(col("id") + 50 as "id")
        .select(col("id") as "key", col("id") + 1000 as "value")
        .write.format("delta").saveAsTable(sourceTableName)

      val input = createSparkRelation(
        proto.DeltaRelation.newBuilder()
          .setMergeIntoTable(
            proto.MergeIntoTable.newBuilder()
              .setTarget(createSubqueryAlias(createScan(targetTableName), alias = "t"))
              .setSource(createSubqueryAlias(createScan(sourceTableName), alias = "s"))
              .setCondition(createExpression("t.key = s.key"))
              .addMatchedActions(
                proto.MergeIntoTable.Action.newBuilder()
                  .setUpdateAction(
                    proto.MergeIntoTable.Action.UpdateAction.newBuilder()
                      .addAssignments(createAssignment(field = "t.key", value = "s.key"))
                      .addAssignments(createAssignment(field = "t.value", value = "s.value"))
                  )
              )
          )
      )

      val plan = transform(input)
      val result = Dataset.ofRows(spark, plan).collect()
      assert(result.length === 1)
      assert(result.head.getLong(0) === 50) // num_affected_rows
      assert(result.head.getLong(1) === 50) // num_updated_rows
      assert(result.head.getLong(2) === 0) // num_deleted_rows
      assert(result.head.getLong(3) === 0) // num_inserted_rows

      checkAnswer(
        spark.read.table(targetTableName),
        Seq.tabulate(50)(i => Row(i, i)) ++ Seq.tabulate(50)(i => Row(i + 50, i + 1050))
      )
    }
  }

  test("merge - mixed") {
    val targetTableName = "target"
    val sourceTableName = "source"
    withTable(targetTableName, sourceTableName) {
      spark.range(end = 100).select(col("id") as "key", col("id") as "value")
        .write.format("delta").saveAsTable(targetTableName)

      spark.range(end = 100)
        .select(col("id") + 50 as "id")
        .select(col("id") as "key", col("id") + 1000 as "value")
        .write.format("delta").saveAsTable(sourceTableName)

      val input = createSparkRelation(
        proto.DeltaRelation.newBuilder()
          .setMergeIntoTable(
            proto.MergeIntoTable.newBuilder()
              .setTarget(createSubqueryAlias(createScan(targetTableName), alias = "t"))
              .setSource(createSubqueryAlias(createScan(sourceTableName), alias = "s"))
              .setCondition(createExpression("t.key = s.key"))
              .addMatchedActions(
                proto.MergeIntoTable.Action.newBuilder()
                  .setUpdateStarAction(proto.MergeIntoTable.Action.UpdateStarAction.newBuilder())
              )
              .addNotMatchedActions(
                proto.MergeIntoTable.Action.newBuilder()
                  .setInsertStarAction(proto.MergeIntoTable.Action.InsertStarAction.newBuilder())
              )
              .addNotMatchedBySourceActions(
                proto.MergeIntoTable.Action.newBuilder()
                  .setCondition(createExpression("t.value < 25"))
                  .setDeleteAction(proto.MergeIntoTable.Action.DeleteAction.newBuilder())
              )
          )
      )

      val plan = transform(input)
      val result = Dataset.ofRows(spark, plan).collect()
      assert(result.length === 1)
      assert(result.head.getLong(0) === 125) // num_affected_rows
      assert(result.head.getLong(1) === 50) // num_updated_rows
      assert(result.head.getLong(2) === 25) // num_deleted_rows
      assert(result.head.getLong(3) === 50) // num_inserted_rows

      checkAnswer(
        spark.read.table(targetTableName),
        Seq.tabulate(25)(i => Row(25 + i, 25 + i)) ++
          Seq.tabulate(50)(i => Row(i + 50, i + 1050)) ++
          Seq.tabulate(50)(i => Row(i + 100, i + 1100))
      )
    }
  }

  test("optimize - compaction") {
    val tableName = "test_table"
    withTable(tableName) {
      spark.range(1000).select(col("id") % 3 as "key", col("id") as "val")
        .write.format("delta").saveAsTable(tableName)

      val plan = transform(createSparkRelation(
        proto.DeltaRelation.newBuilder()
          .setOptimizeTable(
            proto.OptimizeTable.newBuilder()
              .setTable(proto.DeltaTable.newBuilder().setTableOrViewName(tableName))
          )
      ))
      assert(plan.columns.toSeq === Seq("path", "metrics"))
      val df = Dataset.ofRows(spark, plan)

      checkOptimizeMetrics(df)
      checkOptimizeUsingHistory(tableName, expectedPredicates = Nil, expectedZorderCols = Nil)
    }
  }

  test("optimize - compaction with partition filters") {
    val tableName = "test_table"
    withTable(tableName) {
      spark.range(1000).select(col("id") % 3 as "key", col("id") as "val")
        .write.partitionBy("key").format("delta").saveAsTable(tableName)

      val plan = transform(createSparkRelation(
        proto.DeltaRelation.newBuilder()
          .setOptimizeTable(
            proto.OptimizeTable.newBuilder()
              .setTable(proto.DeltaTable.newBuilder().setTableOrViewName(tableName))
              .addPartitionFilters("key = 1")
          )
      ))
      assert(plan.columns.toSeq === Seq("path", "metrics"))
      val df = Dataset.ofRows(spark, plan)

      checkOptimizeMetrics(df)
      checkOptimizeUsingHistory(
        tableName, expectedPredicates = Seq("'key = 1"), expectedZorderCols = Nil)
    }
  }

  test("optimize - z-order") {
    val tableName = "test_table"
    withTable(tableName) {
      spark.range(1000).select(col("id") % 3 as "key", col("id") as "val")
        .write.format("delta").saveAsTable(tableName)

      val plan = transform(createSparkRelation(
        proto.DeltaRelation.newBuilder()
          .setOptimizeTable(
            proto.OptimizeTable.newBuilder()
              .setTable(proto.DeltaTable.newBuilder().setTableOrViewName(tableName))
              .addZorderColumns("val")
          )
      ))
      assert(plan.columns.toSeq === Seq("path", "metrics"))
      val df = Dataset.ofRows(spark, plan)

      checkOptimizeMetrics(df)
      checkOptimizeUsingHistory(
        tableName, expectedPredicates = Nil, expectedZorderCols = Seq("val"))
    }
  }

  test("optimize - z-order with partition filters") {
    val tableName = "test_table"
    withTable(tableName) {
      spark.range(1000).select(col("id") % 3 as "key", col("id") as "val")
        .write.partitionBy("key").format("delta").saveAsTable(tableName)

      val plan = transform(createSparkRelation(
        proto.DeltaRelation.newBuilder()
          .setOptimizeTable(
            proto.OptimizeTable.newBuilder()
              .setTable(proto.DeltaTable.newBuilder().setTableOrViewName(tableName))
              .addPartitionFilters("key = 1")
              .addZorderColumns("val")
          )
      ))
      assert(plan.columns.toSeq === Seq("path", "metrics"))
      val df = Dataset.ofRows(spark, plan)

      checkOptimizeMetrics(df)
      checkOptimizeUsingHistory(
        tableName, expectedPredicates = Seq("'key = 1"), expectedZorderCols = Seq("val"))
    }
  }

  private def checkOptimizeMetrics(df: DataFrame): Unit = {
    import testImplicits._
    val result = df.as[(String, OptimizeMetrics)].collect()
    assert(result.length === 1)
    val (_, metrics) = result.head
    assert(metrics.numFilesRemoved > metrics.numFilesAdded)
  }

  private def checkOptimizeUsingHistory(
      tableName: String, expectedPredicates: Seq[String], expectedZorderCols: Seq[String]): Unit = {
    import testImplicits._
    val (operation, operationParameters) = DeltaTable.forName(spark, tableName).history()
      .select("operation", "operationParameters").as[(String, Map[String, String])].head()
    assert(operation === "OPTIMIZE")
    assert(operationParameters("predicate") ===
      expectedPredicates.map(p => s"""\"($p)\"""").mkString(start = "[", sep = ",", end = "]"))
    assert(operationParameters("zOrderBy") ===
      expectedZorderCols.map(c => s"""\"$c\"""").mkString(start = "[", sep = ",", end = "]"))
  }

  private def createScan(tableName: String): spark_proto.Relation = {
    createSparkRelation(
      proto.DeltaRelation.newBuilder()
        .setScan(
          proto.Scan.newBuilder()
            .setTable(proto.DeltaTable.newBuilder().setTableOrViewName(tableName))
        )
    )
  }

  private def createSubqueryAlias(
      input: spark_proto.Relation, alias: String): spark_proto.Relation = {
    spark_proto.Relation.newBuilder()
      .setSubqueryAlias(
        spark_proto.SubqueryAlias.newBuilder()
          .setAlias(alias)
          .setInput(input)
      )
      .build()
  }

  private def createExpression(expr: String): spark_proto.Expression = {
    spark_proto.Expression.newBuilder()
      .setExpressionString(
        spark_proto.Expression.ExpressionString.newBuilder()
          .setExpression(expr)
      )
      .build()
  }

  private def createAssignment(field: String, value: String): proto.Assignment = {
    proto.Assignment.newBuilder()
      .setField(createExpression(field))
      .setValue(createExpression(value))
      .build()
  }

  private def getTimestampForVersion(log: DeltaLog, version: Long): String = {
    val file = new File(FileNames.unsafeDeltaFile(log.logPath, version).toUri)
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
    sdf.format(file.lastModified())
  }
}
