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

import scala.collection.JavaConverters._

import org.apache.spark.sql.delta.{DeltaAnalysisException, DeltaConfigs, DeltaLog}
import org.apache.spark.sql.delta.commands.{DescribeDeltaDetailCommand, DescribeDeltaHistory}
import org.apache.spark.sql.delta.commands.optimize.OptimizeMetrics
import org.apache.spark.sql.delta.hooks.GenerateSymlinkManifest
import org.apache.spark.sql.delta.sources.DeltaSourceUtils.GENERATION_EXPRESSION_METADATA_KEY
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaTestImplicits._
import org.apache.spark.sql.delta.util.{DeltaFileOperations, FileNames}
import com.google.protobuf
import io.delta.connect.proto
import io.delta.tables.DeltaTable
import io.grpc.stub.StreamObserver

import org.apache.spark.SparkConf
import org.apache.spark.connect.{proto => spark_proto}
import org.apache.spark.sql.{AnalysisException, DataFrame, Dataset, QueryTest, Row}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.ResolvedTable
import org.apache.spark.sql.connect.config.Connect
import org.apache.spark.sql.connect.planner.SparkConnectPlanner
import org.apache.spark.sql.connect.service.{ExecuteHolder, ExecuteStatus, SessionHolder, SessionStatus, SparkConnectService}
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.functions._
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{IntegerType, LongType, MetadataBuilder, StructField, StructType}
import org.apache.spark.util.Utils

class DeltaConnectPlannerSuite extends QueryTest with DeltaSQLCommandTest {
  import DeltaConnectPlannerSuite._

  protected override def sparkConf: SparkConf = {
    super.sparkConf
      .set(Connect.CONNECT_EXTENSIONS_RELATION_CLASSES.key, classOf[DeltaRelationPlugin].getName)
      .set(Connect.CONNECT_EXTENSIONS_COMMAND_CLASSES.key, classOf[DeltaCommandPlugin].getName)
  }

  private def createSparkRelation(relation: proto.DeltaRelation.Builder): spark_proto.Relation = {
    spark_proto.Relation.newBuilder().setExtension(protobuf.Any.pack(relation.build())).build()
  }

  private def createSparkCommand(command: proto.DeltaCommand.Builder): spark_proto.Command = {
    spark_proto.Command.newBuilder().setExtension(protobuf.Any.pack(command.build())).build()
  }

  class MockObserver extends StreamObserver[spark_proto.ExecutePlanResponse] {
    override def onNext(value: spark_proto.ExecutePlanResponse): Unit = {}
    override def onError(t: Throwable): Unit = {}
    override def onCompleted(): Unit = {}
  }

  private def transform(command: spark_proto.Command): Unit = {
    val executeHolder = buildExecutePlanHolder(command)
    new SparkConnectPlanner(executeHolder).process(command, new MockObserver())
  }

  def buildExecutePlanHolder(command: spark_proto.Command): ExecuteHolder = {
    val sessionHolder = SessionHolder.forTesting(spark)
    sessionHolder.eventManager.status_(SessionStatus.Started)

    val context = spark_proto.UserContext
      .newBuilder()
      .setUserId(sessionHolder.userId)
      .build()
    val plan = spark_proto.Plan
      .newBuilder()
      .setCommand(command)
      .build()
    val request = spark_proto.ExecutePlanRequest
      .newBuilder()
      .setPlan(plan)
      .setSessionId(sessionHolder.sessionId)
      .setUserContext(context)
      .build()
    val executeHolder = SparkConnectService.executionManager.createExecuteHolder(request)
    executeHolder.eventsManager.status_(ExecuteStatus.Started)
    executeHolder
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

      val result = new SparkConnectPlanner(SessionHolder.forTesting(spark)).transformRelation(input)
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

      val result = new SparkConnectPlanner(SessionHolder.forTesting(spark)).transformRelation(input)
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
      val plan = new SparkConnectPlanner(SessionHolder.forTesting(spark)).transformRelation(input)
      val result = Dataset.ofRows(spark, plan).collect()

      assert(result.length == 1)
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
      val plan = new SparkConnectPlanner(SessionHolder.forTesting(spark)).transformRelation(input)
      val result = Dataset.ofRows(spark, plan).collect()

      assert(result.length == 1)
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
      val plan = new SparkConnectPlanner(SessionHolder.forTesting(spark)).transformRelation(input)
      val result = Dataset.ofRows(spark, plan).collect()

      assert(result.length == 1)
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

      val result = new SparkConnectPlanner(SessionHolder.forTesting(spark)).transformRelation(input)
      result match {
        case DescribeDeltaHistory(_: ResolvedTable, None, _) =>
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

      val result = new SparkConnectPlanner(SessionHolder.forTesting(spark)).transformRelation(input)
      val expected = DeltaTable.forName(spark, "table").detail().queryExecution.analyzed

      assert(result.isInstanceOf[DescribeDeltaDetailCommand])
      val childResult = result.asInstanceOf[DescribeDeltaDetailCommand].child
      val childExpected = expected.asInstanceOf[DescribeDeltaDetailCommand].child

      assert(childResult.asInstanceOf[ResolvedTable].identifier.name ==
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

      val plan = new SparkConnectPlanner(SessionHolder.forTesting(spark)).transformRelation(input)
      assert(plan.columns.toSeq == expectedRestoreOutputColumns)
      val result = Dataset.ofRows(spark, plan).collect()
      assert(result.length == 1)
      assert(result.head.getLong(2) == 2) // Two files should have been removed.
      assert(spark.read.table("table").count() == 1000)
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

      val plan = new SparkConnectPlanner(SessionHolder.forTesting(spark)).transformRelation(input)
      assert(plan.columns.toSeq == expectedRestoreOutputColumns)
      val result = Dataset.ofRows(spark, plan).collect()
      assert(result.length == 1)
      assert(result.head.getLong(2) == 2) // Two files should have been removed.
      assert(spark.read.load(dir.getAbsolutePath).count() == 1000)
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

      val plan = new SparkConnectPlanner(SessionHolder.forTesting(spark)).transformRelation(input)
      assert(plan.schema.length == 1)
      val result = Dataset.ofRows(spark, plan).collect()
      assert(result.length == 1)
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

      val plan = new SparkConnectPlanner(SessionHolder.forTesting(spark)).transformRelation(input)
      assert(plan.schema.length == 1)
      val result = Dataset.ofRows(spark, plan).collect()
      assert(result.length == 1)
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

      val plan = new SparkConnectPlanner(SessionHolder.forTesting(spark)).transformRelation(input)
      val result = Dataset.ofRows(spark, plan).collect()
      assert(result.length == 1)
      assert(result.head.getLong(0) == 1000)
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

      val plan = new SparkConnectPlanner(SessionHolder.forTesting(spark)).transformRelation(input)
      val result = Dataset.ofRows(spark, plan).collect()
      assert(result.length == 1)
      assert(result.head.getLong(0) == 500)
      assert(spark.read.table(tableName).count() == 500)
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

      val plan = new SparkConnectPlanner(SessionHolder.forTesting(spark)).transformRelation(input)
      val result = Dataset.ofRows(spark, plan).collect()
      assert(result.length == 1)
      assert(result.head.getLong(0) == 1000)
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

      val plan = new SparkConnectPlanner(SessionHolder.forTesting(spark)).transformRelation(input)
      val result = Dataset.ofRows(spark, plan).collect()
      assert(result.length == 1)
      assert(result.head.getLong(0) == 500)
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
                      .addAssignments(createAssignment(field = "key", value = "key"))
                      .addAssignments(createAssignment(field = "value", value = "value"))
                  )
              )
          )
      )

      val plan = new SparkConnectPlanner(SessionHolder.forTesting(spark)).transformRelation(input)
      val result = Dataset.ofRows(spark, plan).collect()
      assert(result.length == 1)
      assert(result.head.getLong(0) == 50) // num_affected_rows
      assert(result.head.getLong(1) == 0) // num_updated_rows
      assert(result.head.getLong(2) == 0) // num_deleted_rows
      assert(result.head.getLong(3) == 50) // num_inserted_rows

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

      val plan = new SparkConnectPlanner(SessionHolder.forTesting(spark)).transformRelation(input)
      val result = Dataset.ofRows(spark, plan).collect()
      assert(result.length == 1)
      assert(result.head.getLong(0) == 50) // num_affected_rows
      assert(result.head.getLong(1) == 50) // num_updated_rows
      assert(result.head.getLong(2) == 0) // num_deleted_rows
      assert(result.head.getLong(3) == 0) // num_inserted_rows

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

      val plan = new SparkConnectPlanner(SessionHolder.forTesting(spark)).transformRelation(input)
      val result = Dataset.ofRows(spark, plan).collect()
      assert(result.length == 1)
      assert(result.head.getLong(0) == 125) // num_affected_rows
      assert(result.head.getLong(1) == 50) // num_updated_rows
      assert(result.head.getLong(2) == 25) // num_deleted_rows
      assert(result.head.getLong(3) == 50) // num_inserted_rows

      checkAnswer(
        spark.read.table(targetTableName),
        Seq.tabulate(25)(i => Row(25 + i, 25 + i)) ++
          Seq.tabulate(50)(i => Row(i + 50, i + 1050)) ++
          Seq.tabulate(50)(i => Row(i + 100, i + 1100))
      )
    }
  }

  test("clone - deep") {
    withTempDir { targetDir =>
      val sourceTableName = "source_table"
      withTable(sourceTableName) {
        spark.range(end = 1000).write.format("delta").saveAsTable(sourceTableName)

        transform(createSparkCommand(
          proto.DeltaCommand.newBuilder()
            .setCloneTable(
              proto.CloneTable.newBuilder()
                .setTable(proto.DeltaTable.newBuilder().setTableOrViewName(sourceTableName))
                .setTarget(targetDir.getAbsolutePath)
                .setIsShallow(false)
                .setReplace(true)
            )
        ))

        // Check that we have successfully cloned the table.
        checkAnswer(
          spark.read.load(targetDir.getAbsolutePath),
          spark.read.table(sourceTableName))

        // Check that a deep clone was performed
        val log = DeltaLog.forTable(spark, targetDir)
        log.update().allFiles.collect().foreach { f =>
          assert(!f.pathAsUri.isAbsolute)
        }
      }
    }
  }

  test("clone - shallow") {
    withTempDir { targetDir =>
      val sourceTableName = "source_table"
      withTable(sourceTableName) {
        spark.range(end = 1000).write.format("delta").saveAsTable(sourceTableName)

        transform(createSparkCommand(
          proto.DeltaCommand.newBuilder()
            .setCloneTable(
              proto.CloneTable.newBuilder()
                .setTable(proto.DeltaTable.newBuilder().setTableOrViewName(sourceTableName))
                .setTarget(targetDir.getAbsolutePath)
                .setIsShallow(true)
                .setReplace(true)
            )
        ))

        // Check that we have successfully cloned the table.
        checkAnswer(
          spark.read.load(targetDir.getAbsolutePath),
          spark.read.table(sourceTableName))

        // Check that a shallow clone was performed
        val log = DeltaLog.forTable(spark, targetDir)
        log.update().allFiles.collect().foreach { f =>
          assert(f.pathAsUri.isAbsolute)
        }
      }
    }
  }

  test("clone - replace") {
    withTempDir { targetDir =>
      val sourceTableName = "source_table"
      withTable(sourceTableName) {
        spark.range(end = 1000).write.format("delta").saveAsTable(sourceTableName)
        spark.range(end = 100).write.format("delta").save(targetDir.getAbsolutePath)

        // Check that the table is not replaced when `replace` is set to false.
        val exception = intercept[DeltaAnalysisException] {
          transform(createSparkCommand(
            proto.DeltaCommand.newBuilder()
              .setCloneTable(
                proto.CloneTable.newBuilder()
                  .setTable(proto.DeltaTable.newBuilder().setTableOrViewName(sourceTableName))
                  .setTarget(targetDir.getAbsolutePath)
                  .setIsShallow(false)
                  .setReplace(false)
              )
          ))
        }
        checkError(
          exception,
          errorClass = "DELTA_CREATE_TABLE_WITH_NON_EMPTY_LOCATION",
          parameters = Map(
            "tableId" -> s"`delta`.`${targetDir.getAbsolutePath}`",
            "tableLocation" -> targetDir.getAbsolutePath
          )
        )

        // Check that the table is replaced when `replace` is set to true.
        transform(createSparkCommand(
          proto.DeltaCommand.newBuilder()
            .setCloneTable(
              proto.CloneTable.newBuilder()
                .setTable(proto.DeltaTable.newBuilder().setTableOrViewName(sourceTableName))
                .setTarget(targetDir.getAbsolutePath)
                .setIsShallow(false)
                .setReplace(true)
            )
        ))
        assert(spark.read.load(targetDir.getAbsolutePath).count() == 1000)
      }
    }
  }

  test("clone - as of version") {
    withTempDir { targetDir =>
      val sourceTableName = "source_table"
      withTable(sourceTableName) {
        spark.range(end = 1000).write.format("delta").saveAsTable(sourceTableName)
        spark.range(end = 2000).write.format("delta").mode("append").saveAsTable(sourceTableName)

        transform(createSparkCommand(
          proto.DeltaCommand.newBuilder()
            .setCloneTable(
              proto.CloneTable.newBuilder()
                .setTable(proto.DeltaTable.newBuilder().setTableOrViewName(sourceTableName))
                .setTarget(targetDir.getAbsolutePath)
                .setVersion(0)
                .setIsShallow(false)
                .setReplace(false)
            )
        ))
        assert(spark.read.load(targetDir.getAbsolutePath).count() == 1000)
      }
    }
  }

  test("clone - as of timestamp") {
    withTempDir { targetDir =>
      val sourceTableName = "source_table"
      withTable(sourceTableName) {
        spark.range(end = 1000).write.format("delta").saveAsTable(sourceTableName)
        spark.range(end = 2000).write.format("delta").mode("append").saveAsTable(sourceTableName)

        val log = DeltaLog.forTable(spark, TableIdentifier(sourceTableName))
        transform(createSparkCommand(
          proto.DeltaCommand.newBuilder()
            .setCloneTable(
              proto.CloneTable.newBuilder()
                .setTable(proto.DeltaTable.newBuilder().setTableOrViewName(sourceTableName))
                .setTarget(targetDir.getAbsolutePath)
                .setTimestamp(getTimestampForVersion(log, version = 0))
                .setIsShallow(false)
                .setReplace(false)
            )
        ))
        assert(spark.read.load(targetDir.getAbsolutePath).count() == 1000)
      }
    }
  }

  test("optimize - compaction") {
    val tableName = "test_table"
    withTable(tableName) {
      spark.range(1000).select(col("id") % 3 as "key", col("id") as "val")
        .write.format("delta").saveAsTable(tableName)

      val plan = new SparkConnectPlanner(SessionHolder.forTesting(spark))
        .transformRelation(createSparkRelation(
        proto.DeltaRelation.newBuilder()
          .setOptimizeTable(
            proto.OptimizeTable.newBuilder()
              .setTable(proto.DeltaTable.newBuilder().setTableOrViewName(tableName))
          )
      ))
      assert(plan.columns.toSeq == Seq("path", "metrics"))
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

      val plan = new SparkConnectPlanner(SessionHolder.forTesting(spark))
        .transformRelation(createSparkRelation(
        proto.DeltaRelation.newBuilder()
          .setOptimizeTable(
            proto.OptimizeTable.newBuilder()
              .setTable(proto.DeltaTable.newBuilder().setTableOrViewName(tableName))
              .addPartitionFilters("key = 1")
          )
      ))
      assert(plan.columns.toSeq == Seq("path", "metrics"))
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

      val plan = new SparkConnectPlanner(SessionHolder.forTesting(spark))
        .transformRelation(createSparkRelation(
        proto.DeltaRelation.newBuilder()
          .setOptimizeTable(
            proto.OptimizeTable.newBuilder()
              .setTable(proto.DeltaTable.newBuilder().setTableOrViewName(tableName))
              .addZorderColumns("val")
          )
      ))
      assert(plan.columns.toSeq == Seq("path", "metrics"))
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

      val plan = new SparkConnectPlanner(SessionHolder.forTesting(spark))
        .transformRelation(createSparkRelation(
        proto.DeltaRelation.newBuilder()
          .setOptimizeTable(
            proto.OptimizeTable.newBuilder()
              .setTable(proto.DeltaTable.newBuilder().setTableOrViewName(tableName))
              .addPartitionFilters("key = 1")
              .addZorderColumns("val")
          )
      ))
      assert(plan.columns.toSeq == Seq("path", "metrics"))
      val df = Dataset.ofRows(spark, plan)

      checkOptimizeMetrics(df)
      checkOptimizeUsingHistory(
        tableName, expectedPredicates = Seq("'key = 1"), expectedZorderCols = Seq("val"))
    }
  }

  private def checkOptimizeMetrics(df: DataFrame): Unit = {
    import testImplicits._
    val result = df.as[(String, OptimizeMetrics)].collect()
    assert(result.length == 1)
    val (_, metrics) = result.head
    assert(metrics.numFilesRemoved > metrics.numFilesAdded)
  }

  private def checkOptimizeUsingHistory(
      tableName: String, expectedPredicates: Seq[String], expectedZorderCols: Seq[String]): Unit = {
    import testImplicits._
    val (operation, operationParameters) = DeltaTable.forName(spark, tableName).history()
      .select("operation", "operationParameters").as[(String, Map[String, String])].head()
    assert(operation == "OPTIMIZE")
    assert(operationParameters("predicate") ==
      expectedPredicates.map(p => s"""\"($p)\"""").mkString(start = "[", sep = ",", end = "]"))
    assert(operationParameters("zOrderBy") ==
      expectedZorderCols.map(c => s"""\"$c\"""").mkString(start = "[", sep = ",", end = "]"))
  }

  test("vacuum - without retention hours argument") {
    val tableName = "test_table"

    def runVacuum(): Unit = {
      transform(createSparkCommand(
        proto.DeltaCommand.newBuilder()
          .setVacuumTable(
            proto.VacuumTable.newBuilder()
              .setTable(proto.DeltaTable.newBuilder().setTableOrViewName(tableName))
          )
      ))
    }

    def setRetentionInterval(retentionInterval: String): Unit = {
      spark.sql(
        s"""ALTER TABLE $tableName
           |SET TBLPROPERTIES (
           |  '${DeltaConfigs.TOMBSTONE_RETENTION.key}' = '$retentionInterval'
           |)""".stripMargin
      )
    }

    withTable(tableName) {
      // Set up a Delta table with an untracked file.
      spark.range(1000).write.format("delta").saveAsTable(tableName)
      val log = DeltaLog.forTable(spark, TableIdentifier(tableName))
      val tempFile =
        new File(DeltaFileOperations.absolutePath(log.dataPath.toString, "abc.parquet").toUri)
      tempFile.createNewFile()

      // Run a vacuum with the untracked file still within the retention period.
      setRetentionInterval(retentionInterval = "1000 hours")
      runVacuum()
      assert(tempFile.exists())

      // Run a vacuum with the untracked file outside of the retention period.
      setRetentionInterval(retentionInterval = "0 hours")
      runVacuum()
      assert(!tempFile.exists())
    }
  }

  test("vacuum - with retention hours argument") {
    val tableName = "test_table"

    def runVacuum(retentionHours: Float): Unit = {
      transform(createSparkCommand(
        proto.DeltaCommand.newBuilder()
          .setVacuumTable(
            proto.VacuumTable.newBuilder()
              .setTable(proto.DeltaTable.newBuilder().setTableOrViewName(tableName))
              .setRetentionHours(retentionHours)
          )
      ))
    }

    withTable(tableName) {
      // Set up a Delta table with an untracked file.
      spark.range(1000).write.format("delta").saveAsTable(tableName)
      val log = DeltaLog.forTable(spark, TableIdentifier(tableName))
      val tempFile =
        new File(DeltaFileOperations.absolutePath(log.dataPath.toString, "abc.parquet").toUri)
      tempFile.createNewFile()

      // Run a vacuum with the untracked file still within the retention period.
      runVacuum(retentionHours = 1000.0f)
      assert(tempFile.exists())

      // Run a vacuum with the untracked file outside of the retention period.
      withSQLConf(DeltaSQLConf.DELTA_VACUUM_RETENTION_CHECK_ENABLED.key -> "false") {
        runVacuum(retentionHours = 0.0f)
      }
      assert(!tempFile.exists())
    }
  }

  test("upgrade table protocol") {
    val tableName = "test_table"
    withTable(tableName) {
      // Create a Delta table with protocol version (1, 1).
      val oldReaderVersion = 1
      val oldWriterVersion = 1
      spark.range(1000)
        .write
        .format("delta")
        .option(DeltaConfigs.MIN_READER_VERSION.key, oldReaderVersion)
        .option(DeltaConfigs.MIN_WRITER_VERSION.key, oldWriterVersion)
        .saveAsTable(tableName)

      // Check that protocol version is as expected, before we upgrade it.
      val deltaLog = DeltaLog.forTable(spark, TableIdentifier(tableName))
      val oldProtocol = deltaLog.update().protocol
      assert(oldProtocol.minReaderVersion == oldReaderVersion)
      assert(oldProtocol.minWriterVersion == oldWriterVersion)

      // Use Delta Connect to upgrade the protocol of the table.
      val newReaderVersion = 3
      val newWriterVersion = 7
      transform(createSparkCommand(
        proto.DeltaCommand.newBuilder()
          .setUpgradeTableProtocol(
            proto.UpgradeTableProtocol.newBuilder()
              .setTable(proto.DeltaTable.newBuilder().setTableOrViewName(tableName))
              .setReaderVersion(newReaderVersion)
              .setWriterVersion(newWriterVersion)
          )
      ))

      // Check that protocol version is as expected, after we have upgraded it.
      val newProtocol = deltaLog.update().protocol
      assert(newProtocol.minReaderVersion == newReaderVersion)
      assert(newProtocol.minWriterVersion == newWriterVersion)
    }
  }

  test("generate manifest") {
    withTempDir { dir =>
      spark.range(1000).write.format("delta").mode("overwrite").save(dir.getAbsolutePath)

      val manifestFile = new File(dir, GenerateSymlinkManifest.MANIFEST_LOCATION)
      assert(!manifestFile.exists())

      transform(createSparkCommand(
        proto.DeltaCommand.newBuilder()
          .setGenerate(
            proto.Generate.newBuilder()
              .setTable(
                proto.DeltaTable.newBuilder()
                  .setPath(proto.DeltaTable.Path.newBuilder().setPath(dir.getAbsolutePath)))
              .setMode("symlink_format_manifest"))))

      assert(manifestFile.exists())
    }
  }

  test("create delta table - basic") {
    withTempDir { dir =>
      val tableName = "test_table"
      withTable(tableName) {
        val tableComment = "table comment"
        val nameColA = "colA"
        val generatedAlwaysAsColA = "1"
        val nameColB = "colB"
        val commentColB = "colB comment"
        val tableProperties = Map("k1" -> "v1", "k2" -> "v2")
        transform(createSparkCommand(
          proto.DeltaCommand.newBuilder()
            .setCreateDeltaTable(
              proto.CreateDeltaTable.newBuilder()
                .setMode(proto.CreateDeltaTable.Mode.MODE_CREATE)
                .setTableName(tableName)
                .setLocation(dir.getAbsolutePath)
                .setComment(tableComment)
                .addColumns(
                  proto.CreateDeltaTable.Column.newBuilder()
                    .setName(nameColA)
                    .setDataType(UNPARSED_INT_DATA_TYPE)
                    .setNullable(true)
                    .setGeneratedAlwaysAs(generatedAlwaysAsColA))
                .addColumns(
                  proto.CreateDeltaTable.Column.newBuilder()
                    .setName(nameColB)
                    .setDataType(LONG_DATA_TYPE)
                    .setNullable(false)
                    .setComment(commentColB))
                .putAllProperties(tableProperties.asJava))))

        val table = spark.sessionState.catalog.getTableMetadata(TableIdentifier(tableName))
        assert(table.comment.contains(tableComment))
        assert(table.location.getPath == dir.getAbsolutePath)
        assert(table.properties == tableProperties)

        val colAMetadata = new MetadataBuilder()
          .putString(GENERATION_EXPRESSION_METADATA_KEY, generatedAlwaysAsColA)
          .build()
        val colBMetadata = new MetadataBuilder().putString("comment", commentColB).build()
        val expectedSchema = StructType(Seq(
          StructField(nameColA, IntegerType, nullable = true, colAMetadata),
          StructField(nameColB, LongType, nullable = false, colBMetadata)))
        assert(table.schema == expectedSchema)
      }
    }
  }

  test("create delta table - modes") {
    val tableName = "test_table"
    def createTable(mode: proto.CreateDeltaTable.Mode, colName: String): Unit = {
      transform(createSparkCommand(
        proto.DeltaCommand.newBuilder()
          .setCreateDeltaTable(
            proto.CreateDeltaTable.newBuilder()
              .setMode(mode)
              .setTableName(tableName)
              .addColumns(
                proto.CreateDeltaTable.Column.newBuilder()
                  .setName(colName)
                  .setDataType(LONG_DATA_TYPE)
                  .setNullable(true)))))
    }

    def getColumnName(): String = {
      spark.sessionState.catalog.getTableMetadata(TableIdentifier(tableName)).schema.head.name
    }

    val expectedQualifiedTableName = {
      s"`default`.`$tableName`"
    }

    withTable(tableName) {
      val replaceError = intercept[AnalysisException] {
        createTable(proto.CreateDeltaTable.Mode.MODE_REPLACE, colName = "column1")
      }
      checkError(
        replaceError,
        errorClass = "TABLE_OR_VIEW_NOT_FOUND",
        parameters = Map("relationName" -> expectedQualifiedTableName))

      createTable(proto.CreateDeltaTable.Mode.MODE_CREATE, colName = "column2")
      assert(getColumnName() == "column2")

      val createError = intercept[AnalysisException] {
        createTable(proto.CreateDeltaTable.Mode.MODE_CREATE, colName = "column3")
      }
      checkError(
        createError,
        errorClass = "TABLE_OR_VIEW_ALREADY_EXISTS",
        parameters = Map("relationName" -> s"`default`.`$tableName`"))

      createTable(proto.CreateDeltaTable.Mode.MODE_REPLACE, colName = "column4")
      assert(getColumnName() == "column4")

      createTable(proto.CreateDeltaTable.Mode.MODE_CREATE_IF_NOT_EXISTS, colName = "column5")
      assert(getColumnName() == "column4")

      spark.sql(s"DROP TABLE $tableName")
      createTable(proto.CreateDeltaTable.Mode.MODE_CREATE_IF_NOT_EXISTS, colName = "column6")
      assert(getColumnName() == "column6")

      createTable(proto.CreateDeltaTable.Mode.MODE_CREATE_OR_REPLACE, colName = "column7")
      assert(getColumnName() == "column7")

      spark.sql(s"DROP TABLE $tableName")
      createTable(proto.CreateDeltaTable.Mode.MODE_CREATE_OR_REPLACE, colName = "column8")
      assert(getColumnName() == "column8")
    }
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

object DeltaConnectPlannerSuite {
  val UNPARSED_INT_DATA_TYPE: spark_proto.DataType = spark_proto.DataType
    .newBuilder()
    .setUnparsed(spark_proto.DataType.Unparsed.newBuilder().setDataTypeString("int"))
    .build()

  val LONG_DATA_TYPE: spark_proto.DataType = spark_proto.DataType
    .newBuilder()
    .setLong(spark_proto.DataType.Long.newBuilder())
    .build()
}
