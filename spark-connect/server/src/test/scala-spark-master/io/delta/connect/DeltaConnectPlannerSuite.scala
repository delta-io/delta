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

import com.google.protobuf
import io.delta.connect.proto
import io.delta.connect.spark.{proto => spark_proto}
import io.delta.tables.DeltaTable

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, QueryTest, SparkSession}
import org.apache.spark.sql.catalyst.analysis.ResolvedTable
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
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

      val result = new SparkConnectPlanner(createDummySessionHolder(spark))
        .transformRelation(input)
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

      val result = new SparkConnectPlanner(createDummySessionHolder(spark))
        .transformRelation(input)
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

      val result = new SparkConnectPlanner(createDummySessionHolder(spark)).transformRelation(input)
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

  private def getTimestampForVersion(log: DeltaLog, version: Long): String = {
    val file = new File(FileNames.unsafeDeltaFile(log.logPath, version).toUri)
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
    sdf.format(file.lastModified())
  }
}
