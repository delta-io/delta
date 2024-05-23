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

package io.delta.connect

import java.io.File
import java.text.SimpleDateFormat

import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.util.FileNames
import com.google.protobuf
import io.delta.tables.DeltaTable

import org.apache.spark.SparkConf
import org.apache.spark.connect.{proto => spark_proto}
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.connect.config.Connect
import org.apache.spark.sql.connect.planner.SparkConnectPlanner
import org.apache.spark.sql.connect.service.SessionHolder
import org.apache.spark.sql.functions._
import org.apache.spark.sql.test.SharedSparkSession

class DeltaConnectPlannerSuite extends QueryTest with SharedSparkSession {

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
        spark.range(end = 0).write.format("delta").save(targetDir.getAbsolutePath)

        // Check that the table is not replaced when `replace` is set to false.
        val exception = intercept[DeltaAnalysisException] {
          transform(createSparkCommand(
            proto.DeltaCommand.newBuilder()
              .setCloneTable(
                proto.CloneTable.newBuilder()
                  .setTable(proto.DeltaTable.newBuilder().setTableOrViewName(sourceTableName))
                  .setTarget(targetDir.getAbsolutePath)
                  .setIsShallow(true)
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
                .setIsShallow(true)
                .setReplace(true)
            )
        ))
        assert(spark.read.load(targetDir.getAbsolutePath).count() === 1000)
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
                .setIsShallow(true)
                .setReplace(false)
            )
        ))
        assert(spark.read.load(targetDir.getAbsolutePath).count() === 1000)
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
                .setIsShallow(true)
                .setReplace(false)
            )
        ))
        assert(spark.read.load(targetDir.getAbsolutePath).count() === 1000)
      }
    }
  }

  private def getTimestampForVersion(log: DeltaLog, version: Long): String = {
    val file = new File(FileNames.unsafeDeltaFile(log.logPath, version).toUri)
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
    sdf.format(file.lastModified())
  }
}
