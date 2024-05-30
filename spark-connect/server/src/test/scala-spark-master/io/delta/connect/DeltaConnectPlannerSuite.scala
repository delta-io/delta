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

import com.google.protobuf
import io.delta.connect.proto
import io.delta.connect.spark.{proto => spark_proto}
import io.delta.tables.DeltaTable

import org.apache.spark.SparkConf
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.connect.config.Connect
import org.apache.spark.sql.connect.delta.ImplicitProtoConversions._
import org.apache.spark.sql.connect.planner.{SparkConnectPlanner, SparkConnectPlanTest}
import org.apache.spark.sql.connect.service.SessionHolder
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest

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
}
