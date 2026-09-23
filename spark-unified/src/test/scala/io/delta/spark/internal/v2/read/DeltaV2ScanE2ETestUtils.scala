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

package io.delta.spark.internal.v2.read

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.delta.DeltaTableProvider
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.{DeltaSQLCommandTest, DeltaSQLTestUtils}

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, QueryTest}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation

/** Provides shared table fixtures and path-neutral plan hooks for Delta V2 E2E suites.
 * Concrete suites combine these utilities with reusable tests and path-specific overrides.
 */
private[read] trait DeltaV2ScanE2ETestUtils
  extends QueryTest
  with DeltaSQLCommandTest
  with DeltaSQLTestUtils
  with DeltaTableProvider
  with AdaptiveSparkPlanHelper
  with DeltaV2BatchScanAssertions {

  protected def configureE2ESparkConf(conf: SparkConf): SparkConf = conf

  abstract override protected def sparkConf: SparkConf =
    configureE2ESparkConf(
      super.sparkConf.set(DeltaSQLConf.V2_ENABLE_MODE.key, "STRICT"))

  protected def withV1Mode[T](body: => T): T =
    withSQLConf(DeltaSQLConf.V2_ENABLE_MODE.key -> "NONE")(body)

  protected def withDeltaTable(
      name: String,
      properties: String = "")(body: => Unit): Unit = {
    withTable(name) {
      withV1Mode {
        sql(s"CREATE TABLE $name (id LONG, value STRING) USING $tableProvider $properties")
        sql(s"INSERT INTO $name VALUES (1, 'a'), (2, 'b'), (3, 'c')")
      }
      body
    }
  }

  protected def withPartitionColumnInMiddleTable(
      name: String,
      properties: String = "")(body: => Unit): Unit = {
    withTable(name) {
      withV1Mode {
        sql(
          s"CREATE TABLE $name (id LONG, part LONG, col3 INT) USING $tableProvider " +
            s"PARTITIONED BY (part) $properties")
        sql(
          s"INSERT INTO $name VALUES " +
            "(1, 10, 100), (2, 20, 200), (3, 30, 300)")
      }
      body
    }
  }

  protected def fileCount(table: String): Long = {
    withV1Mode {
      sql(s"DESCRIBE DETAIL $table")
        .selectExpr("CAST(numFiles AS BIGINT)")
        .head()
        .getLong(0)
    }
  }

  protected def withDeltaV2ScanForSelectedFiles[T](
      query: => DataFrame,
      context: String)(body: DeltaV2Scan => T): T = {
    val df = query
    val plan = df.queryExecution.optimizedPlan
    val scans = plan.collect {
      case relation: DataSourceV2ScanRelation
          if relation.scan.isInstanceOf[DeltaV2Scan] =>
        relation.scan.asInstanceOf[DeltaV2Scan]
    }
    assert(
      scans.length == 1,
      s"expected one optimized DeltaV2Scan for $context, got " +
        s"${scans.length}:\n$plan")
    body(scans.head)
  }

  private def selectedFileDeletionVectorFlags(
      query: => DataFrame,
      context: String): Seq[Boolean] = {
    withDeltaV2ScanForSelectedFiles(query, context) { scan =>
      scan.getSelectedFiles.asScala.map(_.getDeletionVector.isPresent).toSeq
    }
  }

  protected def assertAnySelectedFileHasDeletionVector(
      query: => DataFrame,
      context: String): Unit = {
    val hasDeletionVectors = selectedFileDeletionVectorFlags(query, context)
    assert(
      hasDeletionVectors.contains(true),
      s"expected a selected file with a deletion vector for $context")
  }

  protected def assertMixedSelectedFileDeletionVectors(
      query: => DataFrame,
      context: String): Unit = {
    val hasDeletionVectors = selectedFileDeletionVectorFlags(query, context)
    assert(
      hasDeletionVectors.contains(true) && hasDeletionVectors.contains(false),
      s"expected selected files with and without deletion vectors for $context")
  }

  protected def assertAllSelectedFilesHaveDeletionVectors(
      query: => DataFrame,
      context: String): Unit = {
    val hasDeletionVectors = selectedFileDeletionVectorFlags(query, context)
    assert(
      hasDeletionVectors.nonEmpty && hasDeletionVectors.forall(identity),
      s"expected all selected files to have deletion vectors for $context")
  }

  protected def assertExpectedScan(df: DataFrame, context: String): Unit =
    assertDeltaV2BatchScan(df, context)

  protected def assertExpectedScanCount(
      df: DataFrame,
      expected: Int,
      context: String): Unit =
    assertDeltaV2BatchScanCount(df, expected, context)

  protected def expectedScanRequiredFieldNames(
      df: DataFrame,
      expectedScanCount: Int,
      context: String): Seq[Seq[String]] =
    deltaV2BatchScanReadFieldNames(df, expectedScanCount, context)

  protected def assertExpectedPushedLimit(
      df: DataFrame,
      expected: Int,
      context: String): Unit =
    assertDeltaV2BatchScanPushedLimit(df, expected, context)

  protected def assertExpectedNoPushedLimit(df: DataFrame, context: String): Unit =
    assertDeltaV2BatchScanHasNoPushedLimit(df, context)

  protected def assertRouteSpecificInputFileCount(
      df: DataFrame,
      expected: Int,
      context: String): Unit = {}

  protected def checkRead(context: String)(
      query: => DataFrame)(
      assertions: DataFrame => Unit): Unit = {
    val df = query
    assertions(df)
    assertExpectedScan(df, context)
  }
}
