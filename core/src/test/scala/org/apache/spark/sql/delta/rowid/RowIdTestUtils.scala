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

package org.apache.spark.sql.delta.rowid

import org.apache.spark.sql.delta.{DeltaLog, RowId, RowIdFeature}
import org.apache.spark.sql.delta.actions.AddFile
import org.apache.spark.sql.delta.actions.TableFeatureProtocolUtils.{defaultPropertyKey, propertyKey}
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest

import org.apache.spark.SparkConf
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SharedSparkSession

trait RowIdTestUtils extends QueryTest
  with SharedSparkSession
  with DeltaSQLCommandTest {

  val rowIdFeatureName: String = propertyKey(RowIdFeature)
  val defaultRowIdFeatureProperty: String = defaultPropertyKey(RowIdFeature)

  override protected def sparkConf: SparkConf =
    super.sparkConf.set(DeltaSQLConf.ROW_IDS_ALLOWED.key, "true")

  def withRowIdsEnabled(enabled: Boolean)(f: => Unit): Unit = {
    // Even when we don't want Row Ids on created tables, we want to enable code paths that
    // interact with them, which is controlled by this config.
    assert(spark.conf.get(DeltaSQLConf.ROW_IDS_ALLOWED.key) == "true")
    val configPairs = if (enabled) Seq(defaultRowIdFeatureProperty -> "supported") else Seq.empty
    withSQLConf(configPairs: _*)(f)
  }

  protected def getRowIdRangeInclusive(f: AddFile): (Long, Long) = {
    val min = f.baseRowId.get.toLong
    val max = min + f.numPhysicalRecords.get - 1L
    (min, max)
  }

  def assertRowIdsDoNotOverlap(log: DeltaLog): Unit = {
    val files = log.update().allFiles.collect()

    val sortedRanges = files
      .map(f => (f.path, getRowIdRangeInclusive(f)))
      .sortBy { case (_, (min, _)) => min }

    for (i <- sortedRanges.indices.dropRight(1)) {
      val (curPath, (_, curMax)) = sortedRanges(i)
      val (nextPath, (nextMin, _)) = sortedRanges(i + 1)
      assert(curMax < nextMin, s"$curPath and $nextPath have overlapping row IDs")
    }
  }

  def assertHighWatermarkIsCorrect(log: DeltaLog): Unit = {
    val snapshot = log.update()
    val files = snapshot.allFiles.collect()

    val highWatermarkOpt = snapshot.rowIdHighWaterMarkOpt
    if (files.isEmpty) {
      assert(highWatermarkOpt.isDefined)
    } else {
      val maxAssignedRowId = files
        .map(a => a.baseRowId.get + a.numPhysicalRecords.get - 1L)
        .max
      assert(highWatermarkOpt.get.highWaterMark == maxAssignedRowId)
    }
  }

  def assertRowIdsAreValid(log: DeltaLog): Unit = {
    assertRowIdsDoNotOverlap(log)
    assertHighWatermarkIsCorrect(log)
  }

  def assertHighWatermarkIsCorrectAfterUpdate(
      log: DeltaLog, highWatermarkBeforeUpdate: Long, expectedNumRecordsWritten: Long): Unit = {
    val highWaterMarkAfterUpdate =
      RowId.extractHighWatermark(spark, log.update()).get.highWaterMark
    assert((highWatermarkBeforeUpdate + expectedNumRecordsWritten) === highWaterMarkAfterUpdate)
    assertRowIdsAreValid(log)
  }

  def assertRowIdsAreNotSet(log: DeltaLog): Unit = {
    val snapshot = log.update()

    val highWatermarks = snapshot.rowIdHighWaterMarkOpt
    assert(highWatermarks.isEmpty)

    val files = snapshot.allFiles.collect()
    assert(files.forall(_.baseRowId.isEmpty))
  }
}
