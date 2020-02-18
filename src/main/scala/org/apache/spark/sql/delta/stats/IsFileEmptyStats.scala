/*
 * Copyright 2019 Databricks, Inc.
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

package org.apache.spark.sql.delta.stats

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.{WriteJobStatsTracker, WriteTaskStats, WriteTaskStatsTracker}

case class IsFileEmptyStats(nonEmptyFiles: Set[String]) extends WriteTaskStats

class IsFileEmptyTaskStatTracker extends WriteTaskStatsTracker with Logging {

  private[this] var nonEmptyFiles = Set[String]()
  private[this] var curFile: Option[String] = None

  override def newFile(filePath: String): Unit = {
    curFile = Some(filePath)
  }

  override def newRow(row: InternalRow): Unit = {
    curFile.foreach { path =>
      nonEmptyFiles += path
    }
  }

  override def newPartition(partitionValues: InternalRow): Unit = {
    // Not implemented.
  }

  override def newBucket(bucketId: Int): Unit = {
    // Not implemented.
  }

  override def getFinalStats(): WriteTaskStats = {
    IsFileEmptyStats(nonEmptyFiles)
  }
}

class IsFileEmptyStatTracker extends WriteJobStatsTracker {

  var nonEmptyFiles: Set[String] = Set[String]()

  override def newTaskInstance(): WriteTaskStatsTracker = {
    new IsFileEmptyTaskStatTracker
  }

  override def processStats(stats: Seq[WriteTaskStats]): Unit = {
    val basicStats = stats.map(_.asInstanceOf[IsFileEmptyStats])

    basicStats.foreach { summary =>
      nonEmptyFiles ++= summary.nonEmptyFiles
    }
  }

}
