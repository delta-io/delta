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

package org.apache.spark.sql.delta.v2.interop

import java.sql.Timestamp

import org.apache.spark.sql.delta.{DeltaErrors, VersionNotFoundException => V1VersionNotFoundException}
import org.apache.spark.sql.delta.util.{DateTimeUtils, TimestampFormatter}
import io.delta.spark.internal.v2.exception.{NoRecreatableHistoryException, TableNotFoundException, TimestampOutOfRangeException, VersionNotFoundException}
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.internal.SQLConf

/**
 * Maps V2 connector exceptions to their V1 [[DeltaErrors]] equivalents.
 *
 * Each method throws the mapped V1 error (never returns). The V1 errors are checked
 * `AnalysisException`s, so throwing them from a Scala file lets the caller propagate them without
 * a `throws` clause.
 */
object DeltaV2ErrorInterop {

  def throwAsDeltaError(e: TimestampOutOfRangeException): Nothing = {
    val userTs = new Timestamp(e.getUserMillis)
    val commitTs = new Timestamp(e.getCommitMillis)
    val tsString = DateTimeUtils.timestampToString(
      TimestampFormatter(DateTimeUtils.getTimeZone(SQLConf.get.sessionLocalTimeZone)),
      DateTimeUtils.fromJavaTimestamp(commitTs))
    if (e.isAfterLatest) {
      throw DeltaErrors.timestampGreaterThanLatestCommit(userTs, commitTs, tsString)
    } else {
      throw DeltaErrors.TimestampEarlierThanCommitRetentionException(userTs, commitTs, tsString)
    }
  }

  def throwAsDeltaError(e: VersionNotFoundException): Nothing =
    throw V1VersionNotFoundException(e.getUserVersion, e.getEarliest, e.getLatest)

  def throwAsDeltaError(e: TableNotFoundException): Nothing =
    throw DeltaErrors.pathNotExistsException(e.getTablePath)

  def throwAsDeltaError(e: NoRecreatableHistoryException): Nothing =
    throw DeltaErrors.noRecreatableHistoryFound(new Path(e.getTablePath))
}
