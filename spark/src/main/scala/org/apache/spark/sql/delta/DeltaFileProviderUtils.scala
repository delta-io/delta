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

package org.apache.spark.sql.delta

import org.apache.spark.sql.delta.actions.Action
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.storage.ClosableIterator

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.JsonToStructs
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.types.UTF8String

object DeltaFileProviderUtils extends DeltaLogging {

  protected def readThreadPool = SnapshotManagement.deltaLogAsyncUpdateThreadPool

  /** Put any future parsing options here. */
  val jsonStatsParseOption = Map.empty[String, String]

  private[delta] def createJsonStatsParser(schemaToUse: StructType): String => InternalRow = {
    val parser = JsonToStructs(
      schema = schemaToUse,
      options = jsonStatsParseOption,
      child = null,
      timeZoneId = Some(SQLConf.get.sessionLocalTimeZone)
    )
    (json: String) => {
      val utf8json = UTF8String.fromString(json)
      parser.nullSafeEval(utf8json).asInstanceOf[InternalRow]
    }
  }

  /**
   * List the commits in the version range [startVersion, endVersion] (both inclusive) as
   * [[SingleCommit]]. Returns in sorted order, and throws if any in the range are missing.
   */
  def getCommitsInVersionRange(
      spark: SparkSession,
      deltaLog: DeltaLog,
      startVersion: Long,
      endVersion: Long,
      catalogTableOpt: Option[CatalogTable]): Seq[SingleCommit] = {
    // Pass `failOnDataLoss = false` as we do an explicit contiguity validation on the result below
    // to identify that there are no gaps.
    val commits = deltaLog
      .getChangesIterator(startVersion, endVersion, catalogTableOpt, failOnDataLoss = false)
      .toSeq
    // Verify that we got the entire range requested.
    if (commits.size.toLong != endVersion - startVersion + 1) {
      // [[unsafeVolatileSnapshot]] may be null, which needs to be explicitly filtered out.
      val snapshot = Some(deltaLog.unsafeVolatileSnapshot).filter(_ != null)
      recordDeltaEvent(
        provider = deltaLog,
        opType = "delta.exceptions.deltaVersionsNotContiguous",
        data = Map(
          // Remove the first element of the stack trace since this represents
          // the [[Thread.getStackTrace]] call itself.
          "stackTrace" -> Thread.currentThread().getStackTrace.tail.mkString("\n\t"),
          "startVersion" -> startVersion,
          "endVersion" -> endVersion,
          "unsafeVolatileSnapshot.latestCheckpointVersion" ->
            snapshot.map(_.checkpointProvider.version).getOrElse(-1L),
          "unsafeVolatileSnapshot.latestSnapshotVersion" ->
            snapshot.map(_.version).getOrElse(-1L),
          "unsafeVolatileSnapshot.checksumOpt" ->
            snapshot.map(_.checksumOpt).orNull
        ))
      throw DeltaErrors.deltaVersionsNotContiguousException(
        spark = spark,
        deltaVersions = commits.map(_.version),
        startVersion = startVersion,
        endVersion = endVersion,
        // Get the latest snapshot version for visibility when throwing the exception,
        // this is not exactly "the version to load snapshot" but
        // we just use the latest snapshot version here.
        versionToLoad = snapshot.map(_.version).getOrElse(-1L))
    }
    commits
  }

  /**
   * Parallel-read a set of commits off the driver, returning one lazy [[ClosableIterator]] of
   * [[Action]]s per commit, in the input order.
   */
  def parallelReadAndParseDeltaFilesAsIterator(
      spark: SparkSession,
      deltaLog: DeltaLog,
      commits: Seq[SingleCommit]): Seq[ClosableIterator[Action]] =
    parallelReadDeltaFiles(spark, deltaLog, commits)(_.getActionsIterator())

  /**
   * Parallel-read a set of commits off the driver, materializing each commit's [[Action]]s into a
   * [[Seq]], in the input order.
   */
  def parallelReadAndParseDeltaFilesAsSeq(
      spark: SparkSession,
      deltaLog: DeltaLog,
      commits: Seq[SingleCommit]): Seq[Seq[Action]] =
    parallelReadDeltaFiles(spark, deltaLog, commits) { commit =>
      commit.getActionsIterator().processAndClose(_.toSeq)
    }

  /**
   * Parallel-read a set of commits off the driver, applying `f` to each. The commits are read
   * concurrently on the driver delta-log thread pool; results are returned in the input order.
   */
  protected def parallelReadDeltaFiles[A](
      spark: SparkSession,
      deltaLog: DeltaLog,
      commits: Seq[SingleCommit])(f: SingleCommit => A): Seq[A] = {
    val hadoopConf = deltaLog.newDeltaHadoopConf()
    readThreadPool.parallelMap(spark, commits) { commit =>
      f(commit)
    }.toSeq
  }
}
