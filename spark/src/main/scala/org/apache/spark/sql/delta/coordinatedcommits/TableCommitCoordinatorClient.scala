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

package org.apache.spark.sql.delta.coordinatedcommits

import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.storage.LogStore
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

/**
 * A wrapper around [[CommitCoordinatorClient]] that provides a more user-friendly API for
 * committing/ accessing commits to a specific table. This class takes care of passing the
 * table specific configuration to the underlying [[CommitCoordinatorClient]] e.g. logPath /
 * logStore / coordinatedCommitsTableConf / hadoopConf.
 *
 * @param commitCoordinatorClient the underlying [[CommitCoordinatorClient]]
 * @param logPath the path to the log directory
 * @param tableConf the table specific coordinated-commits configuration
 * @param hadoopConf hadoop configuration
 * @param logStore the log store
 */
case class TableCommitCoordinatorClient(
    commitCoordinatorClient: CommitCoordinatorClient,
    logPath: Path,
    tableConf: Map[String, String],
    hadoopConf: Configuration,
    logStore: LogStore) {

  def commit(
      commitVersion: Long,
      actions: Iterator[String],
      updatedActions: UpdatedActions): CommitResponse = {
    commitCoordinatorClient.commit(
      logStore, hadoopConf, logPath, tableConf, commitVersion, actions, updatedActions)
  }

  def getCommits(
      startVersion: Option[Long] = None,
      endVersion: Option[Long] = None): GetCommitsResponse = {
    commitCoordinatorClient.getCommits(logPath, tableConf, startVersion, endVersion)
  }

  def backfillToVersion(
      version: Long,
      lastKnownBackfilledVersion: Option[Long] = None): Unit = {
    commitCoordinatorClient.backfillToVersion(
      logStore, hadoopConf, logPath, tableConf, version, lastKnownBackfilledVersion)
  }

  /**
   * Checks whether the signature of the underlying backing [[CommitCoordinatorClient]] is the same
   * as the given `otherCommitCoordinatorClient`
   */
  def semanticsEquals(otherCommitCoordinatorClient: CommitCoordinatorClient): Boolean = {
    CommitCoordinatorClient.semanticEquals(
      Some(commitCoordinatorClient), Some(otherCommitCoordinatorClient))
  }

  /**
   * Checks whether the signature of the underlying backing [[CommitCoordinatorClient]] is the same
   * as the given `otherCommitCoordinatorClient`
   */
  def semanticsEquals(otherCommitCoordinatorClient: TableCommitCoordinatorClient): Boolean = {
    semanticsEquals(otherCommitCoordinatorClient.commitCoordinatorClient)
  }
}

object TableCommitCoordinatorClient {
    def apply(
      commitCoordinatorClient: CommitCoordinatorClient,
      deltaLog: DeltaLog,
      coordinatedCommitsTableConf: Map[String, String]): TableCommitCoordinatorClient = {
    val hadoopConf = deltaLog.newDeltaHadoopConf()
    new TableCommitCoordinatorClient(
      commitCoordinatorClient,
      deltaLog.logPath,
      coordinatedCommitsTableConf,
      hadoopConf,
      deltaLog.store)
  }
}
