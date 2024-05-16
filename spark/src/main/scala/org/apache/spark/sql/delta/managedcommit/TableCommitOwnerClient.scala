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

package org.apache.spark.sql.delta.managedcommit

import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.storage.LogStore
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

/**
 * A wrapper around [[CommitOwnerClient]] that provides a more user-friendly API for committing
 * / accessing commits to a specific table. This class takes care of passing the table specific
 * configuration to the underlying [[CommitOwnerClient]] e.g. logPath / logStore
 * / managedCommitTableConf / hadoopConf.
 *
 * @param commitOwnerClient the underlying [[CommitOwnerClient]]
 * @param logPath the path to the log directory
 * @param tableConf the table specific managed-commit configuration
 * @param hadoopConf hadoop configuration
 * @param logStore the log store
 */
case class TableCommitOwnerClient(
    commitOwnerClient: CommitOwnerClient,
    logPath: Path,
    tableConf: Map[String, String],
    hadoopConf: Configuration,
    logStore: LogStore) {

  def commit(
      commitVersion: Long,
      actions: Iterator[String],
      updatedActions: UpdatedActions): CommitResponse = {
    commitOwnerClient.commit(
      logStore, hadoopConf, logPath, tableConf, commitVersion, actions, updatedActions)
  }

  def getCommits(
      startVersion: Option[Long] = None,
      endVersion: Option[Long] = None): GetCommitsResponse = {
    commitOwnerClient.getCommits(logPath, tableConf, startVersion, endVersion)
  }

  def backfillToVersion(
      version: Long,
      lastKnownBackfilledVersion: Option[Long] = None): Unit = {
    commitOwnerClient.backfillToVersion(
      logStore, hadoopConf, logPath, tableConf, version, lastKnownBackfilledVersion)
  }

  /**
   * Checks whether the signature of the underlying backing [[CommitOwnerClient]] is the same as the
   * given `otherCommitOwnerClient`
   */
  def semanticsEquals(otherCommitOwnerClient: CommitOwnerClient): Boolean = {
    CommitOwnerClient.semanticEquals(Some(commitOwnerClient), Some(otherCommitOwnerClient))
  }

  /**
   * Checks whether the signature of the underlying backing [[CommitOwnerClient]] is the same as the
   * given `otherCommitOwnerClient`
   */
  def semanticsEquals(otherCommitOwnerClient: TableCommitOwnerClient): Boolean = {
    semanticsEquals(otherCommitOwnerClient.commitOwnerClient)
  }
}

object TableCommitOwnerClient {
    def apply(
      commitOwnerClient: CommitOwnerClient,
      deltaLog: DeltaLog,
      managedCommitTableConf: Map[String, String]): TableCommitOwnerClient = {
    val hadoopConf = deltaLog.newDeltaHadoopConf()
    new TableCommitOwnerClient(
      commitOwnerClient, deltaLog.logPath, managedCommitTableConf, hadoopConf, deltaLog.store)
  }
}
