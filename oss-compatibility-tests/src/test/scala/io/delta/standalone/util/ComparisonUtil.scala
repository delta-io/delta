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


package io.delta.standalone.util

import scala.collection.JavaConverters._

trait ComparisonUtil {

  private def compareOptions[J, S](a: java.util.Optional[J], b: Option[S]): Unit = {
    assert(a.isPresent == b.isDefined)
    if (a.isPresent) {
      assert(a.get() == b.get)
    }
  }

  def compareMetadata(
      standalone: io.delta.standalone.actions.Metadata,
      oss: org.apache.spark.sql.delta.actions.Metadata): Unit = {

    assert(standalone.getId == oss.id)
    assert(standalone.getName == oss.name)
    assert(standalone.getDescription == oss.description)
    compareFormat(standalone.getFormat, oss.format)
    assert(standalone.getSchema.toJson == oss.schemaString)
    assert(standalone.getPartitionColumns.asScala == oss.partitionColumns)
    assert(standalone.getConfiguration.asScala == oss.configuration)
    compareOptions(standalone.getCreatedTime, oss.createdTime)
  }

  def compareFormat(
      standalone: io.delta.standalone.actions.Format,
      oss: org.apache.spark.sql.delta.actions.Format): Unit = {

    assert(standalone.getProvider == oss.provider)
    assert(standalone.getOptions.asScala == oss.options)
  }

  def compareCommitInfo(
      standalone: io.delta.standalone.actions.CommitInfo,
      oss: org.apache.spark.sql.delta.actions.CommitInfo): Unit = {

    // Do not compare `version`s. Standalone will inject the commitVersion using
    // DeltaHistoryManager. To get the OSS commitInfo, we are just reading using the store, so
    // the version is not injected.

    assert(standalone.getTimestamp == oss.timestamp)
    compareOptions(standalone.getUserId, oss.userId)
    compareOptions(standalone.getUserName, oss.userName)
    assert(standalone.getOperation == oss.operation)
    assert(standalone.getOperationParameters.asScala == oss.operationParameters)
    // TODO: job
    // TODO: notebook
    compareOptions(standalone.getClusterId, oss.clusterId)
    compareOptions(standalone.getReadVersion, oss.readVersion)
    compareOptions(standalone.getIsolationLevel, oss.isolationLevel)
  }

  def compareAddFiles(
      standaloneSnapshot: io.delta.standalone.Snapshot,
      ossSnapshot: org.apache.spark.sql.delta.Snapshot): Unit = {
    val standaloneAddFilesMap2 = standaloneSnapshot.getAllFiles.asScala
      .map { f => f.getPath -> f }.toMap
    val ossAddFilesMap2 = ossSnapshot.allFiles.collect().map { f => f.path -> f }.toMap

    assert(standaloneAddFilesMap2.size == ossAddFilesMap2.size)
    assert(standaloneAddFilesMap2.keySet == ossAddFilesMap2.keySet)

    standaloneAddFilesMap2.keySet.foreach { path =>
      compareAddFile(standaloneAddFilesMap2(path), ossAddFilesMap2(path))
    }
  }

  def compareAddFile(
      standalone: io.delta.standalone.actions.AddFile,
      oss: org.apache.spark.sql.delta.actions.AddFile): Unit = {
    // TODO
  }
}
