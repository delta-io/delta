/*
 * Copyright (2020) The Delta Lake Project Authors.
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

package io.delta.standalone.internal.util

import java.lang.{String => StringJ}
import java.util.{Optional => OptionalJ}

import collection.JavaConverters._

import io.delta.standalone.actions.{AddFile => AddFileJ, CommitInfo => CommitInfoJ, Format => FormatJ, JobInfo => JobInfoJ, Metadata => MetadataJ, NotebookInfo => NotebookInfoJ}
import io.delta.standalone.internal.actions.{AddFile, CommitInfo, Format, JobInfo, Metadata, NotebookInfo}

/**
 * Provide helper methods to convert from Scala to Java types.
 */
private[internal] object ConversionUtils {

  private def toJavaLongOptional(opt: Option[Long]): OptionalJ[java.lang.Long] = opt match {
    case Some(v) => OptionalJ.ofNullable(v)
    case None => OptionalJ.empty()
  }

  private def toJavaBooleanOptional(
      opt: Option[Boolean]): OptionalJ[java.lang.Boolean] = opt match {
    case Some(v) => OptionalJ.ofNullable(v)
    case None => OptionalJ.empty()
  }

  private def toJavaStringOptional(opt: Option[String]): OptionalJ[StringJ] = opt match {
    case Some(v) => OptionalJ.ofNullable(v)
    case None => OptionalJ.empty()
  }

  private def toJavaMapOptional(
      opt: Option[Map[String, String]]): OptionalJ[java.util.Map[StringJ, StringJ]] = opt match {
    case Some(v) => OptionalJ.ofNullable(v.asJava)
    case None => OptionalJ.empty()
  }

  /**
   * Convert an [[AddFile]] (Scala) to an [[AddFileJ]] (Java)
   */
  def convertAddFile(internal: AddFile): AddFileJ = {
    new AddFileJ(
      internal.path,
      internal.partitionValues.asJava,
      internal.size,
      internal.modificationTime,
      internal.dataChange,
      internal.stats,
      internal.tags.asJava)
  }

  /**
   * Convert a [[Metadata]] (Scala) to a [[MetadataJ]] (Java)
   */
  def convertMetadata(internal: Metadata): MetadataJ = {
    new MetadataJ(
      internal.id,
      internal.name,
      internal.description,
      convertFormat(internal.format),
      internal.partitionColumns.toList.asJava,
      internal.configuration.asJava,
      toJavaLongOptional(internal.createdTime),
      internal.schema)
  }

  /**
   * Convert a [[Format]] (Scala) to a [[FormatJ]] (Java)
   */
  def convertFormat(internal: Format): FormatJ = {
    new FormatJ(internal.provider, internal.options.asJava)
  }

  /**
   * Convert a [[CommitInfo]] (Scala) to a [[CommitInfoJ]] (Java)
   */
  def convertCommitInfo(internal: CommitInfo): CommitInfoJ = {
    val notebookInfoOpt: OptionalJ[NotebookInfoJ] = if (internal.notebook.isDefined) {
      OptionalJ.of(convertNotebookInfo(internal.notebook.get))
    } else {
      OptionalJ.empty()
    }

    val jobInfoOpt: OptionalJ[JobInfoJ] = if (internal.job.isDefined) {
      OptionalJ.of(convertJobInfo(internal.job.get))
    } else {
      OptionalJ.empty()
    }

    new CommitInfoJ(
      toJavaLongOptional(internal.version),
      internal.timestamp,
      toJavaStringOptional(internal.userId),
      toJavaStringOptional(internal.userName),
      internal.operation,
      internal.operationParameters.asJava,
      jobInfoOpt,
      notebookInfoOpt,
      toJavaStringOptional(internal.clusterId),
      toJavaLongOptional(internal.readVersion),
      toJavaStringOptional(internal.isolationLevel),
      toJavaBooleanOptional(internal.isBlindAppend),
      toJavaMapOptional(internal.operationMetrics),
      toJavaStringOptional(internal.userMetadata)
    )
  }

  /**
   * Convert a [[JobInfo]] (Scala) to a [[JobInfoJ]] (Java)
   */
  def convertJobInfo(internal: JobInfo): JobInfoJ = {
    new JobInfoJ(
      internal.jobId,
      internal.jobName,
      internal.runId,
      internal.jobOwnerId,
      internal.triggerType)
  }

  /**
   * Convert a [[NotebookInfo]] (Scala) to a [[NotebookInfoJ]] (Java)
   */
  def convertNotebookInfo(internal: NotebookInfo): NotebookInfoJ = {
    new NotebookInfoJ(internal.notebookId)
  }
}
