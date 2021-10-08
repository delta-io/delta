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

import scala.collection.JavaConverters._

import io.delta.standalone.actions.{Action => ActionJ, AddFile => AddFileJ, AddCDCFile => AddCDCFileJ, CommitInfo => CommitInfoJ, Format => FormatJ, JobInfo => JobInfoJ, Metadata => MetadataJ, NotebookInfo => NotebookInfoJ, Protocol => ProtocolJ, RemoveFile => RemoveFileJ, SetTransaction => SetTransactionJ}
import io.delta.standalone.internal.actions.{Action, AddCDCFile, AddFile, CommitInfo, Format, JobInfo, Metadata, NotebookInfo, Protocol, RemoveFile, SetTransaction}

/**
 * Provide helper methods to convert from Scala to Java types and vice versa.
 */
private[internal] object ConversionUtils {

  ///////////////////////////////////////////////////////////////////////////
  // Scala to Java conversions
  ///////////////////////////////////////////////////////////////////////////

  /**
   * This is a workaround for a known issue in Scala 2.11: `asJava` doesn't handle `null`.
   * See https://github.com/scala/scala/pull/4343
   */
  private def nullableMapAsJava[K, V](map: Map[K, V]): java.util.Map[K, V] = {
    if (map == null) {
      null
    } else {
      map.asJava
    }
  }

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
      nullableMapAsJava(internal.tags))
  }

  def convertAddCDCFile(internal: AddCDCFile): AddCDCFileJ = {
    new AddCDCFileJ(
      internal.path,
      internal.partitionValues.asJava,
      internal.size,
      nullableMapAsJava(internal.tags))
  }

  def convertRemoveFile(internal: RemoveFile): RemoveFileJ = {
    new RemoveFileJ(
      internal.path,
      toJavaLongOptional(internal.deletionTimestamp),
      internal.dataChange,
      internal.extendedFileMetadata,
      nullableMapAsJava(internal.partitionValues),
      internal.size,
      nullableMapAsJava(internal.tags))
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
      nullableMapAsJava(internal.operationParameters),
      jobInfoOpt,
      notebookInfoOpt,
      toJavaStringOptional(internal.clusterId),
      toJavaLongOptional(internal.readVersion),
      toJavaStringOptional(internal.isolationLevel),
      toJavaBooleanOptional(internal.isBlindAppend),
      toJavaMapOptional(internal.operationMetrics),
      toJavaStringOptional(internal.userMetadata),
      toJavaStringOptional(internal.engineInfo)
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

  def convertSetTransaction(internal: SetTransaction): SetTransactionJ = {
    new SetTransactionJ(internal.appId, internal.version, toJavaLongOptional(internal.lastUpdated))
  }

  def convertProtocol(internal: Protocol): ProtocolJ = {
    new ProtocolJ(internal.minReaderVersion, internal.minWriterVersion)
  }

  def convertAction(internal: Action): ActionJ = internal match {
    case x: AddFile => convertAddFile(x)
    case x: AddCDCFile => convertAddCDCFile(x)
    case x: RemoveFile => convertRemoveFile(x)
    case x: CommitInfo => convertCommitInfo(x)
    case x: Format => convertFormat(x)
    case x: JobInfo => convertJobInfo(x)
    case x: Metadata => convertMetadata(x)
    case x: NotebookInfo => convertNotebookInfo(x)
    case x: SetTransaction => convertSetTransaction(x)
    case x: Protocol => convertProtocol(x)
  }

  ///////////////////////////////////////////////////////////////////////////
  // Java to Scala conversions
  ///////////////////////////////////////////////////////////////////////////

//  private implicit def toScalaLongOption(opt: OptionalJ[java.lang.Long]): Option[Long] =
//    if (opt.isPresent) Some(opt.get()) else None
//
//  private implicit def toScalaStringOption(opt: OptionalJ[StringJ]): Option[String] =
//    if (opt.isPresent) Some(opt.get()) else None

  // TODO verify this actually works
  private implicit def toScalaOption[J, S](opt: OptionalJ[J]): Option[S] =
    if (opt.isPresent) Some(opt.get().asInstanceOf[S]) else None

  def convertActionJ(external: ActionJ): Action = external match {
    case x: AddFileJ => convertAddFileJ(x)
    case x: RemoveFileJ => convertRemoveFileJ(x)
    case x: CommitInfoJ => convertCommitInfoJ(x)
    case x: MetadataJ => convertMetadataJ(x)
    case x: ProtocolJ => convertProtocolJ(x)
    case x: SetTransactionJ => convertSetTransactionJ(x)
    // TODO others
    case _ => throw new UnsupportedOperationException("cannot convert this Java Action")
  }

  def convertAddFileJ(external: AddFileJ): AddFile = {
    AddFile(
      external.getPath,
      external.getPartitionValues.asScala.toMap,
      external.getSize,
      external.getModificationTime,
      external.isDataChange,
      external.getStats,
      if (external.getTags != null) external.getTags.asScala.toMap else null
    )
  }

  def convertRemoveFileJ(external: RemoveFileJ): RemoveFile = {
    RemoveFile(
      external.getPath,
      external.getDeletionTimestamp, // implicit check this!
      external.isDataChange,
      external.isExtendedFileMetadata,
      if (external.isExtendedFileMetadata && external.getPartitionValues != null) {
        external.getPartitionValues.asScala.toMap
      } else null,
      if (external.isExtendedFileMetadata) external.getSize else 0,
      if (external.isExtendedFileMetadata && external.getTags != null) {
        external.getTags.asScala.toMap
      } else null
    )
  }

  def convertCommitInfoJ(external: CommitInfoJ): CommitInfo = {
    CommitInfo(
      external.getVersion, // implicit check this!
      external.getTimestamp,
      external.getUserId, // implicit check this!
      external.getUserName, // implicit check this!
      external.getOperation,
      if (external.getOperationParameters != null) {
        external.getOperationParameters.asScala.toMap
      } else null,
      None, // TODO: Option[JobInfo]
      None, // TODO: Option[NotebookInfo]
      external.getClusterId, // implicit check this!
      external.getReadVersion, // implicit check this!
      external.getIsolationLevel, // implicit check this!
      external.getIsBlindAppend, // implicit check this!
      external.getOperationMetrics, // implicit check this!
      external.getUserMetadata, // implicit check this!
      external.getEngineInfo // implicit check this!
    )
  }

  def convertMetadataJ(external: MetadataJ): Metadata = {
    Metadata(
      external.getId,
      external.getName,
      external.getDescription,
      convertFormatJ(external.getFormat),
      if (external.getSchema == null) null else external.getSchema.toJson,
      external.getPartitionColumns.asScala,
      external.getConfiguration.asScala.toMap,
      external.getCreatedTime // implicit check this!
    )
  }

  def convertProtocolJ(external: ProtocolJ): Protocol = {
    Protocol(
      external.getMinReaderVersion,
      external.getMinWriterVersion
    )
  }

  def convertFormatJ(external: FormatJ): Format = {
    Format(
      external.getProvider,
      external.getOptions.asScala.toMap
    )
  }

  def convertSetTransactionJ(external: SetTransactionJ): SetTransaction = {
    SetTransaction(
      external.getAppId,
      external.getVerion,
      external.getLastUpdated // implicit check this!
    )
  }

}
