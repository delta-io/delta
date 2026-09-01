/*
 * Copyright (2026) The Delta Lake Project Authors.
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

package org.apache.spark.sql.delta.v2.kernel

import java.sql.Timestamp

import scala.collection.JavaConverters._
import scala.jdk.OptionConverters._

import org.apache.spark.sql.delta.actions.{
  Action,
  AddFile,
  CommitInfo,
  DeletionVectorDescriptor,
  Format,
  Metadata,
  Protocol,
  RemoveFile
}
import io.delta.kernel.{CommitActions => KernelCommitActions}
import io.delta.kernel.data.{ColumnarBatch => KernelColumnarBatch}
import io.delta.kernel.data.{ColumnVector => KernelColumnVector}
import io.delta.kernel.data.{MapValue => KernelMapValue}
import io.delta.kernel.internal.DeltaLogActionUtils.{DeltaAction => KernelDeltaAction}
import io.delta.kernel.internal.actions.{AddFile => KernelAddFile}
import io.delta.kernel.internal.actions.{CommitInfo => KernelCommitInfo}
import io.delta.kernel.internal.actions.{
  DeletionVectorDescriptor => KernelDeletionVectorDescriptor
}
import io.delta.kernel.internal.actions.{Metadata => KernelMetadata}
import io.delta.kernel.internal.actions.{Protocol => KernelProtocol}
import io.delta.kernel.internal.actions.{RemoveFile => KernelRemoveFile}
import io.delta.kernel.internal.data.{StructRow => KernelStructRow}
import io.delta.kernel.internal.util.{VectorUtils => KernelVectorUtils}

/**
 * Bridges Kernel's actions to V1 Delta actions.
 */
private[v2] object KernelActionUtils {

  /**
   * Reads a Kernel commit actions into V1 [[Action]]s.
   */
  private[v2] def readActions(commitActions: KernelCommitActions): Seq[Action] = {
    val kernelActionsBatchIter = commitActions.getActions()
    try {
      val actions = Seq.newBuilder[Action]
      while (kernelActionsBatchIter.hasNext) {
        actions ++= readActionBatch(kernelActionsBatchIter.next())
      }
      actions.result()
    } finally {
      kernelActionsBatchIter.close()
    }
  }

  private def readActionBatch(
      kernelActionsBatch: KernelColumnarBatch
  ): Seq[Action] = {
    val schema = kernelActionsBatch.getSchema
    // (action type -> its column vector), for the action columns actually present in this batch.
    val actionToColumnVector = KernelDeltaAction.values().flatMap { action =>
      val ordinal = schema.indexOf(action.colName)
      if (ordinal >= 0) Some(action -> kernelActionsBatch.getColumnVector(ordinal)) else None
    }
    // A Delta log row carries exactly one action, so at most one action column is non-null per row;
    // `collectFirst` decodes that single column into an Action.
    (0 until kernelActionsBatch.getSize).flatMap { rowId =>
      actionToColumnVector.collectFirst {
        case (action, columnVector) if !columnVector.isNullAt(rowId) =>
          actionFromKernel(action, columnVector, rowId)
      }
    }
  }

  private def actionFromKernel(
      action: KernelDeltaAction,
      columnVector: KernelColumnVector,
      rowId: Int
  ): Action = action match {
    case KernelDeltaAction.ADD =>
      addFileFromKernel(
        new KernelAddFile(KernelStructRow.fromStructVector(columnVector, rowId))
      )
    case KernelDeltaAction.REMOVE =>
      // scalastyle:off removeFile // KernelRemoveFile is the Kernel wrapper, not a V1 RemoveFile
      removeFileFromKernel(
        new KernelRemoveFile(KernelStructRow.fromStructVector(columnVector, rowId)))
      // scalastyle:on removeFile
    case KernelDeltaAction.METADATA =>
      metadataFromKernel(
        KernelMetadata.fromColumnVector(columnVector, rowId))
    case KernelDeltaAction.PROTOCOL =>
      protocolFromKernel(
        KernelProtocol.fromColumnVector(columnVector, rowId))
    case KernelDeltaAction.COMMITINFO =>
      commitInfoFromKernel(
        KernelCommitInfo.fromColumnVector(columnVector, rowId))
    case other =>
      throw new UnsupportedOperationException(
        s"No V1 action from Kernel decoder for a '${other.colName}' action yet")
  }

  /**
   * Converts a Kernel [[KernelAddFile]] into a V1 [[AddFile]].
   */
  def addFileFromKernel(addFile: KernelAddFile): AddFile = {
    AddFile(
      path = addFile.getPath,
      partitionValues = partitionValuesFromKernel(addFile.getPartitionValues),
      size = addFile.getSize,
      modificationTime = addFile.getModificationTime,
      dataChange = addFile.getDataChange,
      tags = tagsFromKernel(addFile.getTags),
      deletionVector = deletionVectorFromKernel(addFile.getDeletionVector),
      baseRowId = addFile.getBaseRowId.toScala.map(_.longValue()),
      defaultRowCommitVersion = addFile.getDefaultRowCommitVersion.toScala.map(_.longValue()))
  }

  /**
   * Converts a Kernel [[KernelRemoveFile]] into a V1 [[RemoveFile]].
   */
  def removeFileFromKernel(removeFile: KernelRemoveFile): RemoveFile = {
    // scalastyle:off removeFile
    RemoveFile(
      path = removeFile.getPath,
      deletionTimestamp = removeFile.getDeletionTimestamp.toScala.map(_.longValue()),
      dataChange = removeFile.getDataChange,
      extendedFileMetadata = removeFile.getExtendedFileMetadata.toScala.map(_.booleanValue()),
      partitionValues = removeFile.getPartitionValues.toScala.map(partitionValuesFromKernel).orNull,
      size = removeFile.getSize.toScala.map(_.longValue()),
      tags = tagsFromKernel(removeFile.getTags),
      deletionVector = deletionVectorFromKernel(removeFile.getDeletionVector),
      baseRowId = removeFile.getBaseRowId.toScala.map(_.longValue()),
      defaultRowCommitVersion = removeFile.getDefaultRowCommitVersion.toScala.map(_.longValue()),
      stats = removeFile.getStatsJson.toScala.orNull)
    // scalastyle:on removeFile
  }

  /**
   * Converts a Kernel [[KernelMetadata]] into a V1 [[Metadata]].
   */
  def metadataFromKernel(metadata: KernelMetadata): Metadata = {
    Metadata(
      id = metadata.getId,
      name = metadata.getName.orElse(null),
      description = metadata.getDescription.orElse(null),
      format = Format(
        provider = metadata.getFormat.getProvider,
        options = metadata.getFormat.getOptions.asScala.toMap),
      schemaString = metadata.getSchemaString,
      // getPartitionColumns preserves the declared order and original case of partition
      // columns; getPartitionColNames would return an unordered set of lowercased names.
      partitionColumns =
        KernelVectorUtils.toJavaList[String](metadata.getPartitionColumns).asScala.toSeq,
      configuration = metadata.getConfiguration.asScala.toMap,
      createdTime =
        if (metadata.getCreatedTime.isPresent) Some(metadata.getCreatedTime.get.longValue())
        else None)
  }

  /**
   * Converts a Kernel [[KernelProtocol]] into a V1 [[Protocol]].
   */
  def protocolFromKernel(protocol: KernelProtocol): Protocol = {
    val readerFeatures =
      Option(protocol.getReaderFeatures).map(_.asScala.toSet).getOrElse(Set.empty)
    val writerFeatures =
      Option(protocol.getWriterFeatures).map(_.asScala.toSet).getOrElse(Set.empty)

    Protocol(protocol.getMinReaderVersion, protocol.getMinWriterVersion)
      .withWriterFeatures(writerFeatures)
      .withReaderFeatures(readerFeatures)
  }

  /**
   * Converts a Kernel [[KernelCommitInfo]] into a V1 [[CommitInfo]].
   */
  def commitInfoFromKernel(commitInfo: KernelCommitInfo): CommitInfo = {
    CommitInfo(
      version = None,
      inCommitTimestamp = commitInfo.getInCommitTimestamp.toScala.map(_.longValue()),
      timestamp = new Timestamp(commitInfo.getTimestamp),
      userId = None,
      userName = None,
      operation = commitInfo.getOperation.orElse(null),
      operationParameters =
        Option(commitInfo.getOperationParameters).map(_.asScala.toMap).getOrElse(Map.empty),
      job = None,
      notebook = None,
      clusterId = None,
      readVersion = None,
      isolationLevel = None,
      isBlindAppend = commitInfo.getIsBlindAppend.toScala.map(_.booleanValue()),
      dataChange = None,
      operationMetrics = Option(commitInfo.getOperationMetrics).map(_.asScala.toMap),
      userMetadata = None,
      tags = None,
      engineInfo = commitInfo.getEngineInfo.toScala,
      txnId = commitInfo.getTxnId.toScala,
      lastManifestCommit = None)
  }

  private def deletionVectorFromKernel(
      deletionVector: java.util.Optional[KernelDeletionVectorDescriptor])
      : DeletionVectorDescriptor =
    deletionVector.toScala.map { dv =>
      DeletionVectorDescriptor(
        storageType = dv.getStorageType,
        pathOrInlineDv = dv.getPathOrInlineDv,
        offset = dv.getOffset.toScala.map(_.intValue()),
        sizeInBytes = dv.getSizeInBytes,
        cardinality = dv.getCardinality)
    }.orNull

  private def partitionValuesFromKernel(partitionValues: KernelMapValue): Map[String, String] = {
    if (partitionValues == null) return Map.empty
    val keys = partitionValues.getKeys
    val values = partitionValues.getValues
    (0 until partitionValues.getSize).map { index =>
      val value = if (values.isNullAt(index)) null else values.getString(index)
      keys.getString(index) -> value
    }.toMap
  }

  private def tagsFromKernel(tags: java.util.Optional[KernelMapValue]): Map[String, String] =
    tags.toScala.map(partitionValuesFromKernel).orNull
}
