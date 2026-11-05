/*
 * Copyright (2025) The Delta Lake Project Authors.
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
package io.delta.spark.internal.v2.read

import java.lang.{StringBuilder => JStringBuilder}
import java.util.Optional

import io.delta.kernel.data.{MapValue => KernelMapValue, Row => KernelRow}
import io.delta.kernel.internal.actions.{AddCDCFile => KernelAddCDCFile, AddFile => KernelAddFile, RemoveFile => KernelRemoveFile}
import io.delta.kernel.internal.util.Preconditions

import org.apache.spark.sql.delta.RowIndexFilterType
import org.apache.spark.sql.delta.commands.cdc.CDCReader

import javax.annotation.Nullable

/**
 * Wrapper for all CDC file variants: inferred from AddFile (insert/update), inferred from
 * RemoveFile (delete), or explicit AddCDCFile actions.
 */
class CDCDataFile private (
    @Nullable kernelAddFileInput: KernelAddFile,
    @Nullable kernelRemoveFileInput: KernelRemoveFile,
    explicitCDCFileInput: Boolean,
    @Nullable cdcPathInput: String,
    @Nullable kernelCdcPartitionValuesInput: KernelMapValue,
    @Nullable changeTypeInput: String,
    commitTimestampInput: Long,
    fileSizeInput: Long,
    @Nullable dvBase64OverrideInput: String) {
  Preconditions.checkArgument(
    kernelAddFileInput != null || kernelRemoveFileInput != null || cdcPathInput != null,
    "CDCDataFile must have at least one of addFile, removeFile, or cdcPath")

  @Nullable private[this] val kernelAddFile = kernelAddFileInput
  @Nullable private[this] val kernelRemoveFile = kernelRemoveFileInput
  private[this] val explicitCDCFile = explicitCDCFileInput
  @Nullable private[this] val cdcPath = cdcPathInput
  @Nullable private[this] val kernelCdcPartitionValues = kernelCdcPartitionValuesInput
  @Nullable private[this] val changeType = changeTypeInput
  private[this] val commitTimestamp = commitTimestampInput
  private[this] val fileSize = fileSizeInput
  // base64-encoded inline DV of the changed rows (the bitmap diff between the Add and Remove DVs).
  @Nullable private[this] val dvBase64Override = dvBase64OverrideInput

  @Nullable
  def getAddFile(): KernelAddFile = kernelAddFile

  @Nullable
  def getRemoveFile(): KernelRemoveFile = kernelRemoveFile

  /** Returns the file path for any CDC file variant. */
  def getPath(): String = {
    if (kernelAddFile != null) kernelAddFile.getPath
    else if (kernelRemoveFile != null) kernelRemoveFile.getPath
    else cdcPath
  }

  /**
   * Returns the partition values for any CDC file variant. May be null for RemoveFile when
   * extendedFileMetadata is not present.
   */
  @Nullable
  def getPartitionValues(): KernelMapValue = {
    if (kernelAddFile != null) kernelAddFile.getPartitionValues
    else if (kernelRemoveFile != null) kernelRemoveFile.getPartitionValues.orElse(null)
    else kernelCdcPartitionValues
  }

  /**
   * Returns the DV bytes to use for filtering when reading this CDC file. For DV-diff entries, this
   * is the override bitmap (the diff between the Add and Remove DVs). For regular CDC inferred from
   * an AddFile/RemoveFile, this is the underlying file's DV if any. Empty if the file has no DV at
   * all.
   */
  def getEffectiveDv(): Optional[String] = {
    if (dvBase64Override != null) {
      Optional.of(dvBase64Override)
    } else if (kernelAddFile != null && kernelAddFile.getDeletionVector.isPresent) {
      Optional.of(kernelAddFile.getDeletionVector.get.serializeToBase64())
    } else if (kernelRemoveFile != null && kernelRemoveFile.getDeletionVector.isPresent) {
      Optional.of(kernelRemoveFile.getDeletionVector.get.serializeToBase64())
    } else {
      Optional.empty()
    }
  }

  /**
   * Returns the filter polarity to apply with [[getEffectiveDv]]. DV-diff files use
   * IF_NOT_CONTAINED (keep rows marked in the bitmap); all other files use IF_CONTAINED (skip rows
   * marked in the bitmap).
   */
  def getDvFilterType(): RowIndexFilterType = {
    if (dvBase64Override != null) RowIndexFilterType.IF_NOT_CONTAINED
    else RowIndexFilterType.IF_CONTAINED
  }

  @Nullable
  def getChangeType(): String = changeType

  def getCommitTimestamp(): Long = commitTimestamp

  def getFileSize(): Long = fileSize

  /** Returns true if this is an explicit CDC file (from AddCDCFile action). */
  def isAddCDCFile(): Boolean = explicitCDCFile

  def hasDeletionVector(): Boolean = {
    if (dvBase64Override != null) true
    else if (kernelAddFile != null) kernelAddFile.getDeletionVector.isPresent
    else if (kernelRemoveFile != null) kernelRemoveFile.getDeletionVector.isPresent
    else false
  }

  override def toString: String = {
    val sb = new JStringBuilder("CDCDataFile{")
    if (kernelAddFile != null) {
      sb.append("addFile=").append(kernelAddFile)
    } else if (kernelRemoveFile != null) {
      sb.append("removeFile=").append(kernelRemoveFile)
    } else {
      sb.append("explicit=true, path=").append(cdcPath)
    }
    if (changeType != null) {
      sb.append(", changeType='").append(changeType).append("'")
    }
    sb.append(", commitTimestamp=").append(commitTimestamp)
    if (dvBase64Override != null) {
      sb.append(", dvDiff=true")
    }
    sb.append("}")
    sb.toString
  }
}

object CDCDataFile {

  /** Create a CDCDataFile inferred from an AddFile action (always "insert"). */
  def fromAddFile(kernelAddFile: KernelAddFile, commitTimestamp: Long): CDCDataFile = {
    new CDCDataFile(
      kernelAddFile,
      null,
      false,
      null,
      null,
      CDCReader.CDC_TYPE_INSERT,
      commitTimestamp,
      kernelAddFile.getSize,
      null)
  }

  /** Create a CDCDataFile inferred from a RemoveFile action (always "delete"). */
  def fromRemoveFile(kernelRemoveFile: KernelRemoveFile, commitTimestamp: Long): CDCDataFile = {
    new CDCDataFile(
      null,
      kernelRemoveFile,
      false,
      null,
      null,
      CDCReader.CDC_TYPE_DELETE_STRING,
      commitTimestamp,
      kernelRemoveFile.getSize.orElse(0L),
      null)
  }

  /** Create a CDCDataFile for an explicit AddCDCFile action. */
  def fromAddCDCFile(kernelCdcRow: KernelRow, commitTimestamp: Long): CDCDataFile = {
    var kernelRow = kernelCdcRow
    val path = kernelRow.getString(KernelAddCDCFile.FULL_SCHEMA.indexOf("path"))
    val partitionValues = kernelRow.getMap(KernelAddCDCFile.FULL_SCHEMA.indexOf("partitionValues"))
    val size = kernelRow.getLong(KernelAddCDCFile.FULL_SCHEMA.indexOf("size"))
    new CDCDataFile(null, null, true, path, partitionValues, null, commitTimestamp, size, null)
  }

  /**
   * Create a CDCDataFile for a DV-diff same-path pair: an Add+Remove on the same parquet file where
   * the DV bitmap diff identifies the changed rows.
   */
  def fromDVDiff(
      kernelAddFile: KernelAddFile,
      changeType: String,
      commitTimestamp: Long,
      dvBase64: String): CDCDataFile = {
    new CDCDataFile(
      kernelAddFile,
      null,
      false,
      null,
      null,
      changeType,
      commitTimestamp,
      kernelAddFile.getSize,
      dvBase64)
  }
}
