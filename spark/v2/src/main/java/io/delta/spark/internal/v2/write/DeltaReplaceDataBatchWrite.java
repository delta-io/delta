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
package io.delta.spark.internal.v2.write;

import io.delta.kernel.Snapshot;
import io.delta.kernel.TransactionCommitResult;
import io.delta.kernel.data.Row;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.expressions.Literal;
import io.delta.kernel.internal.actions.GenerateIcebergCompatActionUtils;
import io.delta.kernel.internal.actions.SingleAction;
import io.delta.kernel.internal.util.Utils;
import io.delta.kernel.utils.CloseableIterable;
import io.delta.spark.internal.v2.read.DeltaScanFile;
import io.delta.spark.internal.v2.utils.PartitionUtils;
import io.delta.spark.internal.v2.utils.SerializableKernelRowWrapper;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.DataWriterFactory;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.PhysicalWriteInfo;
import org.apache.spark.sql.connector.write.Write;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * BatchWrite for DSv2 ReplaceData commits.
 *
 * <p>On the driver this class uses {@link DeltaV2BatchWriteContext} for shared write setup, then
 * adds ReplaceData-specific selected-file validation and RemoveFile action generation.
 *
 * <p>At commit time the authoritative remove-set is derived from the selected files reported by the
 * scan — not from files the writer happens to have observed. This matters because a DELETE that
 * fully empties a source file produces zero output rows, so the writer never reports the source
 * path; only the scan knows which files were read. AddFile actions written by executors are paired
 * with RemoveFile actions generated from the scan-selected files and committed together.
 *
 * <p>Current scope is unpartitioned tables and single-partition writes; multi-partition replace-
 * data is rejected by {@link #resolveWritePartitionValueStrings()} until follow-up work lands.
 */
class DeltaReplaceDataBatchWrite implements Write, BatchWrite {

  private static final Logger LOG = LoggerFactory.getLogger(DeltaReplaceDataBatchWrite.class);

  private final DeltaV2BatchWriteContext context;
  private final String tablePath;
  private final Supplier<List<DeltaScanFile>> selectedFilesProvider;

  DeltaReplaceDataBatchWrite(
      Engine engine,
      Configuration hadoopConf,
      String tablePath,
      Snapshot initialSnapshot,
      LogicalWriteInfo writeInfo,
      Supplier<List<DeltaScanFile>> selectedFilesProvider) {
    this.context =
        DeltaV2BatchWriteContext.create(engine, hadoopConf, tablePath, initialSnapshot, writeInfo);
    this.tablePath = Objects.requireNonNull(tablePath, "tablePath is null");
    this.selectedFilesProvider =
        Objects.requireNonNull(selectedFilesProvider, "selectedFilesProvider is null");
  }

  @Override
  public BatchWrite toBatch() {
    return this;
  }

  @Override
  public DataWriterFactory createBatchWriterFactory(PhysicalWriteInfo physicalWriteInfo) {
    List<DeltaScanFile> selectedFiles = getSelectedFiles();
    Map<String, String> partitionValueStrings = resolveWritePartitionValueStrings(selectedFiles);
    Map<String, Literal> partitionLiterals =
        PartitionUtils.buildKernelPartitionLiteralMap(
            partitionValueStrings, context.getPartitionSchema(), context.getSessionTimeZone());
    String targetDirectory = context.getTargetDirectory(partitionLiterals);
    return new DeltaReplaceDataWriterFactory(
        targetDirectory,
        context.getSerializableHadoopConf(),
        context.getSerializedTxnState(),
        context.getDataSchema(),
        context.getOutputWriterFactory(),
        context.getPartitionSchema(),
        partitionValueStrings,
        context.getSessionTimeZoneId());
  }

  @Override
  public void commit(WriterCommitMessage[] messages) {
    List<DeltaScanFile> selectedFiles = getSelectedFiles();
    validateWriterSourcePaths(messages, selectedFiles);

    List<Row> allActionRows = new ArrayList<>();
    for (WriterCommitMessage message : messages) {
      if (message instanceof DeltaReplaceDataCommitMessage) {
        DeltaReplaceDataCommitMessage replaceDataMessage = (DeltaReplaceDataCommitMessage) message;
        for (SerializableKernelRowWrapper wrapper : replaceDataMessage.getAddFileActionRows()) {
          allActionRows.add(wrapper.getRow());
        }
      }
    }

    long removeTimestamp = System.currentTimeMillis();
    for (DeltaScanFile selectedFile : selectedFiles) {
      allActionRows.add(createRemoveFileActionRow(selectedFile, removeTimestamp));
    }

    if (allActionRows.isEmpty()) {
      LOG.info("DSv2 replace-data found no selected files and no replacement files; no commit");
      return;
    }

    CloseableIterable<Row> dataActions =
        CloseableIterable.inMemoryIterable(Utils.toCloseableIterator(allActionRows.iterator()));

    TransactionCommitResult result =
        context.getTransaction().commit(context.getEngine(), dataActions);
    LOG.info(
        "DSv2 replace-data committed at version {} after rewriting {} source files",
        result.getVersion(),
        selectedFiles.size());
  }

  @Override
  public void abort(WriterCommitMessage[] messages) {
    LOG.warn(
        "DSv2 replace-data write aborted. {} task messages will not be committed. "
            + "Orphaned data files will be cleaned up by VACUUM.",
        messages != null ? messages.length : 0);
  }

  private Row createRemoveFileActionRow(DeltaScanFile selectedFile, long removeTimestamp) {
    // Master replaced AddFile.toRemoveFileRow with this dropin helper; v2 callers pass
    // Optional.empty() for stats on the read path per its javadoc note.
    Row removeFileRow =
        GenerateIcebergCompatActionUtils.createRemoveFileRowWithExtendedFileMetadata(
            selectedFile.getPath(),
            removeTimestamp,
            /* dataChange */ true,
            selectedFile.getPartitionValues(),
            selectedFile.getSize(),
            Optional.empty(),
            context.getKernelTableSchema(),
            selectedFile.getBaseRowId(),
            selectedFile.getDefaultRowCommitVersion(),
            selectedFile.getDeletionVector());
    return SingleAction.createRemoveFileSingleAction(removeFileRow);
  }

  private List<DeltaScanFile> getSelectedFiles() {
    List<DeltaScanFile> selectedFiles = selectedFilesProvider.get();
    return selectedFiles == null ? Collections.emptyList() : List.copyOf(selectedFiles);
  }

  private void validateWriterSourcePaths(
      WriterCommitMessage[] messages, List<DeltaScanFile> selectedFiles) {
    Set<String> selectedSourcePaths = new java.util.HashSet<>();
    for (DeltaScanFile selectedFile : selectedFiles) {
      selectedSourcePaths.add(selectedFile.getPath());
      Path absolutePath = new Path(tablePath, selectedFile.getPath());
      selectedSourcePaths.add(absolutePath.toString());
      selectedSourcePaths.add(absolutePath.toUri().toString());
    }

    for (WriterCommitMessage message : messages) {
      if (message instanceof DeltaReplaceDataCommitMessage) {
        DeltaReplaceDataCommitMessage replaceDataMessage = (DeltaReplaceDataCommitMessage) message;
        for (String sourceFilePath : replaceDataMessage.getSourceFilePaths()) {
          if (!selectedSourcePaths.contains(sourceFilePath)) {
            throw new IllegalStateException(
                "Delta ReplaceData writer reported source file outside scan-selected files: "
                    + sourceFilePath);
          }
        }
      }
    }
  }

  private Map<String, String> resolveWritePartitionValueStrings(List<DeltaScanFile> selectedFiles) {
    if (context.getPartitionSchema().fields().length == 0) {
      return Collections.emptyMap();
    }

    if (selectedFiles.isEmpty()) {
      return Collections.emptyMap();
    }

    Map<String, String> resolvedPartitionValues = null;
    for (DeltaScanFile selectedFile : selectedFiles) {
      Map<String, String> currentPartitionValues =
          PartitionUtils.getLogicalPartitionValueStrings(
              selectedFile.getPartitionValues(), context.getPartitionSchema());
      if (resolvedPartitionValues == null) {
        resolvedPartitionValues = new LinkedHashMap<>(currentPartitionValues);
      } else if (!resolvedPartitionValues.equals(currentPartitionValues)) {
        throw new IllegalStateException(
            "Delta ReplaceData currently requires scan-selected files from a single partition "
                + "to determine the write context");
      }
    }
    return resolvedPartitionValues;
  }
}
