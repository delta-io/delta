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
package io.delta.kernel.internal.actions;

import static io.delta.kernel.internal.util.InternalUtils.relativizePath;
import static io.delta.kernel.internal.util.PartitionUtils.serializePartitionMap;
import static io.delta.kernel.internal.util.Preconditions.checkArgument;
import static io.delta.kernel.internal.util.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

import io.delta.kernel.Transaction;
import io.delta.kernel.data.MapValue;
import io.delta.kernel.data.Row;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.exceptions.KernelException;
import io.delta.kernel.expressions.Literal;
import io.delta.kernel.internal.DeltaErrors;
import io.delta.kernel.internal.TableConfig;
import io.delta.kernel.internal.annotation.VisibleForTesting;
import io.delta.kernel.internal.data.GenericRow;
import io.delta.kernel.internal.data.TransactionStateRow;
import io.delta.kernel.internal.fs.Path;
import io.delta.kernel.internal.icebergcompat.IcebergCompatV2MetadataValidatorAndUpdater;
import io.delta.kernel.internal.icebergcompat.IcebergCompatV3MetadataValidatorAndUpdater;
import io.delta.kernel.statistics.DataFileStatistics;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterable;
import io.delta.kernel.utils.DataFileStatus;
import java.net.URI;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/** Utilities to convert Iceberg add/removes to Delta Kernel add/removes */
public final class GenerateIcebergCompatActionUtils {

  /**
   * Create an add action {@link Row} that can be passed to {@link Transaction#commit(Engine,
   * CloseableIterable)} from an Iceberg add.
   *
   * @param transactionState the transaction state from the built transaction
   * @param fileStatus the file status to create the add with (contains path, time, size, and stats)
   * @param partitionValues the partition values for the add
   * @param dataChange whether or not the add constitutes a dataChange (i.e. append vs. compaction)
   * @param tags key-value metadata to be attached to the add action
   * @param physicalSchemaOpt An optional pre-parsed physical schema. Improves performance for batch
   *     operations by avoiding repeated JSON parsing. Recommended when generating many actions with
   *     the same schema.
   * @return add action row that can be included in the transaction
   * @throws UnsupportedOperationException if icebergWriterCompatV1 is not enabled
   * @throws UnsupportedOperationException if maxRetries != 0 in the transaction
   * @throws KernelException if stats are not present (required for icebergCompatV2)
   * @throws UnsupportedOperationException if the table is partitioned (currently unsupported)
   */
  public static Row generateIcebergCompatWriterV1AddAction(
      Row transactionState,
      DataFileStatus fileStatus,
      Map<String, Literal> partitionValues,
      boolean dataChange,
      Map<String, String> tags,
      Optional<StructType> physicalSchemaOpt) {
    Map<String, String> configuration = TransactionStateRow.getConfiguration(transactionState);

    /* ----- Validate that this is a valid usage of this API ----- */
    validateIcebergWriterCompatV1Enabled(configuration);
    validateMaxRetriesSetToZero(transactionState);

    /* ----- Validate this is valid write given the table's protocol & configurations ----- */
    checkState(
        TableConfig.ICEBERG_COMPAT_V2_ENABLED.fromMetadata(configuration),
        "icebergCompatV2 not enabled despite icebergWriterCompatV1 enabled");
    // We require field `numRecords` when icebergCompatV2 is enabled
    IcebergCompatV2MetadataValidatorAndUpdater.validateDataFileStatus(fileStatus);

    /* --- Validate and update partitionValues ---- */
    // Currently we don't support partitioned tables; fail here
    blockPartitionedTables(transactionState, partitionValues);

    URI tableRoot = new Path(TransactionStateRow.getTablePath(transactionState)).toUri();
    // This takes care of relativizing the file path and serializing the file statistics
    AddFile addFile =
        AddFile.convertDataFileStatus(
            physicalSchemaOpt.orElseGet(
                () -> TransactionStateRow.getPhysicalSchema(transactionState)),
            tableRoot,
            fileStatus,
            partitionValues,
            dataChange,
            tags,
            Optional.empty() /* baseRowId */,
            Optional.empty() /* defaultRowCommitVersion */,
            Optional.empty() /* deletionVectorDescriptor */);
    return SingleAction.createAddFileSingleAction(addFile.toRow());
  }

  public static Row generateIcebergCompatWriterV1AddAction(
      Row transactionState,
      DataFileStatus fileStatus,
      Map<String, Literal> partitionValues,
      boolean dataChange,
      Optional<StructType> physicalSchemaOpt) {
    return generateIcebergCompatWriterV1AddAction(
        transactionState,
        fileStatus,
        partitionValues,
        dataChange,
        Collections.emptyMap(),
        physicalSchemaOpt);
  }

  /**
   * Create an add action {@link Row} that can be passed to {@link Transaction#commit(Engine,
   * CloseableIterable)} from an Iceberg add.
   *
   * @param transactionState the transaction state from the built transaction
   * @param fileStatus the file status to create the add with (contains path, time, size, and stats)
   * @param partitionValues the partition values for the add
   * @param dataChange whether or not the add constitutes a dataChange (i.e. append vs. compaction)
   * @param tags key-value metadata to be attached to the add action
   * @param deletionVectorDescriptor optional deletion vector descriptor for the add action
   * @param physicalSchemaOpt An optional pre-parsed physical schema. Improves performance for batch
   *     operations by avoiding repeated JSON parsing. Recommended when generating many actions with
   *     the same schema.
   * @return add action row that can be included in the transaction
   * @throws UnsupportedOperationException if icebergWriterCompatV3 is not enabled
   * @throws UnsupportedOperationException if maxRetries != 0 in the transaction
   * @throws KernelException if stats are not present (required for icebergCompatV3)
   * @throws UnsupportedOperationException if the table is partitioned (currently unsupported)
   */
  public static Row generateIcebergCompatWriterV3AddAction(
      Row transactionState,
      DataFileStatus fileStatus,
      Map<String, Literal> partitionValues,
      boolean dataChange,
      Map<String, String> tags,
      Optional<Long> baseRowId,
      Optional<Long> defaultRowCommitVersion,
      Optional<DeletionVectorDescriptor> deletionVectorDescriptor,
      Optional<StructType> physicalSchemaOpt) {
    Map<String, String> configuration = TransactionStateRow.getConfiguration(transactionState);

    /* ----- Validate that this is a valid usage of this API ----- */
    validateIcebergWriterCompatV3Enabled(configuration);
    validateMaxRetriesSetToZero(transactionState);

    /* ----- Validate that deletion vector is passed in only when the table supports it ----- */
    deletionVectorDescriptor.ifPresent(dv -> validateIcebergDeletionVectorsEnabled(configuration));

    /* ----- Validate this is valid write given the table's protocol & configurations ----- */
    checkState(
        TableConfig.ICEBERG_COMPAT_V3_ENABLED.fromMetadata(configuration),
        "icebergCompatV3 not enabled despite icebergWriterCompatV3 enabled");
    // We require field `numRecords` when icebergCompatV3 is enabled
    IcebergCompatV3MetadataValidatorAndUpdater.validateDataFileStatus(fileStatus);

    /* --- Validate and update partitionValues ---- */
    // Currently we don't support partitioned tables; fail here
    blockPartitionedTables(transactionState, partitionValues);

    URI tableRoot = new Path(TransactionStateRow.getTablePath(transactionState)).toUri();
    // This takes care of relativizing the file path and serializing the file statistics
    AddFile addFile =
        AddFile.convertDataFileStatus(
            physicalSchemaOpt.orElseGet(
                () -> TransactionStateRow.getPhysicalSchema(transactionState)),
            tableRoot,
            fileStatus,
            partitionValues,
            dataChange,
            tags,
            baseRowId,
            defaultRowCommitVersion,
            deletionVectorDescriptor);
    return SingleAction.createAddFileSingleAction(addFile.toRow());
  }

  /**
   * Create a remove action {@link Row} that can be passed to {@link Transaction#commit(Engine,
   * CloseableIterable)} from an Iceberg remove.
   *
   * @param transactionState the transaction state from the built transaction
   * @param fileStatus the file status to create the remove with (contains path, time, size, and
   *     stats)
   * @param partitionValues the partition values for the remove
   * @param dataChange whether or not the remove constitutes a dataChange (i.e. delete vs.
   *     compaction)
   * @param physicalSchemaOpt An optional pre-parsed physical schema. Improves performance for batch
   *     operations by avoiding repeated JSON parsing. Recommended when generating many actions with
   *     the same schema.
   * @return remove action row that can be committed to the transaction
   * @throws UnsupportedOperationException if icebergWriterCompatV1 is not enabled
   * @throws UnsupportedOperationException if maxRetries != 0 in the transaction
   * @throws KernelException if the table is an append-only table and dataChange=true
   * @throws UnsupportedOperationException if the table is partitioned (currently unsupported)
   */
  public static Row generateIcebergCompatWriterV1RemoveAction(
      Row transactionState,
      DataFileStatus fileStatus,
      Map<String, Literal> partitionValues,
      boolean dataChange,
      Optional<StructType> physicalSchemaOpt) {
    Map<String, String> config = TransactionStateRow.getConfiguration(transactionState);

    /* ----- Validate that this is a valid usage of this API ----- */
    validateIcebergWriterCompatV1Enabled(config);
    validateMaxRetriesSetToZero(transactionState);

    /* ----- Validate this is valid write given the table's protocol & configurations ----- */
    // We only allow removes with dataChange=false when appendOnly=true
    blockUpdatingAppendOnlyTables(dataChange, transactionState, config);

    /* --- Validate and update partitionValues ---- */
    // Currently we don't support partitioned tables; fail here
    blockPartitionedTables(transactionState, partitionValues);

    URI tableRoot = new Path(TransactionStateRow.getTablePath(transactionState)).toUri();
    // This takes care of relativizing the file path and serializing the file statistics
    Row removeFileRow =
        convertRemoveDataFileStatus(
            physicalSchemaOpt.orElseGet(
                () -> TransactionStateRow.getPhysicalSchema(transactionState)),
            tableRoot,
            fileStatus,
            partitionValues,
            dataChange,
            Optional.empty() /* baseRowId */,
            Optional.empty() /* defaultRowCommitVersion */,
            Optional.empty() /* deletionVectorDescriptor */);
    return SingleAction.createRemoveFileSingleAction(removeFileRow);
  }

  /**
   * Create a remove action {@link Row} that can be passed to {@link Transaction#commit(Engine,
   * CloseableIterable)} from an Iceberg remove.
   *
   * @param transactionState the transaction state from the built transaction
   * @param fileStatus the file status to create the remove with (contains path, time, size, and
   *     stats)
   * @param partitionValues the partition values for the remove
   * @param dataChange whether or not the remove constitutes a dataChange (i.e. delete vs.
   *     compaction)
   * @param deletionVectorDescriptor optional deletion vector descriptor for the add action
   * @param physicalSchemaOpt An optional pre-parsed physical schema. Improves performance for batch
   *     operations by avoiding repeated JSON parsing. Recommended when generating many actions with
   *     the same schema.
   * @return remove action row that can be committed to the transaction
   * @throws UnsupportedOperationException if icebergWriterCompatV3 is not enabled
   * @throws UnsupportedOperationException if maxRetries != 0 in the transaction
   * @throws KernelException if the table is an append-only table and dataChange=true
   * @throws UnsupportedOperationException if the table is partitioned (currently unsupported)
   */
  public static Row generateIcebergCompatWriterV3RemoveAction(
      Row transactionState,
      DataFileStatus fileStatus,
      Map<String, Literal> partitionValues,
      boolean dataChange,
      Optional<Long> baseRowId,
      Optional<Long> defaultRowCommitVersion,
      Optional<DeletionVectorDescriptor> deletionVectorDescriptor,
      Optional<StructType> physicalSchemaOpt) {
    Map<String, String> config = TransactionStateRow.getConfiguration(transactionState);

    /* ----- Validate that this is a valid usage of this API ----- */
    validateIcebergWriterCompatV3Enabled(config);
    validateMaxRetriesSetToZero(transactionState);

    /* ----- Validate that deletion vector is passed in only when the table supports it ----- */
    deletionVectorDescriptor.ifPresent(dv -> validateIcebergDeletionVectorsEnabled(config));

    /* ----- Validate this is valid write given the table's protocol & configurations ----- */
    // We only allow removes with dataChange=false when appendOnly=true
    if (dataChange && TableConfig.APPEND_ONLY_ENABLED.fromMetadata(config)) {
      throw DeltaErrors.cannotModifyAppendOnlyTable(
          TransactionStateRow.getTablePath(transactionState));
    }

    /* ----- Validate this is valid write given the table's protocol & configurations ----- */
    // We only allow removes with dataChange=false when appendOnly=true
    blockUpdatingAppendOnlyTables(dataChange, transactionState, config);

    /* --- Validate and update partitionValues ---- */
    // Currently we don't support partitioned tables; fail here
    blockPartitionedTables(transactionState, partitionValues);

    URI tableRoot = new Path(TransactionStateRow.getTablePath(transactionState)).toUri();
    // This takes care of relativizing the file path and serializing the file statistics
    Row removeFileRow =
        convertRemoveDataFileStatus(
            physicalSchemaOpt.orElseGet(
                () -> TransactionStateRow.getPhysicalSchema(transactionState)),
            tableRoot,
            fileStatus,
            partitionValues,
            dataChange,
            Optional.empty() /* baseRowId */,
            Optional.empty() /* defaultRowCommitVersion */,
            deletionVectorDescriptor);
    return SingleAction.createRemoveFileSingleAction(removeFileRow);
  }

  /////////////////////
  // Private helpers //
  /////////////////////

  /**
   * Validates that table feature `icebergWriterCompatV1` is enabled. We restrict usage of these
   * APIs to require that this table feature is enabled to prevent any unsafe usage due to the table
   * features that are blocked via `icebergWriterCompatV1` (for example, rowTracking or
   * deletionVectors).
   */
  private static void validateIcebergWriterCompatV1Enabled(Map<String, String> config) {
    if (!TableConfig.ICEBERG_WRITER_COMPAT_V1_ENABLED.fromMetadata(config)) {
      throw new UnsupportedOperationException(
          String.format(
              "APIs within GenerateIcebergCompatActionUtils are only supported on tables with"
                  + " '%s' set to true",
              TableConfig.ICEBERG_WRITER_COMPAT_V1_ENABLED.getKey()));
    }
  }

  /**
   * Validates that table feature `icebergWriterCompatV3` is enabled. We restrict usage of these
   * APIs to require that this table feature is enabled to prevent any unsafe usage due to the table
   * features that are blocked via `icebergWriterCompatV3`.
   */
  private static void validateIcebergWriterCompatV3Enabled(Map<String, String> config) {
    if (!TableConfig.ICEBERG_WRITER_COMPAT_V3_ENABLED.fromMetadata(config)) {
      throw new UnsupportedOperationException(
          String.format(
              "APIs within GenerateIcebergCompatActionUtils are only supported on tables with"
                  + " '%s' set to true",
              TableConfig.ICEBERG_WRITER_COMPAT_V3_ENABLED.getKey()));
    }
  }

  /**
   * Validates that table feature `deletion vectors` is enabled. Checked when a deletion vector
   * descriptor is passed to generateIcebergCompatWriterV3AddAction.
   */
  private static void validateIcebergDeletionVectorsEnabled(Map<String, String> config) {
    if (!TableConfig.DELETION_VECTORS_CREATION_ENABLED.fromMetadata(config)) {
      throw new UnsupportedOperationException(
          String.format(
              "APIs within GenerateIcebergCompatActionUtils are only supported on tables with"
                  + " '%s' set to true",
              TableConfig.DELETION_VECTORS_CREATION_ENABLED.getKey()));
    }
  }

  /**
   * Throws an exception if `maxRetries` was not set to 0 in the transaction. We restrict these APIs
   * to require `maxRetries = 0` since conflict resolution is not supported for operations other
   * than blind appends.
   */
  private static void validateMaxRetriesSetToZero(Row transactionState) {
    if (TransactionStateRow.getMaxRetries(transactionState) > 0) {
      throw new UnsupportedOperationException(
          String.format(
              "Usage of GenerateIcebergCompatActionUtils requires maxRetries=0, "
                  + "found maxRetries=%s",
              TransactionStateRow.getMaxRetries(transactionState)));
    }
  }

  private static void blockUpdatingAppendOnlyTables(
      boolean dataChange, Row transactionState, Map<String, String> config) {
    // We only allow removes with dataChange=false when appendOnly=true
    if (dataChange && TableConfig.APPEND_ONLY_ENABLED.fromMetadata(config)) {
      throw DeltaErrors.cannotModifyAppendOnlyTable(
          TransactionStateRow.getTablePath(transactionState));
    }
  }

  private static void blockPartitionedTables(
      Row transactionState, Map<String, Literal> partitionValues) {
    if (!TransactionStateRow.getPartitionColumnsList(transactionState).isEmpty()) {
      throw new UnsupportedOperationException(
          "Currently GenerateIcebergCompatActionUtils "
              + "is not supported for partitioned tables");
    }
    checkArgument(
        partitionValues.isEmpty(), "Non-empty partitionValues provided for an unpartitioned table");
  }

  //////////////////////////////////////////////////
  // Private methods for creating RemoveFile rows //
  //////////////////////////////////////////////////
  // I've added these APIs here since they rely on the assumptions validated within
  // GenerateIcebergCompatActionUtils such as icebergWriterCompatV1 is enabled --> rowTracking is
  // disabled. Since these APIs are not valid without these assumptions, holding off on putting them
  // within RemoveFile.java until we add full support for deletes (which will likely involve
  // generating RemoveFiles directly from AddFiles anyway)

  @VisibleForTesting
  public static Row convertRemoveDataFileStatus(
      StructType physicalSchema,
      URI tableRoot,
      DataFileStatus dataFileStatus,
      Map<String, Literal> partitionValues,
      boolean dataChange,
      Optional<Long> baseRowId,
      Optional<Long> defaultRowCommitVersion,
      Optional<DeletionVectorDescriptor> deletionVectorDescriptor) {
    return createRemoveFileRowWithExtendedFileMetadata(
        relativizePath(new Path(dataFileStatus.getPath()), tableRoot).toUri().toString(),
        dataFileStatus.getModificationTime(),
        dataChange,
        serializePartitionMap(partitionValues),
        dataFileStatus.getSize(),
        dataFileStatus.getStatistics(),
        physicalSchema,
        baseRowId,
        defaultRowCommitVersion,
        deletionVectorDescriptor);
  }

  @VisibleForTesting
  public static Row createRemoveFileRowWithExtendedFileMetadata(
      String path,
      long deletionTimestamp,
      boolean dataChange,
      MapValue partitionValues,
      long size,
      Optional<DataFileStatistics> stats,
      StructType physicalSchema,
      Optional<Long> baseRowId,
      Optional<Long> defaultRowCommitVersion,
      Optional<DeletionVectorDescriptor> deletionVector) {
    Map<Integer, Object> fieldMap = new HashMap<>();
    fieldMap.put(RemoveFile.FULL_SCHEMA.indexOf("path"), requireNonNull(path));
    fieldMap.put(RemoveFile.FULL_SCHEMA.indexOf("deletionTimestamp"), deletionTimestamp);
    fieldMap.put(RemoveFile.FULL_SCHEMA.indexOf("dataChange"), dataChange);
    fieldMap.put(RemoveFile.FULL_SCHEMA.indexOf("extendedFileMetadata"), true);
    fieldMap.put(
        RemoveFile.FULL_SCHEMA.indexOf("partitionValues"), requireNonNull(partitionValues));
    fieldMap.put(RemoveFile.FULL_SCHEMA.indexOf("size"), size);
    stats.ifPresent(
        stat ->
            fieldMap.put(
                RemoveFile.FULL_SCHEMA.indexOf("stats"), stat.serializeAsJson(physicalSchema)));
    baseRowId.ifPresent(id -> fieldMap.put(RemoveFile.FULL_SCHEMA.indexOf("baseRowId"), id));
    defaultRowCommitVersion.ifPresent(
        version ->
            fieldMap.put(RemoveFile.FULL_SCHEMA.indexOf("defaultRowCommitVersion"), version));
    deletionVector.ifPresent(
        dv -> {
          Row dvRow = dv.toRow();
          fieldMap.put(RemoveFile.FULL_SCHEMA.indexOf("deletionVector"), dvRow);
        });
    return new GenericRow(RemoveFile.FULL_SCHEMA, fieldMap);
  }
}
