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

import static io.delta.kernel.internal.data.TransactionStateRow.*;
import static io.delta.kernel.internal.util.Preconditions.checkArgument;

import io.delta.kernel.data.Row;
import io.delta.kernel.expressions.Literal;
import io.delta.kernel.internal.DeltaErrors;
import io.delta.kernel.internal.TableConfig;
import io.delta.kernel.internal.data.TransactionStateRow;
import io.delta.kernel.internal.fs.Path;
import io.delta.kernel.internal.icebergcompat.IcebergCompatV2MetadataValidatorAndUpdater;
import io.delta.kernel.utils.DataFileStatus;
import java.net.URI;
import java.util.Map;

/** TODO docs (what is okay to say here?) */
public final class GenerateIcebergCompatActionUtils {

  /**
   * Validates that table feature `icebergWriterCompatV1` is enabled. We restrict usage of these
   * APIs to require that this table feature is enabled to prevent any unsafe usage due to the table
   * features that are blocked via `icebergWriterCompatV1` (for example, rowTracking or
   * deletionVectors).
   */
  private static void validateIcebergWriterCompatV1Enabled(Map<String, String> config) {
    if (!TableConfig.ICEBERG_WRITER_COMPAT_V1_ENABLED.fromMetadata(config)) {
      throw new UnsupportedOperationException(
          "APIs within GenerateIcebergCompatActionUtils are only supported on tables with"
              + " 'delta.enableIcebergWriterCompatV1' set to true");
    }
  }

  /**
   * Throws an exception if `maxRetries` was not set to 0 in the transaction. We restrict these APIs
   * to require `maxRetries = 0` since conflict resolution is not supported for operations other
   * than blind appends.
   */
  private static void validateMaxRetriesSetToZero(Row transactionState) {
    if (getMaxRetries(transactionState) > 0) {
      throw new UnsupportedOperationException(
          String.format(
              "Usage of GenerateIcebergCompatActionUtils requires maxRetries=0, "
                  + "found maxRetries=%s",
              getMaxRetries(transactionState)));
    }
  }

  // TODO is there any future reason we would need an `engine` arg?
  // TODO docs
  public static Row generateIcebergCompatWriterV1AddAction(
      Row transactionState,
      DataFileStatus fileStatus,
      Map<String, Literal> partitionValues,
      boolean dataChange) {
    Map<String, String> config = getConfiguration(transactionState);

    /* ----- Validate that this is a valid usage of this API ----- */
    validateIcebergWriterCompatV1Enabled(config);
    validateMaxRetriesSetToZero(transactionState);

    /* ----- Validate this is valid write given the table's protocol & configurations ----- */
    // note -- we know this must be enabled since IcebergCompatWriterV1 is enabled
    if (TableConfig.ICEBERG_COMPAT_V2_ENABLED.fromMetadata(config)) {
      // We require field `numRecords` when icebergCompatV2 is enabled
      IcebergCompatV2MetadataValidatorAndUpdater.validateDataFileStatus(fileStatus);
    }
    if (!getClusteringColumns(transactionState).isEmpty()) {
      // TODO when adding clustering support validate that stats are present for clustering
      //  columns here
      throw new UnsupportedOperationException("Clustering support not yet implemented");
    }

    /* --- Validate and update partitionValues ---- */
    // Currently we don't support partitioned tables; fail here
    if (!getPartitionColumnsList(transactionState).isEmpty()) {
      throw new UnsupportedOperationException(
          "Currently GenerateIcebergCompatActionUtils "
              + "is not supported for partitioned tables");
    }
    checkArgument(
        partitionValues.isEmpty(), "Non-empty partitionValues provided for an unpartitioned table");

    URI tableRoot = new Path(getTablePath(transactionState)).toUri();
    // This takes care of relativizing the file path and serializing the file statistics
    // (including converting from fieldId -> physical name)
    AddFile addFile =
        AddFile.convertDataFileStatus(
            TransactionStateRow.getPhysicalSchema(transactionState),
            tableRoot,
            fileStatus,
            partitionValues,
            dataChange);
    return SingleAction.createAddFileSingleAction(addFile.toRow());
  }

  // TODO docs
  public static Row generateIcebergCompatWriterV1RemoveAction(
      Row transactionState,
      DataFileStatus fileStatus,
      Map<String, Literal> partitionValues,
      boolean dataChange) {
    Map<String, String> config = getConfiguration(transactionState);

    /* ----- Validate that this is a valid usage of this API ----- */
    validateIcebergWriterCompatV1Enabled(config);
    validateMaxRetriesSetToZero(transactionState);

    /* ----- Validate this is valid write given the table's protocol & configurations ----- */
    // We only allow removes with dataChange=false when appendOnly=true
    if (dataChange && TableConfig.APPEND_ONLY_ENABLED.fromMetadata(config)) {
      throw DeltaErrors.cannotModifyAppendOnlyTable(getTablePath(transactionState));
    }
    // TODO do this check at commit time as well

    /* --- Validate and update partitionValues ---- */
    // Currently we don't support partitioned tables; fail here
    if (!getPartitionColumnsList(transactionState).isEmpty()) {
      throw new UnsupportedOperationException(
          "Currently GenerateIcebergCompatActionUtils "
              + "is not supported for partitioned tables");
    }
    checkArgument(
        partitionValues.isEmpty(), "Non-empty partitionValues provided for an unpartitioned table");

    URI tableRoot = new Path(getTablePath(transactionState)).toUri();
    // This takes care of relativizing the file path and serializing the file statistics
    // (including converting from fieldId -> physical name)
    Row removeFileRow =
        RemoveFile.convertDataFileStatus(
            TransactionStateRow.getPhysicalSchema(transactionState),
            tableRoot,
            fileStatus,
            partitionValues,
            dataChange);
    return SingleAction.createRemoveFileSingleAction(removeFileRow);
  }
}
