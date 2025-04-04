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

import io.delta.kernel.Transaction;
import io.delta.kernel.data.Row;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.exceptions.KernelException;
import io.delta.kernel.expressions.Literal;
import io.delta.kernel.internal.TableConfig;
import io.delta.kernel.internal.data.TransactionStateRow;
import io.delta.kernel.internal.fs.Path;
import io.delta.kernel.internal.icebergcompat.IcebergCompatV2MetadataValidatorAndUpdater;
import io.delta.kernel.utils.CloseableIterable;
import io.delta.kernel.utils.DataFileStatus;
import java.net.URI;
import java.util.Map;

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
   * @return add action row that can be committed to the transaction
   * @throws UnsupportedOperationException if icebergWriterCompatV1 is not enabled
   * @throws UnsupportedOperationException if maxRetries != 0 in the transaction
   * @throws KernelException if stats are not present (required for icebergCompatV2)
   * @throws UnsupportedOperationException if the table is partitioned (currently unsupported)
   */
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
    // TODO possible stats enforcement for clustering (maybe not necessary due to icebergCompatV2?)

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
    AddFile addFile =
        AddFile.convertDataFileStatus(
            TransactionStateRow.getPhysicalSchema(transactionState),
            tableRoot,
            fileStatus,
            partitionValues,
            dataChange);
    return SingleAction.createAddFileSingleAction(addFile.toRow());
  }

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
}
