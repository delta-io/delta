/*
 * Copyright (2023) The Delta Lake Project Authors.
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

package io.delta.kernel;

import io.delta.kernel.annotation.Evolving;
import io.delta.kernel.data.*;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.expressions.Predicate;
import io.delta.kernel.internal.InternalScanFileUtils;
import io.delta.kernel.internal.ScanImpl;
import io.delta.kernel.internal.TableConfig;
import io.delta.kernel.internal.actions.DeletionVectorDescriptor;
import io.delta.kernel.internal.data.ScanStateRow;
import io.delta.kernel.internal.data.SelectionColumnVector;
import io.delta.kernel.internal.deletionvectors.DeletionVectorUtils;
import io.delta.kernel.internal.deletionvectors.RoaringBitmapArray;
import io.delta.kernel.internal.rowtracking.MaterializedRowTrackingColumn;
import io.delta.kernel.internal.util.ColumnMapping.ColumnMappingMode;
import io.delta.kernel.internal.util.PartitionUtils;
import io.delta.kernel.internal.util.Tuple2;
import io.delta.kernel.types.MetadataColumn;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;
import java.io.IOException;
import java.util.Map;
import java.util.Optional;

/**
 * Represents a scan of a Delta table.
 *
 * @since 3.0.0
 */
@Evolving
public interface Scan {
  /**
   * Get an iterator of data files to scan.
   *
   * @param engine {@link Engine} instance to use in Delta Kernel.
   * @return iterator of {@link FilteredColumnarBatch}s where each selected row in the batch
   *     corresponds to one scan file. Schema of each row is defined as follows:
   *     <p>
   *     <ol>
   *       <li>
   *           <ul>
   *             <li>name: {@code add}, type: {@code struct}
   *             <li>Description: Represents `AddFile` DeltaLog action
   *             <li>
   *                 <ul>
   *                   <li>name: {@code path}, type: {@code string}, description: location of the
   *                       file. The path is a URI as specified by RFC 2396 URI Generic Syntax,
   *                       which needs to be decoded to get the data file path.
   *                   <li>name: {@code partitionValues}, type: {@code map(string, string)},
   *                       description: A map from partition column to value for this logical file.
   *                   <li>name: {@code size}, type: {@code long}, description: size of the file.
   *                   <li>name: {@code modificationTime}, type: {@code log}, description: the time
   *                       this logical file was created, as milliseconds since the epoch.
   *                   <li>name: {@code dataChange}, type: {@code boolean}, description: When false
   *                       the logical file must already be present in the table or the records in
   *                       the added file must be contained in one or more remove actions in the
   *                       same version
   *                   <li>name: {@code deletionVector}, type: {@code string}, description: Either
   *                       null (or absent in JSON) when no DV is associated with this data file, or
   *                       a struct (described below) that contains necessary information about the
   *                       DV that is part of this logical file. For description of each member
   *                       variable in `deletionVector` @see <a
   *                       href=https://github.com/delta-io/delta/blob/master/PROTOCOL.md#Deletion-Vectors>
   *                       Protocol</a>
   *                       <ul>
   *                         <li>name: {@code storageType}, type: {@code string}
   *                         <li>name: {@code pathOrInlineDv}, type: {@code string}, description:
   *                             The path is a URI as specified by RFC 2396 URI Generic Syntax,
   *                             which needs to be decoded to get the data file path.
   *                         <li>name: {@code offset}, type: {@code log}
   *                         <li>name: {@code sizeInBytes}, type: {@code log}
   *                         <li>name: {@code cardinality}, type: {@code log}
   *                       </ul>
   *                   <li>name: {@code tags}, type: {@code map(string, string)}, description: Map
   *                       containing metadata about the scan file.
   *                 </ul>
   *           </ul>
   *       <li>
   *           <ul>
   *             <li>name: {@code tableRoot}, type: {@code string}
   *             <li>Description: Absolute path of the table location. The path is a URI as
   *                 specified by RFC 2396 URI Generic Syntax, which needs to be decoded to get the
   *                 data file path. NOTE: this is temporary. Will be removed in the future.
   * @see <a href=https://github.com/delta-io/delta/issues/2089></a>
   *     </ul>
   *     </ol>
   */
  CloseableIterator<FilteredColumnarBatch> getScanFiles(Engine engine);

  /**
   * Get the remaining filter that is not guaranteed to be satisfied for the data Delta Kernel
   * returns. This filter is used by Delta Kernel to do data skipping when possible.
   *
   * @return the remaining filter as a {@link Predicate}.
   */
  Optional<Predicate> getRemainingFilter();

  /**
   * Get the scan state associated with the current scan. This state is common across all files in
   * the scan to be read.
   *
   * @param engine {@link Engine} instance to use in Delta Kernel.
   * @return Scan state in {@link Row} format.
   */
  Row getScanState(Engine engine);

  /**
   * Transform the physical data read from the table data file to the logical data that are expected
   * out of the Delta table.
   *
   * <p>This iterator effectively reverses the logical-to-physical schema transformation performed
   * in {@link ScanImpl#getScanState(Engine)} by transforming physical data batches into the logical
   * data requested by the connector.
   *
   * @param engine Connector provided {@link Engine} implementation.
   * @param scanState Scan state returned by {@link Scan#getScanState(Engine)}
   * @param scanFile Scan file from where the physical data {@code physicalDataIter} is read from.
   * @param physicalDataIter Iterator of {@link ColumnarBatch}s containing the physical data read
   *     from the {@code scanFile}.
   * @return Data read from the input scan files as an iterator of {@link FilteredColumnarBatch}s.
   *     Each {@link FilteredColumnarBatch} instance contains the data read and an optional
   *     selection vector that indicates data rows as valid or invalid. It is the responsibility of
   *     the caller to close this iterator.
   * @throws IOException when error occurs while reading the data.
   */
  static CloseableIterator<FilteredColumnarBatch> transformPhysicalData(
      Engine engine, Row scanState, Row scanFile, CloseableIterator<ColumnarBatch> physicalDataIter)
      throws IOException {
    return new CloseableIterator<FilteredColumnarBatch>() {
      boolean inited = false;

      // initialized as part of init()
      StructType logicalSchema = null;
      Map<String, String> configuration = null;
      ColumnMappingMode columnMappingMode = null;
      String tablePath = null;

      RoaringBitmapArray currBitmap = null;
      DeletionVectorDescriptor currDV = null;

      private void initIfRequired() {
        if (inited) {
          return;
        }
        logicalSchema = ScanStateRow.getLogicalSchema(scanState);
        configuration = ScanStateRow.getConfiguration(scanState);
        columnMappingMode = ScanStateRow.getColumnMappingMode(scanState);
        tablePath = ScanStateRow.getTableRoot(scanState).toString();
        inited = true;
      }

      @Override
      public void close() throws IOException {
        physicalDataIter.close();
      }

      @Override
      public boolean hasNext() {
        initIfRequired();
        return physicalDataIter.hasNext();
      }

      @Override
      public FilteredColumnarBatch next() {
        initIfRequired();
        ColumnarBatch nextDataBatch = physicalDataIter.next();

        // Step 1: If row tracking is enabled, check for physical row tracking columns in the data
        // batch and transform them to logical row tracking columns as needed
        if (TableConfig.ROW_TRACKING_ENABLED.fromMetadata(configuration)) {
          nextDataBatch =
              MaterializedRowTrackingColumn.transformPhysicalData(
                  nextDataBatch, scanFile, logicalSchema, configuration, engine);
        }

        // Step 2: Get the selectionVector if DV is present
        DeletionVectorDescriptor dv =
            InternalScanFileUtils.getDeletionVectorDescriptorFromRow(scanFile);
        Optional<ColumnVector> selectionVector;
        if (dv == null) {
          selectionVector = Optional.empty();
        } else {
          int rowIndexOrdinal = nextDataBatch.getSchema().indexOf(MetadataColumn.ROW_INDEX);
          if (rowIndexOrdinal == -1) {
            throw new IllegalArgumentException(
                "Row index column is not present in the data read from the Parquet file.");
          }
          if (!dv.equals(currDV)) {
            Tuple2<DeletionVectorDescriptor, RoaringBitmapArray> dvInfo =
                DeletionVectorUtils.loadNewDvAndBitmap(engine, tablePath, dv);
            this.currDV = dvInfo._1;
            this.currBitmap = dvInfo._2;
          }
          ColumnVector rowIndexVector = nextDataBatch.getColumnVector(rowIndexOrdinal);
          selectionVector = Optional.of(new SelectionColumnVector(currBitmap, rowIndexVector));
        }

        // Step 3: If a column was only requested to compute other columns, we remove it
        for (StructField field : nextDataBatch.getSchema().fields()) {
          if (field.isInternalColumn()) {
            int columnOrdinal = nextDataBatch.getSchema().indexOf(field.getName());
            if (columnOrdinal == -1) {
              // This should never happen since we only interact with a single schema
              throw new IllegalArgumentException(
                  String.format(
                      "Column %s was requested internally but is not present in the data batch.",
                      field.getName()));
            }
            nextDataBatch = nextDataBatch.withDeletedColumnAt(columnOrdinal);
          }
        }

        // Step 4: Add partition columns back to the data batch
        nextDataBatch =
            PartitionUtils.withPartitionColumns(
                nextDataBatch,
                logicalSchema,
                InternalScanFileUtils.getPartitionValues(scanFile),
                engine.getExpressionHandler());

        // Step 5: Transform column names back to logical names if column mapping is enabled
        switch (columnMappingMode) {
          case NAME: // fall through
          case ID:
            nextDataBatch = nextDataBatch.withNewSchema(logicalSchema);
            break;
          case NONE:
            break;
          default:
            throw new UnsupportedOperationException(
                "Column mapping mode is not yet supported: " + columnMappingMode);
        }

        return new FilteredColumnarBatch(nextDataBatch, selectionVector);
      }
    };
  }
}
