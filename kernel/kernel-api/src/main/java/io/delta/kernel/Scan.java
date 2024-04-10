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

import java.io.IOException;
import java.util.Optional;

import io.delta.kernel.annotation.Evolving;
import io.delta.kernel.client.TableClient;
import io.delta.kernel.data.*;
import io.delta.kernel.expressions.Cast;
import io.delta.kernel.expressions.Column;
import io.delta.kernel.expressions.ExpressionEvaluator;
import io.delta.kernel.expressions.Predicate;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;

import io.delta.kernel.internal.InternalScanFileUtils;
import io.delta.kernel.internal.actions.DeletionVectorDescriptor;
import io.delta.kernel.internal.data.ScanStateRow;
import io.delta.kernel.internal.data.SelectionColumnVector;
import io.delta.kernel.internal.deletionvectors.DeletionVectorUtils;
import io.delta.kernel.internal.deletionvectors.RoaringBitmapArray;
import io.delta.kernel.internal.util.ColumnMapping;
import io.delta.kernel.internal.util.PartitionUtils;
import io.delta.kernel.internal.util.Tuple2;

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
     * @param tableClient {@link TableClient} instance to use in Delta Kernel.
     * @return iterator of {@link FilteredColumnarBatch}s where each selected row in
     * the batch corresponds to one scan file. Schema of each row is defined as follows:
     * <p>
     * <ol>
     *  <li><ul>
     *   <li>name: {@code add}, type: {@code struct}</li>
     *   <li>Description: Represents `AddFile` DeltaLog action</li>
     *   <li><ul>
     *    <li>name: {@code path}, type: {@code string}, description: location of the file.</li>
     *    <li>name: {@code partitionValues}, type: {@code map(string, string)},
     *       description: A map from partition column to value for this logical file. </li>
     *    <li>name: {@code size}, type: {@code log}, description: size of the file.</li>
     *    <li>name: {@code modificationTime}, type: {@code log}, description: the time this
     *       logical file was created, as milliseconds since the epoch.</li>
     *    <li>name: {@code dataChange}, type: {@code boolean}, description: When false the
     *      logical file must already be present in the table or the records in the added file
     *      must be contained in one or more remove actions in the same version</li>
     *    <li>name: {@code deletionVector}, type: {@code string}, description: Either null
     *      (or absent in JSON) when no DV is associated with this data file, or a struct
     *      (described below) that contains necessary information about the DV that is part of
     *      this logical file. For description of each member variable in `deletionVector` @see
     *      <a href=https://github.com/delta-io/delta/blob/master/PROTOCOL.md#Deletion-Vectors>
     *          Protocol</a><ul>
     *       <li>name: {@code storageType}, type: {@code string}</li>
     *       <li>name: {@code pathOrInlineDv}, type: {@code string}</li>
     *       <li>name: {@code offset}, type: {@code log}</li>
     *       <li>name: {@code sizeInBytes}, type: {@code log}</li>
     *       <li>name: {@code cardinality}, type: {@code log}</li>
     *    </ul></li>
     *   </ul></li>
     *  </ul></li>
     *  <li><ul>
     *      <li>name: {@code tableRoot}, type: {@code string}</li>
     *      <li>Description: Absolute path of the table location. NOTE: this is temporary. Will
     *      be removed in future. @see <a href=https://github.com/delta-io/delta/issues/2089>
     *          </a></li>
     *  </ul></li>
     * </ol>
     */
    CloseableIterator<FilteredColumnarBatch> getScanFiles(TableClient tableClient);

    /**
     * Get the remaining filter that is not guaranteed to be satisfied for the data Delta Kernel
     * returns. This filter is used by Delta Kernel to do data skipping when possible.
     *
     * @return the remaining filter as a {@link Predicate}.
     */
    Optional<Predicate> getRemainingFilter();

    /**
     * Get the scan state associated with the current scan. This state is common across all
     * files in the scan to be read.
     *
     * @param tableClient {@link TableClient} instance to use in Delta Kernel.
     * @return Scan state in {@link Row} format.
     */
    Row getScanState(TableClient tableClient);

    /**
     * Get the physical data read schema to pass to the parquet handler to read the given data file.
     *
     * @param tableClient instance of {@link TableClient} to use.
     * @param scanState   Scan state {@link Row}
     * @param scanFile   Scan file {@link Row}
     * @return Physical schema to read from the data files.
     */
    StructType getPhysicalDataReadSchema(TableClient tableClient, Row scanState, Row scanFile);

    /**
     * Transform the physical data read from the table data file into the logical data that expected
     * out of the Delta table.
     *
     * @param tableClient      Connector provided {@link TableClient} implementation.
     * @param scanState        Scan state returned by {@link Scan#getScanState(TableClient)}
     * @param scanFile         Scan file from where the physical data {@code physicalDataIter} is
     *                         read from.
     * @param physicalDataIter Iterator of {@link ColumnarBatch}s containing the physical data read
     *                         from the {@code scanFile}.
     * @return Data read from the input scan files as an iterator of {@link FilteredColumnarBatch}s.
     * Each {@link FilteredColumnarBatch} instance contains the data read and an optional selection
     * vector that indicates data rows as valid or invalid. It is the responsibility of the
     * caller to close this iterator.
     * @throws IOException when error occurs while reading the data.
     */
    static CloseableIterator<FilteredColumnarBatch> transformPhysicalData(
            TableClient tableClient,
            Row scanState,
            Row scanFile,
            CloseableIterator<ColumnarBatch> physicalDataIter) throws IOException {
        return new CloseableIterator<FilteredColumnarBatch>() {
            boolean inited = false;

            // initialized as part of init()
            StructType physicalReadSchema = null;
            StructType logicalReadSchema = null;
            String tablePath = null;

            RoaringBitmapArray currBitmap = null;
            DeletionVectorDescriptor currDV = null;

            private void initIfRequired() {
                if (inited) {
                    return;
                }
                physicalReadSchema = ScanStateRow.getPhysicalSchema(tableClient, scanState);
                logicalReadSchema = ScanStateRow.getLogicalSchema(tableClient, scanState);

                tablePath = ScanStateRow.getTableRoot(scanState);
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

                DeletionVectorDescriptor dv =
                    InternalScanFileUtils.getDeletionVectorDescriptorFromRow(scanFile);

                int rowIndexOrdinal = nextDataBatch.getSchema()
                    .indexOf(StructField.METADATA_ROW_INDEX_COLUMN_NAME);

                // Get the selectionVector if DV is present
                Optional<ColumnVector> selectionVector;
                if (dv == null) {
                    selectionVector = Optional.empty();
                } else {
                    if (rowIndexOrdinal == -1) {
                        throw new IllegalArgumentException("Row index column is not " +
                            "present in the data read from the Parquet file.");
                    }
                    if (!dv.equals(currDV)) {
                        Tuple2<DeletionVectorDescriptor, RoaringBitmapArray> dvInfo =
                            DeletionVectorUtils.loadNewDvAndBitmap(tableClient, tablePath, dv);
                        this.currDV = dvInfo._1;
                        this.currBitmap = dvInfo._2;
                    }
                    ColumnVector rowIndexVector = nextDataBatch.getColumnVector(rowIndexOrdinal);
                    selectionVector =
                        Optional.of(new SelectionColumnVector(currBitmap, rowIndexVector));
                }
                if (rowIndexOrdinal != -1) {
                    nextDataBatch = nextDataBatch.withDeletedColumnAt(rowIndexOrdinal);
                }

                // Add partition columns
                nextDataBatch =
                    PartitionUtils.withPartitionColumns(
                        tableClient.getExpressionHandler(),
                        nextDataBatch,
                        InternalScanFileUtils.getPartitionValues(scanFile),
                        physicalReadSchema
                    );

                // Change back to logical schema
                String columnMappingMode = ScanStateRow.getColumnMappingMode(scanState);
                switch (columnMappingMode) {
                    case ColumnMapping.COLUMN_MAPPING_MODE_NAME:
                    case ColumnMapping.COLUMN_MAPPING_MODE_ID:
                        nextDataBatch = nextDataBatch.withNewSchema(logicalReadSchema);
                        break;
                    case ColumnMapping.COLUMN_MAPPING_MODE_NONE:
                        break;
                    default:
                        throw new UnsupportedOperationException(
                            "Column mapping mode is not yet supported: " + columnMappingMode);
                }

                // Type widening: add casts whenever the type of column read in the parquet file
                // doesn't match the expected type from the table schema.
                for (int colIdx = 0; colIdx < logicalReadSchema.length(); colIdx++) {
                    StructField field = logicalReadSchema.at(colIdx);
                    DataType parquetType =
                            nextDataBatch.getSchema().get(field.getName()).getDataType();

                    if (!parquetType.equals(field.getDataType())) {
                        Cast cast =  new Cast(new Column(field.getName()), field.getDataType());

                        // If creating the evaluators is expensive, we could consider caching and
                        // reuse the same evaluator when the same cast in needed.
                        ExpressionEvaluator evaluator =
                            tableClient.getExpressionHandler().getEvaluator(
                                nextDataBatch.getSchema(),
                                cast,
                                field.getDataType()
                        );

                        ColumnVector converted = evaluator.eval(nextDataBatch);
                        nextDataBatch = nextDataBatch.withDeletedColumnAt(colIdx);
                        nextDataBatch = nextDataBatch.withNewColumn(colIdx, field, converted);
                    }
                }
                return new FilteredColumnarBatch(nextDataBatch, selectionVector);
            }
        };
    }
}
