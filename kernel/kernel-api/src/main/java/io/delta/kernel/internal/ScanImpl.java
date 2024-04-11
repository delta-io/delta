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
package io.delta.kernel.internal;

import java.io.IOException;
import java.util.*;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;

import io.delta.kernel.Scan;
import io.delta.kernel.client.TableClient;
import io.delta.kernel.data.*;
import io.delta.kernel.expressions.*;
import io.delta.kernel.types.*;
import io.delta.kernel.utils.CloseableIterator;

import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.actions.Protocol;
import io.delta.kernel.internal.data.ScanStateRow;
import io.delta.kernel.internal.fs.Path;
import io.delta.kernel.internal.replay.LogReplay;
import io.delta.kernel.internal.skipping.DataSkippingPredicate;
import io.delta.kernel.internal.skipping.DataSkippingUtils;
import io.delta.kernel.internal.util.*;
import static io.delta.kernel.internal.skipping.StatsSchemaHelper.getStatsSchema;
import static io.delta.kernel.internal.util.PartitionUtils.rewritePartitionPredicateOnCheckpointFileSchema;
import static io.delta.kernel.internal.util.PartitionUtils.rewritePartitionPredicateOnScanFileSchema;

/**
 * Implementation of {@link Scan}
 */
public class ScanImpl implements Scan {
    /**
     * Schema of the snapshot from the Delta log being scanned in this scan. It is a logical schema
     * with metadata properties to derive the physical schema.
     */
    private final StructType snapshotSchema;

    /** Schema that we actually want to read. */
    private final StructType readSchema;
    private final Protocol protocol;
    private final Metadata metadata;
    private final LogReplay logReplay;
    private final Path dataPath;
    private final Optional<Tuple2<Predicate, Predicate>> partitionAndDataFilters;
    private final Supplier<Map<String, StructField>> partitionColToStructFieldMap;
    private boolean accessedScanFiles;

    public ScanImpl(
            StructType snapshotSchema,
            StructType readSchema,
            Protocol protocol,
            Metadata metadata,
            LogReplay logReplay,
            Optional<Predicate> filter,
            Path dataPath) {
        this.snapshotSchema = snapshotSchema;
        this.readSchema = readSchema;
        this.protocol = protocol;
        this.metadata = metadata;
        this.logReplay = logReplay;
        this.partitionAndDataFilters = splitFilters(filter);
        this.dataPath = dataPath;
        this.partitionColToStructFieldMap = () -> {
            Set<String> partitionColNames = metadata.getPartitionColNames();
            return metadata.getSchema().fields().stream()
                    .filter(field -> partitionColNames.contains(
                            field.getName().toLowerCase(Locale.ROOT)))
                    .collect(toMap(
                            field -> field.getName().toLowerCase(Locale.ROOT),
                            identity()));
        };
    }

    /**
     * Get an iterator of data files in this version of scan that survived the predicate pruning.
     *
     * @return data in {@link ColumnarBatch} batch format. Each row correspond to one survived file.
     */
    @Override
    public CloseableIterator<FilteredColumnarBatch> getScanFiles(TableClient tableClient) {
        if (accessedScanFiles) {
            throw new IllegalStateException("Scan files are already fetched from this instance");
        }
        accessedScanFiles = true;

        // Generate data skipping filter and decide if we should read the stats column
        Optional<DataSkippingPredicate> dataSkippingFilter = getDataSkippingFilter();
        boolean shouldReadStats = dataSkippingFilter.isPresent();

        // Get active AddFiles via log replay
        // If there is a partition predicate, construct a predicate to prune checkpoint files
        // while constructing the table state.
        CloseableIterator<FilteredColumnarBatch> scanFileIter =
                logReplay.getAddFilesAsColumnarBatches(
                        shouldReadStats,
                        getPartitionsFilters().map(predicate ->
                                rewritePartitionPredicateOnCheckpointFileSchema(
                                        predicate,
                                        partitionColToStructFieldMap.get())));

        // Apply partition pruning
        scanFileIter = applyPartitionPruning(tableClient, scanFileIter);

        // Apply data skipping
        if (shouldReadStats) {
            // there was a usable data skipping filter --> apply data skipping
            // TODO drop stats column before returning
            return applyDataSkipping(tableClient, scanFileIter, dataSkippingFilter.get());
        } else {
            return scanFileIter;
        }
    }

    @Override
    public Row getScanState(TableClient tableClient) {
        // Physical equivalent of the logical read schema.
        StructType physicalReadSchema = ColumnMapping.convertToPhysicalSchema(
            readSchema,
            snapshotSchema,
            ColumnMapping.getColumnMappingMode(metadata.getConfiguration()));

        return ScanStateRow.of(
            metadata,
            protocol,
            readSchema.toJson(),
            physicalReadSchema.toJson(),
            dataPath.toUri().toString());
    }

    @Override
    public StructType getPhysicalDataReadSchema(
            TableClient tableClient, Row scanState, Row scanFile) {
        // Physical equivalent of the logical read schema.
        StructType physicalReadSchema = ScanStateRow.getPhysicalSchema(tableClient, scanState);

        // Compute the physical data read schema, basically the list of columns to read
        // from a Parquet data file. It should exclude partition columns and include
        // row_index metadata columns (in case DVs are present)
        List<String> partitionColumns = VectorUtils.toJavaList(metadata.getPartitionColumns());
        StructType physicalDataReadSchema =
            PartitionUtils.physicalSchemaWithoutPartitionColumns(
                readSchema, /* logical read schema */
                physicalReadSchema,
                new HashSet<>(partitionColumns)
            );

        if (protocol.getReaderFeatures().contains("deletionVectors")) {
            physicalDataReadSchema = physicalDataReadSchema
                .add(StructField.METADATA_ROW_INDEX_COLUMN);
        }

        if (protocol.getReaderFeatures().contains("typeWidening-dev")) {
            // Use the history of type changes recorded in the table schema to adjust the types in
            // read schema to match the types used to write the parquet file at table version
            // `defaultRowCommitVersion`.
            // This somewhat error-prone: the type change metadata must be accurate and we must
            // correctly infer the schema used to write the files from that information.
            //
            // Instead, we can also just read the schema from the parquet footer. Easier but slower.
            physicalDataReadSchema = getSchemaAtVersion(
                physicalDataReadSchema,
                InternalScanFileUtils.getDefaultRowCommitVersion(scanFile));
        }

        return physicalDataReadSchema;
    }

    /**
     * Adjust the given read schema to take into account type changes that were applied to the
     * table in the past. The resulting schema can be used to read files written at the given table
     * version.
     */
    private StructType getSchemaAtVersion(StructType readSchema, long version) {
        // TODO: Handle nested fields
        return new StructType(readSchema.fields().stream().map(field -> {
            FieldMetadata fieldMetadata = metadata.getSchema().get(field.getName()).getMetadata();
            if (!fieldMetadata.contains("delta.typeChanges")) {
                // No recorded type change for this field, use it as is.
                return field;
            }

            List<FieldMetadata> typeChanges =
                Arrays.asList((FieldMetadata[]) field.getMetadata().get("delta.typeChanges"));
            if (typeChanges.isEmpty()) {
                throw new IllegalStateException(
                    "Field " + field.getName() + " has type widening metadata with no type " +
                    "changes. This should never happen.");
            }

            // Sort type changes for this by version and find the type this field had at the given
            // table version.
            typeChanges.sort(Comparator.comparingLong(f -> (long) f.get("tableVersion")));
            DataType actualType = DataType.fromSQL((String) typeChanges.get(0).get("fromType"));

            for (FieldMetadata typeChange : typeChanges) {
                if ((long) typeChange.get("tableVersion") <= version) {
                    actualType = DataType.fromSQL((String) typeChange.get("toType"));
                }
            }
            return new StructField(
                    field.getName(), actualType, field.isNullable(), field.getMetadata());
        }).collect(Collectors.toList()));
    }

    @Override
    public Optional<Predicate> getRemainingFilter() {
        return getDataFilters();
    }

    private Optional<Tuple2<Predicate, Predicate>> splitFilters(Optional<Predicate> filter) {
        return filter.map(predicate ->
            PartitionUtils.splitMetadataAndDataPredicates(
                predicate, metadata.getPartitionColNames()));
    }

    private Optional<Predicate> getDataFilters() {
        return removeAlwaysTrue(partitionAndDataFilters.map(filters -> filters._2));
    }

    private Optional<Predicate> getPartitionsFilters() {
        return removeAlwaysTrue(partitionAndDataFilters.map(filters -> filters._1));
    }

    /**
     * Consider `ALWAYS_TRUE` as no predicate.
     */
    private Optional<Predicate> removeAlwaysTrue(Optional<Predicate> predicate) {
        return predicate
            .filter(filter -> !filter.getName().equalsIgnoreCase("ALWAYS_TRUE"));
    }

    private CloseableIterator<FilteredColumnarBatch> applyPartitionPruning(
        TableClient tableClient,
        CloseableIterator<FilteredColumnarBatch> scanFileIter) {
        Optional<Predicate> partitionPredicate = getPartitionsFilters();
        if (!partitionPredicate.isPresent()) {
            // There is no partition filter, return the scan file iterator as is.
            return scanFileIter;
        }

        Predicate predicateOnScanFileBatch = rewritePartitionPredicateOnScanFileSchema(
                partitionPredicate.get(),
                partitionColToStructFieldMap.get());

        return new CloseableIterator<FilteredColumnarBatch>() {
            PredicateEvaluator predicateEvaluator = null;

            @Override
            public boolean hasNext() {
                return scanFileIter.hasNext();
            }

            @Override
            public FilteredColumnarBatch next() {
                FilteredColumnarBatch next = scanFileIter.next();
                if (predicateEvaluator == null) {
                    predicateEvaluator =
                        tableClient.getExpressionHandler().getPredicateEvaluator(
                            next.getData().getSchema(),
                            predicateOnScanFileBatch);
                }
                ColumnVector newSelectionVector = predicateEvaluator.eval(
                    next.getData(),
                    next.getSelectionVector());
                return new FilteredColumnarBatch(
                    next.getData(),
                    Optional.of(newSelectionVector));
            }

            @Override
            public void close() throws IOException {
                scanFileIter.close();
            }
        };
    }

    private Optional<DataSkippingPredicate> getDataSkippingFilter() {
        return getDataFilters().flatMap(dataFilters ->
            DataSkippingUtils.constructDataSkippingFilter(dataFilters, metadata.getDataSchema())
        );
    }

    private CloseableIterator<FilteredColumnarBatch> applyDataSkipping(
            TableClient tableClient,
            CloseableIterator<FilteredColumnarBatch> scanFileIter,
            DataSkippingPredicate dataSkippingFilter) {
        // Get the stats schema
        // It's possible to instead provide the referenced columns when building the schema but
        // pruning it after is much simpler
        StructType prunedStatsSchema = DataSkippingUtils.pruneStatsSchema(
            getStatsSchema(metadata.getDataSchema()),
            dataSkippingFilter.getReferencedCols());

        // Skipping happens in two steps:
        // 1. The predicate produces false for any file whose stats prove we can safely skip it. A
        //    value of true means the stats say we must keep the file, and null means we could not
        //    determine whether the file is safe to skip, because its stats were missing/null.
        // 2. The coalesce(skip, true) converts null (= keep) to true
        Predicate filterToEval = new Predicate(
            "=",
            new ScalarExpression(
                "COALESCE",
                Arrays.asList(dataSkippingFilter, Literal.ofBoolean(true))),
            AlwaysTrue.ALWAYS_TRUE);

        PredicateEvaluator predicateEvaluator = tableClient
            .getExpressionHandler()
            .getPredicateEvaluator(prunedStatsSchema, filterToEval);

        return scanFileIter.map(filteredScanFileBatch -> {

            ColumnVector newSelectionVector = predicateEvaluator.eval(
                DataSkippingUtils.parseJsonStats(
                    tableClient,
                    filteredScanFileBatch,
                    prunedStatsSchema
                ),
                filteredScanFileBatch.getSelectionVector());

            return new FilteredColumnarBatch(
                filteredScanFileBatch.getData(),
                Optional.of(newSelectionVector));
            }
        );
    }
}
