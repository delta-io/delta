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
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;

import io.delta.kernel.Scan;
import io.delta.kernel.client.TableClient;
import io.delta.kernel.data.*;
import io.delta.kernel.expressions.*;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
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

    private final Optional<Predicate> filter;

    private boolean accessedScanFiles;

    private boolean areFiltersSplit;
    // Subset of partition predicate the expression handler can support evaluating
    private Optional<Predicate> metadataPredicate;
    // Subset of the given query predicate the Kernel tries to use prune scan file list as best as
    // it can, but can't guarantee that the all the scan files returned contains data on which this
    // predicate evaluates to true. The connector needs to apply this filter on the data from the
    // returned scan files to completely remove the data that doesn't satisfy given query predicate.
    //
    // The predicate could be on the data columns and/or unsupported predicate on partition columns
    private Optional<Predicate> remainingPredicate;

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
        this.filter = filter;
        this.dataPath = dataPath;
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
        Optional<DataSkippingPredicate> dataSkippingFilter = getDataSkippingFilter(tableClient);
        boolean shouldReadStats = dataSkippingFilter.isPresent();

        // Get active AddFiles via log replay
        CloseableIterator<FilteredColumnarBatch> scanFileIter = logReplay
            .getAddFilesAsColumnarBatches(shouldReadStats);

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

        return ScanStateRow.of(
            metadata,
            protocol,
            readSchema.toJson(),
            physicalReadSchema.toJson(),
            physicalDataReadSchema.toJson(),
            dataPath.toUri().toString());
    }

    @Override
    public Optional<Predicate> getRemainingFilter(TableClient tableClient) {
        splitFilters(tableClient);
        return remainingPredicate;
    }

    private void splitFilters(TableClient tableClient) {
        if (areFiltersSplit) {
            return;
        }
        filter.map(predicate -> {
            Tuple2<Predicate, Predicate> metadataAndNonMetadataFilters =
                PartitionUtils.splitPredicates(
                    tableClient.getExpressionHandler(),
                    metadata.getSchema(),
                    predicate,
                    metadata.getPartitionColNames());

            metadataPredicate = removeAlwaysTrue(Optional.of(metadataAndNonMetadataFilters._1));
            remainingPredicate = removeAlwaysTrue(Optional.of(metadataAndNonMetadataFilters._2));
            return null;
        });
        areFiltersSplit = true;
    }

    private Optional<Predicate> getDataFilters(TableClient tableClient) {
        splitFilters(tableClient);
        return remainingPredicate;
    }

    private Optional<Predicate> getPartitionsFilters(TableClient tableClient) {
        splitFilters(tableClient);
        return metadataPredicate;
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
        Optional<Predicate> partitionPredicate = getPartitionsFilters(tableClient);
        if (!partitionPredicate.isPresent()) {
            // There is no partition filter, return the scan file iterator as is.
            return scanFileIter;
        }

        Set<String> partitionColNames = metadata.getPartitionColNames();
        Map<String, StructField> partitionColNameToStructFieldMap =
            metadata.getSchema().fields().stream()
                .filter(field -> partitionColNames.contains(field.getName()))
                .collect(toMap(
                    field -> field.getName().toLowerCase(Locale.ENGLISH),
                    identity()));

        Predicate predicateOnScanFileBatch = rewritePartitionPredicateOnScanFileSchema(
            partitionPredicate.get(),
            partitionColNameToStructFieldMap);

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

    private Optional<DataSkippingPredicate> getDataSkippingFilter(TableClient tableClient) {
        return getDataFilters(tableClient).flatMap(dataFilters ->
            DataSkippingUtils.constructDataSkippingFilter(dataFilters, metadata.getSchema())
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
