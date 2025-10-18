/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.delta.flink.internal.table;

import java.util.LinkedHashMap;
import java.util.Map;

import io.delta.flink.internal.options.DeltaConnectorConfiguration;
import io.delta.flink.sink.DeltaSink;
import io.delta.flink.sink.internal.DeltaBucketAssigner;
import io.delta.flink.sink.internal.DeltaPartitionComputer.DeltaRowDataPartitionComputer;
import io.delta.flink.sink.internal.DeltaSinkBuilder;
import io.delta.flink.source.internal.builder.RowDataFormat;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.row.ParquetRowDataBuilder;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.BasePathBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkProvider;
import org.apache.flink.table.connector.sink.abilities.SupportsPartitioning;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.hadoop.conf.Configuration;

/**
 * Sink of a dynamic Flink table to a Delta lake table.
 *
 * <p>
 * It utilizes new Flink Sink API (available for {@code Flink >= 1.12}) and interfaces (available
 * for {@code Flink >= 1.13}) provided for interoperability between this new Sink API and Table API.
 * It also supports static partitioning.
 *
 * <p>
 * For regular batch scenarios, the sink can solely accept insert-only rows and write out bounded
 * streams.
 *
 * <p>
 * For regular streaming scenarios, the sink can solely accept insert-only rows and can write out
 * unbounded streams.
 */
public class DeltaDynamicTableSink implements DynamicTableSink, SupportsPartitioning {

    /**
     * Hardcoded option for {@link RowDataFormat} to threat timestamps as a UTC timestamps.
     */
    private static final boolean PARQUET_UTC_TIMESTAMP = true;

    /**
     * The Delta's mergeSchema option is not supported in Flink SQL/Table API due to fact that
     * Flink's table planner validates every query against table schema.
     * If query schema does not match table's schema the query will fail.
     */
    private static final boolean MERGE_SCHEMA = false;

    private final Path basePath;

    private final Configuration hadoopConf;

    private final RowType rowType;

    private final CatalogTable catalogTable;

    /**
     * Flink is providing the connector with the partition values derived from the PARTITION
     * clause, e.g.
     * <pre>
     * INSERT INTO x PARTITION(col1='val1") ...
     * </pre>
     * Those partition values will be populated to this map via {@link #applyStaticPartition(Map)}
     */
    private LinkedHashMap<String, String> staticPartitionSpec;

    /**
     * Constructor for creating sink of Flink dynamic table to Delta table.
     *
     * @param basePath     full Delta table path
     * @param hadoopConf   Hadoop's configuration
     * @param rowType      Flink's logical type with the structure of the events in the stream
     * @param catalogTable represents the unresolved metadata of derived by Flink framework from
     *                     table's DDL
     */
    public DeltaDynamicTableSink(
            Path basePath,
            Configuration hadoopConf,
            RowType rowType,
            CatalogTable catalogTable) {

        this(basePath, hadoopConf, rowType, catalogTable, new LinkedHashMap<>());
    }

    private DeltaDynamicTableSink(
            Path basePath,
            Configuration hadoopConf,
            RowType rowType,
            CatalogTable catalogTable,
            LinkedHashMap<String, String> staticPartitionSpec) {

        this.basePath = basePath;
        this.rowType = rowType;
        this.hadoopConf = hadoopConf;
        this.catalogTable = catalogTable;
        this.staticPartitionSpec = staticPartitionSpec;
    }

    /**
     * Returns the set of changes that the sink accepts during runtime.
     *
     * @param requestedMode expected set of changes by the current plan
     * @return {@link ChangelogMode} only allowing for inserts to the Delta table
     */
    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        return ChangelogMode.insertOnly();
    }

    /**
     * Utility method for transition from Flink's DataStream to Table API.
     *
     * @param context Context for creating runtime implementation via a {@link
     *                SinkRuntimeProvider}.
     * @return provider representing {@link DeltaSink} implementation for writing the data to a
     * Delta table.
     */
    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {

        DeltaSinkBuilder<RowData> builder =
            new DeltaSinkBuilder.DefaultDeltaFormatBuilder<>(
                this.basePath,
                this.hadoopConf,
                ParquetRowDataBuilder.createWriterFactory(
                    this.rowType,
                    this.hadoopConf,
                    PARQUET_UTC_TIMESTAMP
                ),
                new BasePathBucketAssigner<>(),
                OnCheckpointRollingPolicy.build(),
                this.rowType,
                MERGE_SCHEMA, // mergeSchema = false
                new DeltaConnectorConfiguration()
            );

        if (catalogTable.isPartitioned()) {
            DeltaRowDataPartitionComputer partitionComputer =
                new DeltaRowDataPartitionComputer(
                    rowType,
                    catalogTable.getPartitionKeys().toArray(new String[0]),
                    staticPartitionSpec
                );
            DeltaBucketAssigner<RowData> partitionAssigner =
                new DeltaBucketAssigner<>(partitionComputer);

            builder.withBucketAssigner(partitionAssigner);
        }

        return SinkProvider.of(builder.build());
    }

    @Override
    public DynamicTableSink copy() {
        return new DeltaDynamicTableSink(
            this.basePath,
            this.hadoopConf,
            this.rowType,
            this.catalogTable,
            new LinkedHashMap<>(this.staticPartitionSpec));
    }

    @Override
    public String asSummaryString() {
        return "DeltaSink";
    }

    /**
     * Static values for partitions that should set explicitly instead of being derived from the
     * content of the records.
     *
     * <p>
     * If all partition keys get a value assigned in the {@code PARTITION} clause, the operation
     * is considered an "insertion into a static partition". In the below example, the query result
     * should be written into the static partition {@code region='europe', month='2020-01'} which
     * will be passed by the planner into {@code applyStaticPartition(Map)}.
     *
     * <pre>
     * INSERT INTO t PARTITION (region='europe', month='2020-01') SELECT a, b, c FROM my_view;
     * </pre>
     *
     * <p>If only a subset of all partition keys get a static value assigned in the {@code
     * PARTITION} clause or with a constant part in a {@code SELECT} clause, the operation is
     * considered an "insertion into a dynamic partition". In the below example, the static
     * partition part is {@code region='europe'} which will be passed by the planner into {@code
     * #applyStaticPartition(Map)}. The remaining values for partition keys should be obtained from
     * each individual record by the sink during runtime.
     *
     * <pre>
     * INSERT INTO t PARTITION (region='europe') SELECT a, b, c, month FROM another_view;
     * </pre>
     *
     * @param partition map of static partitions and their values.
     */
    @Override
    public void applyStaticPartition(Map<String, String> partition) {
        // make it a LinkedHashMap to maintain partition column order
        LinkedHashMap<String, String> staticPartitions = new LinkedHashMap<>();

        for (String partitionCol : catalogTable.getPartitionKeys()) {
            if (partition.containsKey(partitionCol)) {
                staticPartitions.put(partitionCol, partition.get(partitionCol));
            }
        }

        this.staticPartitionSpec = staticPartitions;
    }

    @VisibleForTesting
    Configuration getHadoopConf() {
        return new Configuration(hadoopConf);
    }
}
