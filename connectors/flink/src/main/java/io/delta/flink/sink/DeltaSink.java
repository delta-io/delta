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

package io.delta.flink.sink;

import io.delta.flink.sink.internal.DeltaSinkBuilder;
import io.delta.flink.sink.internal.DeltaSinkInternal;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.ParquetWriterFactory;
import org.apache.flink.formats.parquet.row.ParquetRowDataBuilder;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.hadoop.conf.Configuration;

import io.delta.standalone.DeltaLog;

/**
 * A unified sink that emits its input elements to file system files within buckets using
 * Parquet format and commits those files to the {@link DeltaLog}. This sink achieves exactly-once
 * semantics for both {@code BATCH} and {@code STREAMING}.
 * <p>
 * For most use cases users should use {@link DeltaSink#forRowData} utility method to instantiate
 * the sink which provides proper writer factory implementation for the stream of {@link RowData}.
 * <p>
 * To create new instance of the sink to a non-partitioned Delta table for stream of
 * {@link RowData}:
 * <pre>
 *     DataStream&lt;RowData&gt; stream = ...;
 *     RowType rowType = ...;
 *     ...
 *
 *     // sets a sink to a non-partitioned Delta table
 *     DeltaSink&lt;RowData&gt; deltaSink = DeltaSink.forRowData(
 *             new Path(deltaTablePath),
 *             new Configuration(),
 *             rowType).build();
 *     stream.sinkTo(deltaSink);
 * </pre>
 *
 * To create new instance of the sink to a partitioned Delta table for stream of {@link RowData}:
 * <pre>
 *     String[] partitionCols = ...; // array of partition columns' names
 *
 *     DeltaSink&lt;RowData&gt; deltaSink = DeltaSink.forRowData(
 *             new Path(deltaTablePath),
 *             new Configuration(),
 *             rowType)
 *         .withPartitionColumns(partitionCols)
 *         .build();
 *     stream.sinkTo(deltaSink);
 * </pre>
 * <p>
 * Behaviour of this sink splits down upon two phases. The first phase takes place between
 * application's checkpoints when records are being flushed to files (or appended to writers'
 * buffers) where the behaviour is almost identical as in case of
 * {@link org.apache.flink.connector.file.sink.FileSink}.
 * Next during the checkpoint phase files are "closed" (renamed) by the independent instances of
 * {@code io.delta.flink.sink.internal.committer.DeltaCommitter} that behave very similar
 * to {@link org.apache.flink.connector.file.sink.committer.FileCommitter}.
 * When all the parallel committers are done, then all the files are committed at once by
 * single-parallelism {@code io.delta.flink.sink.internal.committer.DeltaGlobalCommitter}.
 * <p>
 *
 * @param <IN> Type of the elements in the input of the sink that are also the elements to be
 *             written to its output
 */
public class DeltaSink<IN> extends DeltaSinkInternal<IN> {

    DeltaSink(DeltaSinkBuilder<IN> sinkBuilder) {
        super(sinkBuilder);
    }

    /**
     * Convenience method for creating a {@link RowDataDeltaSinkBuilder} for {@link DeltaSink} to a
     * Delta table.
     *
     * @param basePath root path of the Delta table
     * @param conf     Hadoop's conf object that will be used for creating instances of
     *                 {@link io.delta.standalone.DeltaLog} and will be also passed to the
     *                 {@link ParquetRowDataBuilder} to create {@link ParquetWriterFactory}
     * @param rowType  Flink's logical type to indicate the structure of the events in the stream
     * @return builder for the DeltaSink
     */
    public static RowDataDeltaSinkBuilder forRowData(
        final Path basePath,
        final Configuration conf,
        final RowType rowType
    ) {
        return new RowDataDeltaSinkBuilder(
            basePath,
            conf,
            rowType,
            false // mergeSchema
        );
    }
}
