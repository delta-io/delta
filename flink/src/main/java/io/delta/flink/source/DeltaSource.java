package io.delta.flink.source;

import io.delta.flink.source.internal.DeltaSourceConfiguration;
import io.delta.flink.source.internal.DeltaSourceInternal;
import io.delta.flink.source.internal.enumerator.SplitEnumeratorProvider;
import io.delta.flink.source.internal.enumerator.supplier.BoundedSnapshotSupplierFactory;
import io.delta.flink.source.internal.enumerator.supplier.ContinuousSnapshotSupplierFactory;
import io.delta.flink.source.internal.state.DeltaSourceSplit;
import org.apache.flink.connector.file.src.reader.BulkFormat;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.data.RowData;
import org.apache.hadoop.conf.Configuration;

/**
 * A unified data source that reads Delta table - both in batch and in streaming mode.
 *
 * <p>This source supports all (distributed) file systems and object stores that can be accessed
 * via the Flink's {@link FileSystem} class.
 * <p>
 * To create a new instance of {@link DeltaSource} for a Delta table that will produce
 * {@link RowData} records that contain all table columns:
 * <pre>
 *     StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
 *     ...
 *     // Bounded mode.
 *     DeltaSource&lt;RowData&gt; deltaSource = DeltaSource.forBoundedRowData(
 *                new Path("s3://some/path"),
 *                new Configuration()
 *             )
 *             .versionAsOf(10)
 *             .build();
 *
 *     env.fromSource(deltaSource, WatermarkStrategy.noWatermarks(), "delta-source")
 *
 *     ..........
 *     // Continuous mode.
 *     DeltaSource&lt;RowData&gt; deltaSource = DeltaSource.forContinuousRowData(
 *                new Path("s3://some/path"),
 *                new Configuration()
 *               )
 *              .updateCheckIntervalMillis(1000)
 *              .startingVersion(10)
 *              .build();
 *
 *     env.fromSource(deltaSource, WatermarkStrategy.noWatermarks(), "delta-source")
 * </pre>
 * <p>
 * To create a new instance of {@link DeltaSource} for a Delta table that will produce
 * {@link RowData} records with user-selected columns:
 * <pre>
 *     StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
 *     ...
 *     // Bounded mode.
 *     DeltaSource&lt;RowData&gt; deltaSource = DeltaSource.forBoundedRowData(
 *                new Path("s3://some/path"),
 *                new Configuration()
 *             )
 *             .columnNames(Arrays.asList("col1", "col2"))
 *             .versionAsOf(10)
 *             .build();
 *
 *     env.fromSource(deltaSource, WatermarkStrategy.noWatermarks(), "delta-source")
 *
 *     ..........
 *     // Continuous mode.
 *     DeltaSource&lt;RowData&gt; deltaSource = DeltaSource.forContinuousRowData(
 *                new Path("s3://some/path"),
 *                new Configuration()
 *               )
 *               .columnNames(Arrays.asList("col1", "col2"))
 *               .updateCheckIntervalMillis(1000)
 *               .startingVersion(10)
 *               .build();
 *
 *     env.fromSource(deltaSource, WatermarkStrategy.noWatermarks(), "delta-source")
 * </pre>
 * When using {@code columnNames(...)} method, the source will discover the data types for the
 * given columns from the Delta log.
 *
 * @param <T> The type of the events/records produced by this source.
 * @implNote <h2>Batch and Streaming</h2>
 *
 * <p>This source supports both bounded/batch and continuous/streaming modes. For the
 * bounded/batch case, the Delta Source processes the full state of the Delta table. In
 * the continuous/streaming case, the default Delta Source will also process the full state of the
 * table, and then begin to periodically check the Delta table for any appending changes and read
 * them. Using either of the {@link RowDataContinuousDeltaSourceBuilder#startingVersion} or
 * {@link RowDataContinuousDeltaSourceBuilder#startingTimestamp} APIs will cause the Delta Source,
 * in continuous mode, to stream only the changes from that historical version.
 *
 * <h2>Format Types</h2>
 *
 * <p>The reading of each file happens through file readers defined by <i>file format</i>. These
 * define the parsing logic for the contents of the underlying Parquet files.
 *
 * <p>A {@link BulkFormat} reads batches of records from a file at a time.
 * @implNote <h2>Discovering / Enumerating Files</h2>
 * <p>The way that the source lists the files to be processes is defined by the {@code
 * AddFileEnumerator}. The {@code AddFileEnumerator} is responsible to select the relevant {@code
 * AddFile} and to optionally splits files into multiple regions (file source splits) that can be
 * read in parallel.
 */
public class DeltaSource<T> extends DeltaSourceInternal<T> {

    DeltaSource(
            Path tablePath,
            BulkFormat<T, DeltaSourceSplit> readerFormat,
            SplitEnumeratorProvider splitEnumeratorProvider,
            Configuration configuration,
            DeltaSourceConfiguration sourceConfiguration) {
        super(tablePath, readerFormat, splitEnumeratorProvider, configuration, sourceConfiguration);
    }

    /**
     * Creates an instance of Delta source builder for Bounded mode and for {@code RowData}
     * elements.
     * @param tablePath Path to Delta table to read data from.
     * @param hadoopConfiguration Hadoop configuration.
     */
    public static RowDataBoundedDeltaSourceBuilder forBoundedRowData(
            Path tablePath,
            Configuration hadoopConfiguration) {

        return new RowDataBoundedDeltaSourceBuilder(
            tablePath,
            hadoopConfiguration,
            new BoundedSnapshotSupplierFactory());
    }

    /**
     * Creates an instance of Delta source builder for Continuous mode and for {@code RowData}
     * elements.
     * @param tablePath Path to Delta table to read data from.
     * @param hadoopConfiguration Hadoop configuration.
     */
    public static RowDataContinuousDeltaSourceBuilder forContinuousRowData(
            Path tablePath,
            Configuration hadoopConfiguration) {

        return new RowDataContinuousDeltaSourceBuilder(
                tablePath,
                hadoopConfiguration,
                new ContinuousSnapshotSupplierFactory());
    }
}
