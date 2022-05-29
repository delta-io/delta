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

import io.delta.standalone.actions.AddFile;

/**
 * A unified data source that reads Delta table - both in batch and in streaming mode.
 *
 * <p>This source supports all (distributed) file systems and object stores that can be accessed
 * via the Flink's {@link FileSystem} class.
 * <p>
 * To create a new instance of Delta source for a non-partitioned Delta table that will produce
 * {@link RowData} records that will contain all columns from Delta table:
 * <pre>
 *     StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
 *     ...
 *     // {@link org.apache.flink.api.connector.source.Boundedness#BOUNDED} mode.
 *     DeltaSource&lt;RowData&gt; deltaSink = DeltaSource.boundedRowDataSourceBuilder(
 *                new Path("s3://some/path"),
 *                new Configuration()
 *             )
 *             .versionAsOf(10)
 *             .build();
 *
 *     env.fromSource(source, WatermarkStrategy.noWatermarks(), "delta-source")
 *
 *     ..........
 *     // {@link org.apache.flink.api.connector.source.Boundedness#CONTINUOUS_UNBOUNDED} mode.
 *     DeltaSource&lt;RowData&gt; deltaSink = DeltaSource.continuousRowDataSourceBuilder(
 *                new Path("s3://some/path"),
 *                new Configuration()
 *               )
 *              .updateCheckIntervalMillis(1000)
 *              .startingVersion(10)
 *              .build();
 *
 *     env.fromSource(source, WatermarkStrategy.noWatermarks(), "delta-source")
 * </pre>
 * <p>
 * To create a new instance of Delta source for a non-partitioned Delta table that will produce
 * {@link RowData} records with user selected columns:
 * <pre>
 *     StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
 *     ...
 *     // {@link org.apache.flink.api.connector.source.Boundedness#BOUNDED} mode.
 *     DeltaSource&lt;RowData&gt; deltaSink = DeltaSource.boundedRowDataSourceBuilder(
 *                new Path("s3://some/path"),
 *                new Configuration()
 *             )
 *             .columnNames(Arrays.asList("col1", "col2"))
 *             .versionAsOf(10)
 *             .build();
 *
 *     env.fromSource(source, WatermarkStrategy.noWatermarks(), "delta-source")
 *
 *     ..........
 *     // {@link org.apache.flink.api.connector.source.Boundedness#CONTINUOUS_UNBOUNDED} mode.
 *     DeltaSource&lt;RowData&gt; deltaSink = DeltaSource.continuousRowDataSourceBuilder(
 *                new Path("s3://some/path"),
 *                new Configuration()
 *               )
 *               .columnNames(Arrays.asList("col1", "col2"))
 *               .updateCheckIntervalMillis(1000)
 *               .startingVersion(10)
 *               .build();
 *
 *     env.fromSource(source, WatermarkStrategy.noWatermarks(), "delta-source")
 * </pre>
 * When using {@code columnNames(...)} method, the source will discover data types for defined
 * columns from Delta log.
 *
 * @param <T> The type of the events/records produced by this source.
 * @implNote <h2>Batch and Streaming</h2>
 *
 * <p>This source supports both bounded/batch and continuous/streaming modes. For the
 * bounded/batch case, the Delta Source processes all {@link AddFile} from Delta table Snapshot. In
 * the continuous/streaming case, the source periodically checks the Delta Table for any appending
 * changes and reads them.
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

    public static RowDataBoundedDeltaSourceBuilder forBoundedRowData(
            Path tablePath,
            Configuration hadoopConfiguration) {

        return new RowDataBoundedDeltaSourceBuilder(
            tablePath,
            hadoopConfiguration,
            new BoundedSnapshotSupplierFactory());
    }

    public static RowDataContinuousDeltaSourceBuilder forContinuousRowData(
            Path tablePath,
            Configuration hadoopConfiguration) {

        return new RowDataContinuousDeltaSourceBuilder(
                tablePath,
                hadoopConfiguration,
                new ContinuousSnapshotSupplierFactory());
    }
}
