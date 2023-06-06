package org.example.source.continuous;

import io.delta.flink.source.DeltaSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.hadoop.conf.Configuration;
import org.utils.ConsoleSink;
import org.utils.Utils;
import org.utils.job.continuous.DeltaContinuousSourceLocalJobExampleBase;

/**
 * Demonstrates how the Flink Delta source can be used to read data from Delta table.
 * <p>
 * If you run this example then application will spawn example local Flink streaming job that will
 * read changes from Delta table placed under "src/main/resources/data/source_table_no_partitions"
 * starting from version specified by user via {@code .startingVersion(long)} method.
 * After that, source connector will start to actively monitor this table for any new changes.
 * Read records will be printed to log using custom Sink Function.
 */
public class DeltaContinuousSourceStartingVersionExample extends
    DeltaContinuousSourceLocalJobExampleBase {

    private static final String TABLE_PATH =
        Utils.resolveExampleTableAbsolutePath("data/source_table_no_partitions");

    public static void main(String[] args) throws Exception {
        new DeltaContinuousSourceStartingVersionExample().run(TABLE_PATH);
    }

    /**
     * An example of using Flink Delta Source in streaming pipeline.
     */
    @Override
    public StreamExecutionEnvironment createPipeline(
            String tablePath,
            int sourceParallelism,
            int sinkParallelism) {

        DeltaSource<RowData> deltaSink = getDeltaSource(tablePath);
        StreamExecutionEnvironment env = getStreamExecutionEnvironment();

        env
            .fromSource(deltaSink, WatermarkStrategy.noWatermarks(), "continuous-delta-source")
            .setParallelism(sourceParallelism)
            .addSink(new ConsoleSink(Utils.FULL_SCHEMA_ROW_TYPE))
            .setParallelism(1);

        return env;
    }

    /**
     * An example of Flink Delta Source configuration that will read all columns from Delta table.
     * This source will read only changes added to the table starting from version specified via
     * {@code .startingVersion(long)} method.
     * Alternatively the {@code .option("startingVersion", Long)} or {@code .option
     * ("startingVersion", String)} options can be used.
     * <p>
     * The {@code .forContinuousRowData(...)} creates Delta Flink source that will monitor
     * delta table for any new changes.
     */
    @Override
    public DeltaSource<RowData> getDeltaSource(String tablePath) {
        return DeltaSource.forContinuousRowData(
                new Path(tablePath),
                new Configuration()
            )
            .startingVersion(10)
            .build();
    }
}
