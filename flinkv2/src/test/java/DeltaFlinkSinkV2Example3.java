import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

public class DeltaFlinkSinkV2Example3 {
    private static final Logger LOG = LoggerFactory.getLogger(DeltaFlinkSinkV2Example3.class);

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.enableCheckpointing(500, CheckpointingMode.EXACTLY_ONCE);

        final String tablePath = "/Users/scott.sandre/tmp/flink_delta_tables/table_" + java.util.UUID.randomUUID().toString().replace("-", "_");

        final RowType rowType = new RowType(Arrays.asList(
            new RowType.RowField("part1", new IntType()),
            new RowType.RowField("part2", new IntType()),
            new RowType.RowField("col3", new IntType()),
            new RowType.RowField("col4", new IntType())
        ));

        final Sink<RowData> deltaSink = io.delta.flinkv2.sink.DeltaSink.forRowData(tablePath, rowType, Arrays.asList("part1", "part2"));

        GeneratorFunction<Long, RowData> generatorFunction = x -> {
            try {
                // Introduce a small delay to control the rate of data generation
                Thread.sleep(30);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }

            System.out.println("generator is yielding value" + x);
            return createSimpleRow(x.intValue());
        };

        DataGeneratorSource<RowData> source = new DataGeneratorSource<>(generatorFunction, 1000, Types.GENERIC(RowData.class));

        DataStreamSource<RowData> stream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Generator Source");

        stream.sinkTo(deltaSink); //.setParallelism(3).setMaxParallelism(3);

        LOG.info(String.format("AAA Stream parallelism: %s", stream.getParallelism()));
        System.out.println(String.format("Stream parallelism: %s", stream.getParallelism()));
        env.execute("Delta Sink Example");
    }

    private static RowData createSimpleRow(int value) {
        final GenericRowData row = new GenericRowData(4);
        row.setField(0, value % 2);
        row.setField(1, value % 5);
        row.setField(2, value);
        row.setField(3, value);
        return row;
    }

}
