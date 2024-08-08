import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;

import java.util.ArrayList;
import java.util.Arrays;

public class DeltaFlinkSinkV2Example1 {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        final String tablePath = "/Users/scott.sandre/tmp/flink_delta_tables/table_" + java.util.UUID.randomUUID().toString().replace("-", "_");

        final RowType rowType = new RowType(Arrays.asList(
            new RowType.RowField("col1", new IntType())
        ));

        final Sink<RowData> deltaSink = io.delta.flinkv2.sink.DeltaSink.forRowData(tablePath, rowType, new ArrayList<>());

        final DataStream<RowData> inputStream = env.fromData(
            createSimpleRow(0),
            createSimpleRow(1),
            createSimpleRow(2),
            createSimpleRow(3)
        );
        inputStream.sinkTo(deltaSink); // .setParallelism(1).setMaxParallelism(1);
        env.execute("Delta Sink Example");
    }

    private static RowData createSimpleRow(int value) {
        final GenericRowData row = new GenericRowData(1);
        row.setField(0, value);
        return row;
    }
}
