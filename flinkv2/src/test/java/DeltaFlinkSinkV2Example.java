import com.fasterxml.jackson.databind.type.LogicalType;
import io.delta.kernel.internal.data.GenericRow;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.hadoop.shaded.org.apache.avro.generic.GenericArray;

import java.util.ArrayList;
import java.util.Arrays;

public class DeltaFlinkSinkV2Example {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        final String tablePath = "/Users/scott.sandre/tmp/flink_delta_tables/table_" + java.util.UUID.randomUUID().toString().replace("-", "_");

        final RowType rowType = new RowType(Arrays.asList(
            new RowType.RowField("col1", new IntType())
        ));

        final DataStream<RowData> inputStream = env.fromData(
            createSimpleRow(0),
            createSimpleRow(1),
            createSimpleRow(2),
            createSimpleRow(3)
        );

        final Sink<RowData> deltaSink = io.delta.flinkv2.sink.DeltaSink.forRowData(tablePath, rowType, new ArrayList<>());

        inputStream.sinkTo(deltaSink);

        env.execute("Delta Sink Example");
    }

    private static RowData createSimpleRow(int value) {
        final GenericRowData row = new GenericRowData(1);
        row.setField(0, value);
        return row;
    }
}
