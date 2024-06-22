package org.example.sql;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

import io.delta.flink.source.DeltaSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.hadoop.conf.Configuration;
import org.utils.Utils;
import static org.utils.job.sql.SqlExampleBase.createTableStreamingEnv;
import static org.utils.job.sql.SqlExampleBase.createTestStreamEnv;

/**
 * This is an example of using Delta Connector both in Streaming and Table API. In this example a
 * Delta Source will be created using Streaming API and will be registered as Flink table. Next we
 * will use Flink SQL to read data from it using SELECT statement and write back to newly created
 * Delta table defined by CREATE TABLE statement.
 */
public class StreamingApiDeltaSourceToTableDeltaSinkJob {

    private static final String SOURCE_TABLE_PATH = Utils.resolveExampleTableAbsolutePath(
        "data/source_table_no_partitions");

    private static final String SINK_TABLE_PATH = Utils.resolveExampleTableAbsolutePath(
        "example_streamingToTableAPI_table_" + UUID.randomUUID().toString().split("-")[0]);

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment streamEnv = createTestStreamEnv(false); // isStreaming = false
        StreamTableEnvironment tableEnv = createTableStreamingEnv(streamEnv);
        createPipeline(streamEnv, tableEnv);
    }

    private static void createPipeline(
            StreamExecutionEnvironment streamEnv,
            StreamTableEnvironment tableEnv) throws Exception {

        // Set up a Delta Source using Flink's Streaming API.
        DeltaSource<RowData> deltaSource = DeltaSource.forBoundedRowData(
            new Path(SOURCE_TABLE_PATH),
            new Configuration()
        ).build();

        // create a source stream from Delta Source connector.
        DataStreamSource<RowData> sourceStream =
            streamEnv.fromSource(deltaSource, WatermarkStrategy.noWatermarks(), "delta-source");

        // setup Delta Catalog
        tableEnv.executeSql("CREATE CATALOG myDeltaCatalog WITH ('type' = 'delta-catalog')");
        tableEnv.executeSql("USE CATALOG myDeltaCatalog");

        // Convert source stream into Flink's table and register it as temporary view under
        // "InputTable" name.
        Table sourceTable = tableEnv.fromDataStream(sourceStream);
        tableEnv.createTemporaryView("InputTable", sourceTable);

        // Create Sink Delta table using Flink SQL API.
        tableEnv.executeSql(String.format(""
                + "CREATE TABLE sinkTable ("
                + "f1 STRING,"
                + "f2 STRING,"
                + "f3 INT"
                + ") WITH ("
                + " 'connector' = 'delta',"
                + " 'table-path' = '%s'"
                + ")",
            SINK_TABLE_PATH)
        );

        // Insert into sinkTable all rows read by Delta Source that is registered as "InputTable"
        // view.
        tableEnv.executeSql("INSERT INTO sinkTable SELECT * FROM InputTable")
            .await(10, TimeUnit.SECONDS);

        // Read and print all rows from sinkTable using Flink SQL.
        tableEnv.executeSql("SELECT * FROM sinkTable").print();
    }
}
