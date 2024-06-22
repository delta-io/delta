package org.utils.job.sql;

import java.util.UUID;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.utils.ConsoleSink;
import org.utils.Utils;

public abstract class ContinuousSqlSourceExampleBase extends SqlExampleBase {

    private final String workPath = Utils.resolveExampleTableAbsolutePath("example_table_" +
        UUID.randomUUID().toString().split("-")[0]);

    protected final boolean isStreaming;

    protected ContinuousSqlSourceExampleBase() {
        this.isStreaming = true;
    }

    public void run(String tablePath) throws Exception {
        System.out.println("Will use table path: " + tablePath);
        Utils.prepareDirs(tablePath, workPath);

        StreamExecutionEnvironment streamEnv = createTestStreamEnv(this.isStreaming);
        StreamTableEnvironment tableEnv = createTableStreamingEnv(streamEnv);
        Table table = runSqlJob(workPath, tableEnv);
        tableEnv.toDataStream(table)
            .map(new RowMapperFunction(Utils.FULL_SCHEMA_ROW_TYPE))
            .addSink(new ConsoleSink(Utils.FULL_SCHEMA_ROW_TYPE))
            .setParallelism(1);
        streamEnv.executeAsync();

        Utils.runSourceTableUpdater(workPath);
    }
}
