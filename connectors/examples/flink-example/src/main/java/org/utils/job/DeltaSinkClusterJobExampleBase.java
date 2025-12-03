package org.utils.job;

import io.delta.flink.sink.DeltaSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.utils.Utils;

public abstract class DeltaSinkClusterJobExampleBase implements DeltaExampleJobRunner {

    @Override
    public void run(String tablePath) throws Exception {
        System.out.println("Will use table path: " + tablePath);
        Utils.prepareDirs(tablePath);
        StreamExecutionEnvironment env = createPipeline(tablePath, 1, 1);
        env.execute("TestJob");
    }

    public abstract DeltaSink<RowData> getDeltaSink(String tablePath);

}
