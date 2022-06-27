package org.utils.job.continuous;

import io.delta.flink.source.DeltaSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.utils.Utils;
import org.utils.job.DeltaExampleLocalJobRunner;

public abstract class DeltaContinuousSourceLocalJobExampleBase
    implements DeltaExampleLocalJobRunner {

    private final String workPath = Utils.resolveExampleTableAbsolutePath("example_table");

    @Override
    public void run(String tablePath) throws Exception {
        System.out.println("Will use table from path: " + tablePath);

        Utils.prepareDirs(tablePath, workPath);
        StreamExecutionEnvironment env = createPipeline(workPath, 2, 3);
        runFlinkJobInBackground(env);
        Utils.runSourceTableUpdater(workPath);
    }

    public abstract DeltaSource<RowData> getDeltaSource(String tablePath);
}
