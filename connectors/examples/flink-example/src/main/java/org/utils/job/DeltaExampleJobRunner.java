package org.utils.job;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public interface DeltaExampleJobRunner {

    default StreamExecutionEnvironment getStreamExecutionEnvironment() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        env.enableCheckpointing(2000, CheckpointingMode.EXACTLY_ONCE);
        return env;
    }

    void run(String tablePath) throws Exception;

    StreamExecutionEnvironment createPipeline(
        String tablePath,
        int sourceParallelism,
        int sinkParallelism
    );
}
