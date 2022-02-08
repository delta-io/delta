package io.delta.flink.example.sink;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Internal class providing utility methods to run local Flink job in memory.
 */
public interface DeltaSinkLocalJobRunner {

    default MiniCluster getMiniCluster() {
        final org.apache.flink.configuration.Configuration config = new org.apache.flink.configuration.Configuration();
        config.setString(RestOptions.BIND_PORT, "18081-19000");
        final MiniClusterConfiguration cfg =
                new MiniClusterConfiguration.Builder()
                        .setNumTaskManagers(2)
                        .setNumSlotsPerTaskManager(4)
                        .setConfiguration(config)
                        .build();
        return new MiniCluster(cfg);
    }

    default void runFlinkJobInBackground(StreamExecutionEnvironment env) {
        new Thread(() -> {
            try (MiniCluster miniCluster = getMiniCluster()) {
                miniCluster.start();
                miniCluster.executeJobBlocking(env.getStreamGraph().getJobGraph());
            } catch (Exception e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        }).start();
    }

    default StreamExecutionEnvironment getStreamExecutionEnvironment() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.enableCheckpointing(2000, CheckpointingMode.EXACTLY_ONCE);
        return env;
    }
}
