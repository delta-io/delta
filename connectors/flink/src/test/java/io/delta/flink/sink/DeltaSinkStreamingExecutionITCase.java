/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.delta.flink.sink;

import java.io.IOException;
import java.util.List;

import io.delta.flink.sink.internal.committables.DeltaCommittable;
import io.delta.flink.sink.internal.committables.DeltaGlobalCommittable;
import io.delta.flink.sink.internal.writer.DeltaWriterBucketState;
import io.delta.flink.sink.utils.DeltaSinkTestUtils;
import io.delta.flink.utils.CheckpointCountingSource;
import io.delta.flink.utils.DeltaTestUtils;
import io.delta.flink.utils.TestParquetReader;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.rules.TemporaryFolder;
import static org.junit.jupiter.api.Assertions.assertEquals;

import io.delta.standalone.DeltaLog;
import io.delta.standalone.actions.AddFile;

/**
 * Integration tests for {@link io.delta.flink.sink.DeltaSink} in STREAMING mode.
 * <p>
 * Flink 2.0 NOTE: This is a simplified version of the original test. The original test
 * included complex failover scenarios with artificial exception injection into GlobalCommitter
 * (removed in Flink 2.0). This simplified version validates basic streaming write functionality
 * with checkpointing.
 * <p>
 * Future work: Add more sophisticated Flink 2.0-native failover tests using checkpoint listeners,
 * task manager failures, and network partition simulation.
 */
public class DeltaSinkStreamingExecutionITCase extends DeltaSinkExecutionITCaseBase {

    private static final int NUM_SOURCES = 2;
    private static final int NUM_SINKS = 2;
    private static final int NUM_RECORDS = 1000;
    private static final int RECORDS_PER_CHECKPOINT = 100;

    public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

    private String deltaTablePath;

    @BeforeAll
    public static void beforeAll() throws IOException {
        TEMPORARY_FOLDER.create();
    }

    @AfterAll
    public static void afterAll() {
        TEMPORARY_FOLDER.delete();
    }

    @BeforeEach
    public void setUp() throws IOException {
        try {
            deltaTablePath = TEMPORARY_FOLDER.newFolder().getAbsolutePath();
            DeltaTestUtils.initTestForNonPartitionedTable(deltaTablePath);
        } catch (IOException e) {
            throw new RuntimeException("Weren't able to setup the test dependencies", e);
        }
    }

    /**
     * Tests streaming sink with checkpointing enabled.
     * Validates that all records are correctly written to the Delta table with proper
     * checkpoint coordination.
     */
    @ParameterizedTest(name = "isPartitioned = {0}")
    @ValueSource(booleans = {false, true})
    public void testStreamingSink(boolean isPartitioned) throws Exception {

        if (isPartitioned) {
            deltaTablePath = TEMPORARY_FOLDER.newFolder().getAbsolutePath();
            DeltaTestUtils.initTestForPartitionedTable(deltaTablePath);
        }

        // GIVEN
        DeltaLog deltaLog = DeltaLog.forTable(DeltaTestUtils.getHadoopConf(), deltaTablePath);
        List<AddFile> initialDeltaFiles = deltaLog.snapshot().getAllFiles();
        int initialTableRecordsCount = TestParquetReader.readAndValidateAllTableRecords(deltaLog);

        if (isPartitioned) {
            assertEquals(1, initialDeltaFiles.size());
        } else {
            assertEquals(2, initialDeltaFiles.size());
        }

        // WHEN
        StreamExecutionEnvironment env = getTestStreamEnv();

        // FLINK 2.0: Sink interface changed - now only has 1 type parameter (input type)
        Sink<RowData> deltaSink =
            DeltaSinkTestUtils.createDeltaSink(deltaTablePath, isPartitioned);

        int numberOfCheckpoints = NUM_RECORDS / RECORDS_PER_CHECKPOINT;

        env.fromSource(
                new CheckpointCountingSource(RECORDS_PER_CHECKPOINT, numberOfCheckpoints),
                org.apache.flink.api.common.eventtime.WatermarkStrategy.noWatermarks(),
                "checkpointCountingSource")
            .setParallelism(NUM_SOURCES)
            .sinkTo(deltaSink)
            .setParallelism(NUM_SINKS);

        env.execute("Delta Sink Streaming Test");

        // THEN
        int writtenRecordsCount =
            DeltaSinkTestUtils.validateIfPathContainsParquetFilesWithData(deltaTablePath);
        assertEquals(NUM_RECORDS, writtenRecordsCount - initialTableRecordsCount);

        List<AddFile> finalDeltaFiles = deltaLog.update().getAllFiles();
        assert (finalDeltaFiles.size() > initialDeltaFiles.size());
    }

    private StreamExecutionEnvironment getTestStreamEnv() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.enableCheckpointing(500, CheckpointingMode.EXACTLY_ONCE);

        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.setMinPauseBetweenCheckpoints(500);

        io.delta.flink.utils.DeltaTestUtils.configureNoRestartStrategy(env);

        return env;
    }
}
