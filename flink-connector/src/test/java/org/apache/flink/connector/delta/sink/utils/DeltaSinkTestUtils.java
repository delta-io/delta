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

package org.apache.flink.connector.delta.sink.utils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.connector.delta.sink.DeltaSink;
import org.apache.flink.connector.delta.sink.committables.DeltaCommittable;
import org.apache.flink.connector.file.sink.utils.FileSinkTestUtils;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.local.LocalFileSystem;
import org.apache.flink.core.fs.local.LocalRecoverableWriter;
import org.apache.flink.formats.parquet.ParquetWriterFactory;
import org.apache.flink.formats.parquet.row.ParquetRowDataBuilder;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.streaming.api.functions.sink.filesystem.DeltaBulkBucketWriter;
import org.apache.flink.streaming.api.functions.sink.filesystem.DeltaPendingFile;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.util.DataFormatConverters;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.Row;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class DeltaSinkTestUtils {

    ///////////////////////////////////////////////////////////////////////////
    // test data utils
    ///////////////////////////////////////////////////////////////////////////

    public static final RowType TEST_ROW_TYPE = new RowType(Arrays.asList(
        new RowType.RowField("name", new VarCharType(VarCharType.MAX_LENGTH)),
        new RowType.RowField("surname", new VarCharType(VarCharType.MAX_LENGTH)),
        new RowType.RowField("age", new IntType())
    ));

    public static final DataFormatConverters.DataFormatConverter<RowData, Row> CONVERTER =
        DataFormatConverters.getConverterForDataType(
            TypeConversions.fromLogicalToDataType(TEST_ROW_TYPE)
        );

    public static List<RowData> getTestRowData(int num_records) {
        List<RowData> rows = new ArrayList<>(num_records);
        for (int i = 0; i < num_records; i++) {
            Integer v = i;
            rows.add(
                CONVERTER.toInternal(
                    Row.of(
                        String.valueOf(v),
                        String.valueOf((v + v)),
                        v)
                )
            );
        }
        return rows;
    }

    ///////////////////////////////////////////////////////////////////////////
    // test delta pending files utils
    ///////////////////////////////////////////////////////////////////////////

    public static DeltaPendingFile getTestDeltaPendingFile() {
        return new DeltaPendingFile(
            "file_name-" + UUID.randomUUID(),
            new FileSinkTestUtils.TestPendingFileRecoverable(),
            new Random().nextInt(30000),
            new Random().nextInt(500000),
            System.currentTimeMillis()
        );
    }

    ///////////////////////////////////////////////////////////////////////////
    // test delta committable utils
    ///////////////////////////////////////////////////////////////////////////

    static final String TEST_APP_ID = UUID.randomUUID().toString();
    static final long TEST_CHECKPOINT_ID = new Random().nextInt(10);

    public static DeltaCommittable getTestDeltaCommittableWithPendingFile() {
        return new DeltaCommittable(
            DeltaSinkTestUtils.getTestDeltaPendingFile(),
            TEST_APP_ID,
            TEST_CHECKPOINT_ID
        );
    }

    public static void validateDeltaCommittablesEquality(
        DeltaCommittable committable,
        DeltaCommittable deserialized) {
        assertEquals(
            committable.getDeltaPendingFile().getPendingFile(),
            deserialized.getDeltaPendingFile().getPendingFile());
        assertEquals(committable.getCheckpointId(), deserialized.getCheckpointId());
        assertEquals(committable.getAppId(), deserialized.getAppId());
        assertEquals(
            committable.getDeltaPendingFile().getFileName(),
            deserialized.getDeltaPendingFile().getFileName());
        assertEquals(
            committable.getDeltaPendingFile().getFileSize(),
            deserialized.getDeltaPendingFile().getFileSize());
        assertEquals(
            committable.getDeltaPendingFile().getRecordCount(),
            deserialized.getDeltaPendingFile().getRecordCount());
        assertEquals(
            committable.getDeltaPendingFile().getLastUpdateTime(),
            deserialized.getDeltaPendingFile().getLastUpdateTime());
    }

    ///////////////////////////////////////////////////////////////////////////
    // hadoop conf test utils
    ///////////////////////////////////////////////////////////////////////////

    public static org.apache.hadoop.conf.Configuration getHadoopConf() {
        org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
        conf.set("parquet.compression", "SNAPPY");
        return conf;
    }

    ///////////////////////////////////////////////////////////////////////////
    // filesystem test utils
    ///////////////////////////////////////////////////////////////////////////

    public static int validateIfPathContainsParquetFilesWithData(String deltaTablePath)
        throws IOException {
        List<File> files = Files.walk(Paths.get(deltaTablePath))
            .map(java.nio.file.Path::toFile)
            .filter(file -> !file.isDirectory())
            .filter(file -> !file.getName().contains("inprogress"))
            .filter(file -> file.getName().endsWith(".snappy.parquet"))
            .collect(Collectors.toList());

        assertTrue(files.size() > 0);

        int totalRecordsCount = 0;
        for (File file : files) {
            // simple check if files contain any data besides footer
            assertTrue(file.length() > 100);
            totalRecordsCount += TestParquetReader.parseAndCountRecords(
                new Path(file.toURI()),
                DeltaSinkTestUtils.TEST_ROW_TYPE);
        }
        return totalRecordsCount;
    }


    ///////////////////////////////////////////////////////////////////////////
    // ParquetWriterFactory test utils
    ///////////////////////////////////////////////////////////////////////////

    public static ParquetWriterFactory<RowData> createTestWriterFactory() {
        return ParquetRowDataBuilder.createWriterFactory(
            DeltaSinkTestUtils.TEST_ROW_TYPE,
            DeltaSinkTestUtils.getHadoopConf(),
            true // utcTimestamp
        );
    }

    ///////////////////////////////////////////////////////////////////////////
    // writer test utils
    ///////////////////////////////////////////////////////////////////////////

    public static final OnCheckpointRollingPolicy<RowData, String> ON_CHECKPOINT_ROLLING_POLICY =
        OnCheckpointRollingPolicy.build();

    /**
     * Internal testing method for getting local data writer.
     *
     * @param path bucket path
     * @return mock implementation for {@link LocalRecoverableWriter}
     */
    private static LocalRecoverableWriter getTestBucketRecoverableWriter(Path path) {
        try {
            final FileSystem fs = FileSystem.get(path.toUri());
            if (!(fs instanceof LocalFileSystem)) {
                fail(
                    "Expected Local FS but got a "
                        + fs.getClass().getName()
                        + " for path: "
                        + path);
            }
            return new LocalRecoverableWriter((LocalFileSystem) fs);
        } catch (IOException e) {
            fail();
        }
        return null;
    }

    public static DeltaBulkBucketWriter<RowData, String> createBucketWriter(Path path)
        throws IOException {
        return new DeltaBulkBucketWriter<>(
            getTestBucketRecoverableWriter(path),
            DeltaSinkTestUtils.createTestWriterFactory());
    }

    ///////////////////////////////////////////////////////////////////////////
    // IT case utils
    ///////////////////////////////////////////////////////////////////////////

    public static DeltaSink<RowData> createDeltaSink(String deltaTablePath) {
        return DeltaSink
            .forDeltaFormat(
                new Path(deltaTablePath),
                DeltaSinkTestUtils.getHadoopConf(),
                DeltaSinkTestUtils.TEST_ROW_TYPE).build();
    }

    public static MiniCluster getMiniCluster() {
        final Configuration config = new Configuration();
        config.setString(RestOptions.BIND_PORT, "18081-19000");
        final MiniClusterConfiguration cfg =
            new MiniClusterConfiguration.Builder()
                .setNumTaskManagers(1)
                .setNumSlotsPerTaskManager(4)
                .setConfiguration(config)
                .build();
        return new MiniCluster(cfg);
    }
}
