package io.delta.flink.source.internal.state;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.flink.connector.file.src.util.CheckpointedPosition;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerialization;
import org.junit.Test;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class DeltaSourceSplitSerializerTest {

    public static final String RANDOM_ID = UUID.randomUUID().toString();

    static void assertSplitsEqual(DeltaSourceSplit expected, DeltaSourceSplit actual) {
        assertEquals(expected.getPartitionValues(), actual.getPartitionValues());
        assertEquals(expected.getReaderPosition(), actual.getReaderPosition());
        assertEquals(expected.splitId(), actual.splitId());
        assertEquals(expected.path(), actual.path());
        assertEquals(expected.offset(), actual.offset());
        assertEquals(expected.length(), actual.length());
        assertArrayEquals(expected.hostnames(), actual.hostnames());
        assertEquals(expected.getReaderPosition(), actual.getReaderPosition());
    }

    private static DeltaSourceSplit serializeAndDeserialize(DeltaSourceSplit split)
        throws IOException {
        DeltaSourceSplitSerializer serializer = DeltaSourceSplitSerializer.INSTANCE;
        byte[] bytes =
            SimpleVersionedSerialization.writeVersionAndSerialize(serializer, split);
        return SimpleVersionedSerialization.readVersionAndDeSerialize(serializer, bytes);
    }

    @Test
    public void serializeSplitWithNoPartitions() throws Exception {
        DeltaSourceSplit split =
            new DeltaSourceSplit(
                Collections.emptyMap(),
                RANDOM_ID,
                new Path("hdfs://namenode:14565/some/path/to/a/file"),
                100_000_000,
                64_000_000,
                new String[]{"host1", "host2", "host3"},
                new CheckpointedPosition(7665391L, 100L));

        DeltaSourceSplit deSerialized = serializeAndDeserialize(split);

        assertSplitsEqual(split, deSerialized);
    }

    @Test
    public void serializeSplitWithSinglePartition() throws Exception {
        DeltaSourceSplit split =
            new DeltaSourceSplit(
                Collections.singletonMap("col1", "val1"),
                "random-id",
                new Path("hdfs://namenode:14565/some/path/to/a/file"),
                100_000_000,
                64_000_000,
                new String[]{"host1", "host2", "host3"},
                new CheckpointedPosition(7665391L, 100L));

        DeltaSourceSplit deSerialized = serializeAndDeserialize(split);

        assertSplitsEqual(split, deSerialized);
    }

    @Test
    public void serializeSplitWithPartitions() throws Exception {

        Map<String, String> partitions = new HashMap<>();
        partitions.put("col1", "val1");
        partitions.put("col2", "val2");
        partitions.put("col3", "val3");

        DeltaSourceSplit split =
            new DeltaSourceSplit(
                partitions,
                "random-id",
                new Path("hdfs://namenode:14565/some/path/to/a/file"),
                100_000_000,
                64_000_000,
                new String[]{"host1", "host2", "host3"},
                new CheckpointedPosition(7665391L, 100L));

        DeltaSourceSplit deSerialized = serializeAndDeserialize(split);

        assertSplitsEqual(split, deSerialized);
    }
}
