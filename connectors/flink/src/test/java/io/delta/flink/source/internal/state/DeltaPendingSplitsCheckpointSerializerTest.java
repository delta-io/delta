package io.delta.flink.source.internal.state;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.function.BiConsumer;

import org.apache.flink.connector.file.src.PendingSplitsCheckpoint;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerialization;
import org.junit.Assert;
import org.junit.Test;
import static org.junit.Assert.assertEquals;

public class DeltaPendingSplitsCheckpointSerializerTest {

    private static final Path TABLE_PATH = new Path("some/path");

    @Test
    public void serializeEmptyCheckpoint() throws Exception {

        PendingSplitsCheckpoint<DeltaSourceSplit> pendingSplits =
            PendingSplitsCheckpoint.fromCollectionSnapshot(Collections.emptyList(),
                Collections.emptyList());

        DeltaEnumeratorStateCheckpoint<DeltaSourceSplit> checkpoint =
            new DeltaEnumeratorStateCheckpoint<>(
                TABLE_PATH, 2, true, pendingSplits);

        DeltaEnumeratorStateCheckpoint<DeltaSourceSplit> deSerialized =
            serializeAndDeserialize(checkpoint);

        assertCheckpointsEqual(checkpoint, deSerialized);
    }

    @Test
    public void serializeSomeSplits() throws Exception {

        PendingSplitsCheckpoint<DeltaSourceSplit> pendingSplits =
            PendingSplitsCheckpoint.fromCollectionSnapshot(
                Arrays.asList(
                    testSplitNoPartitions(), testSplitSinglePartition(),
                    testSplitMultiplePartitions()),
                Collections.emptyList());

        DeltaEnumeratorStateCheckpoint<DeltaSourceSplit> checkpoint =
            new DeltaEnumeratorStateCheckpoint<>(
                TABLE_PATH, 100, true, pendingSplits);

        DeltaEnumeratorStateCheckpoint<DeltaSourceSplit> deSerialized =
            serializeAndDeserialize(checkpoint);

        assertCheckpointsEqual(checkpoint, deSerialized);
    }

    @Test
    public void serializeSplitsAndProcessedPaths() throws Exception {
        PendingSplitsCheckpoint<DeltaSourceSplit> pendingSplits =
            PendingSplitsCheckpoint.fromCollectionSnapshot(
                Arrays.asList(
                    testSplitNoPartitions(), testSplitSinglePartition(),
                    testSplitMultiplePartitions()),
                Arrays.asList(
                    new Path("file:/some/path"),
                    new Path("s3://bucket/key/and/path"),
                    new Path("hdfs://namenode:12345/path")));

        DeltaEnumeratorStateCheckpoint<DeltaSourceSplit> checkpoint =
            new DeltaEnumeratorStateCheckpoint<>(
                TABLE_PATH, 1410, true, pendingSplits);

        DeltaEnumeratorStateCheckpoint<DeltaSourceSplit> deSerialized =
            serializeAndDeserialize(checkpoint);

        assertCheckpointsEqual(checkpoint, deSerialized);
    }

    private DeltaEnumeratorStateCheckpoint<DeltaSourceSplit> serializeAndDeserialize(
        DeltaEnumeratorStateCheckpoint<DeltaSourceSplit> split) throws IOException {

        DeltaPendingSplitsCheckpointSerializer<DeltaSourceSplit> serializer =
            new DeltaPendingSplitsCheckpointSerializer<>(DeltaSourceSplitSerializer.INSTANCE);
        byte[] bytes =
            SimpleVersionedSerialization.writeVersionAndSerialize(serializer, split);
        return SimpleVersionedSerialization.readVersionAndDeSerialize(serializer, bytes);
    }

    private void assertCheckpointsEqual(
        DeltaEnumeratorStateCheckpoint<DeltaSourceSplit> expected,
        DeltaEnumeratorStateCheckpoint<DeltaSourceSplit> actual) {

        assertEquals(expected.getDeltaTablePath(), actual.getDeltaTablePath());
        assertEquals(expected.getSnapshotVersion(), actual.getSnapshotVersion());
        assertEquals(expected.isMonitoringForChanges(),
            actual.isMonitoringForChanges());

        assertOrderedCollectionEquals(
            expected.getSplits(),
            actual.getSplits(),
            DeltaSourceSplitSerializerTest::assertSplitsEqual);

        assertOrderedCollectionEquals(
            expected.getAlreadyProcessedPaths(),
            actual.getAlreadyProcessedPaths(),
            Assert::assertEquals);
    }

    private DeltaSourceSplit testSplitNoPartitions() {
        return new DeltaSourceSplit(
            Collections.emptyMap(),
            "random-id",
            new Path("hdfs://nodename:14565/some/path/to/a/file"),
            100_000_000,
            64_000_000,
            "host1",
            "host2",
            "host3");
    }

    private DeltaSourceSplit testSplitSinglePartition() {
        return new DeltaSourceSplit(Collections.singletonMap("col1", "val1"), "some-id",
            new Path("file:/some/path/to/a/file"), 0, 0);
    }

    private DeltaSourceSplit testSplitMultiplePartitions() {
        Map<String, String> partitions = new HashMap<>();
        partitions.put("col1", "val1");
        partitions.put("col2", "val2");
        partitions.put("col3", "val3");

        return new DeltaSourceSplit(
            partitions, "an-id", new Path("s3://some-bucket/key/to/the/object"), 0, 1234567);
    }

    private <E> void assertOrderedCollectionEquals(
        Collection<E> expected, Collection<E> actual, BiConsumer<E, E> equalityAsserter) {

        assertEquals(expected.size(), actual.size());
        Iterator<E> expectedIter = expected.iterator();
        Iterator<E> actualIter = actual.iterator();
        while (expectedIter.hasNext()) {
            equalityAsserter.accept(expectedIter.next(), actualIter.next());
        }
    }

}
