package io.delta.flink.sink.internal.writer;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.apache.flink.core.fs.Path;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;

class DeltaWriterBucketStateSerializerTest {

    @Test
    public void testSerializerVersion() {
        assertThat(new DeltaWriterBucketStateSerializer().getVersion()).isEqualTo(2);
    }

    @Test
    public void testSerializeAndDeserializeBucketSate() throws IOException {

        // GIVEN
        DeltaWriterBucketStateSerializer serializer = new DeltaWriterBucketStateSerializer();

        DeltaWriterBucketState originalState = new DeltaWriterBucketState(
            "bucketId",
            new Path("file:///tmp/bucketId"),
            "appId-1"
        );

        // WHEN
        byte[] serializeData = serializer.serialize(originalState);
        DeltaWriterBucketState recoveredState = serializer.deserialize(
            serializer.getVersion(),
            serializeData
        );

        // THEN
        assertThat(recoveredState.getBucketId()).isEqualTo(originalState.getBucketId());
        assertThat(recoveredState.getBucketPath()).isEqualTo(originalState.getBucketPath());
        assertThat(recoveredState.getAppId()).isEqualTo(originalState.getAppId());
    }

    /**
     * This test verifies if current version of DeltaWriterBucketStateSerializer is able to
     * deserialize an old (V1) DeltaWriterBucketState.
     * The DeltaWriterBucketStateV1.ser file contains bytes created by previous version of
     * DeltaWriterBucketStateSerializer. In order to recreate DeltaWriterBucketStateV1.ser file
     * simply checkout to commit "ca8df21" and use DeltaWriterBucketStateSerializer from that hash
     * to serialize DeltaWriterBucketState and write crated bytes to the file.
     * @throws IOException while reading file.
     */
    @Test
    public void testDeserializeV1State() throws IOException {
        java.nio.file.Path v1StatePath =
            Paths.get("src/test/resources/state/bucket-writer/DeltaWriterBucketStateV1.ser");
        byte[] v1StateData = Files.readAllBytes(v1StatePath);

        DeltaWriterBucketStateSerializer serializer = new DeltaWriterBucketStateSerializer();
        DeltaWriterBucketState v1RecoveredBucketState = serializer.deserialize(1, v1StateData);

        // THEN
        assertThat(v1RecoveredBucketState.getBucketId()).isEqualTo("bucketId");
        assertThat(v1RecoveredBucketState.getBucketPath())
            .isEqualTo(new Path("file:///tmp/bucketId"));
        assertThat(v1RecoveredBucketState.getAppId()).isEqualTo("appId-1");
    }
}
