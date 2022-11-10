package io.delta.flink.source.internal.state;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import io.delta.flink.source.internal.utils.SourceUtils;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.connector.file.src.PendingSplitsCheckpoint;
import org.apache.flink.connector.file.src.PendingSplitsCheckpointSerializer;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * <p> A de/serializer for objects of class {@link DeltaEnumeratorStateCheckpoint}.
 *
 * <p> This class provides methods for Flink core to serialize and deserialize {@code
 * DeltaPendingSplitsCheckpointSerializer} objects.
 *
 * <p> Serialization of {@code DeltaPendingSplitsCheckpointSerializer} object takes place during
 * checkpoint operation.
 *
 * <p> Deserialization of {@code DeltaPendingSplitsCheckpointSerializer} object takes place during
 * recovering from checkpoint when {@link SplitEnumerator} is being recreated.
 */
public class DeltaPendingSplitsCheckpointSerializer<SplitT extends DeltaSourceSplit> implements
    SimpleVersionedSerializer<DeltaEnumeratorStateCheckpoint<SplitT>> {

    /**
     * The version of the serialization schema.
     * <p>
     * The {@link org.apache.flink.runtime.source.coordinator.SourceCoordinator} adds the version
     * number to {@link SplitEnumerator} checkpoint data.
     * <p>
     * During recovery from checkpoint, this value is deserialize and used as a version argument of
     * {@link DeltaPendingSplitsCheckpointSerializer#deserialize(int, byte[])} method.
     * <p>
     * It can be used to choose proper deserialization schema.
     */
    private static final int VERSION = 1;

    /**
     * A de/serializer for {@link org.apache.flink.connector.file.src.FileSourceSplit} that {@link
     * DeltaSourceSplit} extends. It handles de/serialization all fields inherited from {@code
     * FileSourceSplit}
     */
    private final PendingSplitsCheckpointSerializer<SplitT> decoratedSerDe;

    /**
     * Creates DeltaPendingSplitsCheckpointSerializer with given Split De/Serializer.
     *
     * @param splitSerDe A serializer for {@link SourceSplit} since {@link SplitEnumerator} state
     *                   checkpoint has to serialize unsigned splits.
     */
    public DeltaPendingSplitsCheckpointSerializer(
        SimpleVersionedSerializer<SplitT> splitSerDe) {
        this.decoratedSerDe = new PendingSplitsCheckpointSerializer<>(splitSerDe);
    }

    @Override
    public int getVersion() {
        return VERSION;
    }

    @Override
    public byte[] serialize(DeltaEnumeratorStateCheckpoint<SplitT> state)
        throws IOException {
        checkArgument(
            state.getClass() == DeltaEnumeratorStateCheckpoint.class,
            "Only supports %s", DeltaEnumeratorStateCheckpoint.class.getName());

        PendingSplitsCheckpoint<SplitT> decoratedCheckPoint =
            state.getPendingSplitsCheckpoint();

        byte[] decoratedBytes = decoratedSerDe.serialize(decoratedCheckPoint);

        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        try (DataOutputViewStreamWrapper outputWrapper =
            new DataOutputViewStreamWrapper(byteArrayOutputStream)) {
            outputWrapper.writeInt(decoratedBytes.length);
            outputWrapper.write(decoratedBytes);
            outputWrapper.writeLong(state.getSnapshotVersion());
            outputWrapper.writeBoolean(state.isMonitoringForChanges());

            final byte[] serPath =
                SourceUtils.pathToString(state.getDeltaTablePath())
                    .getBytes(StandardCharsets.UTF_8);

            outputWrapper.writeInt(serPath.length);
            outputWrapper.write(serPath);
        }

        return byteArrayOutputStream.toByteArray();
    }

    @Override
    public DeltaEnumeratorStateCheckpoint<SplitT> deserialize(int version,
        byte[] serialized) throws IOException {
        if (version == 1) {
            return tryDeserializeV1(serialized);
        }

        throw new IOException("Unknown version: " + version);
    }

    private DeltaEnumeratorStateCheckpoint<SplitT> tryDeserializeV1(byte[] serialized)
        throws IOException {
        try (DataInputViewStreamWrapper inputWrapper =
            new DataInputViewStreamWrapper(new ByteArrayInputStream(serialized))) {
            return deserializeV1(inputWrapper);
        }
    }

    private DeltaEnumeratorStateCheckpoint<SplitT> deserializeV1(
        DataInputViewStreamWrapper inputWrapper) throws IOException {
        byte[] decoratedBytes = new byte[inputWrapper.readInt()];
        inputWrapper.readFully(decoratedBytes);
        PendingSplitsCheckpoint<SplitT> decoratedCheckPoint =
            decoratedSerDe.deserialize(decoratedSerDe.getVersion(), decoratedBytes);

        long snapshotVersion = inputWrapper.readLong();
        boolean monitoringForChanges = inputWrapper.readBoolean();

        final byte[] bytes = new byte[inputWrapper.readInt()];
        inputWrapper.readFully(bytes);

        Path deltaTablePath = new Path(new String(bytes, StandardCharsets.UTF_8));

        return new DeltaEnumeratorStateCheckpoint<>(
            deltaTablePath, snapshotVersion, monitoringForChanges, decoratedCheckPoint);
    }
}
