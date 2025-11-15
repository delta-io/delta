package io.delta.flink.utils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.flink.core.io.SimpleVersionedSerializer;

/**
 * Serializer for CheckpointCountingSplitState.
 */
public class CheckpointCountingSplitStateSerializer
    implements SimpleVersionedSerializer<CheckpointCountingSplitState> {

    private static final int VERSION = 1;
    private final CheckpointCountingSplitSerializer splitSerializer =
        new CheckpointCountingSplitSerializer();

    @Override
    public int getVersion() {
        return VERSION;
    }

    @Override
    public byte[] serialize(CheckpointCountingSplitState state) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             DataOutputStream out = new DataOutputStream(baos)) {
            out.writeInt(state.getRemainingSplits().size());
            for (CheckpointCountingSplit split : state.getRemainingSplits()) {
                byte[] splitBytes = splitSerializer.serialize(split);
                out.writeInt(splitBytes.length);
                out.write(splitBytes);
            }
            out.flush();
            return baos.toByteArray();
        }
    }

    @Override
    public CheckpointCountingSplitState deserialize(int version, byte[] serialized)
        throws IOException {
        if (version != VERSION) {
            throw new IOException("Unknown version: " + version);
        }

        try (ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
             DataInputStream in = new DataInputStream(bais)) {
            int numSplits = in.readInt();
            List<CheckpointCountingSplit> splits = new ArrayList<>(numSplits);
            for (int i = 0; i < numSplits; i++) {
                int length = in.readInt();
                byte[] splitBytes = new byte[length];
                in.readFully(splitBytes);
                splits.add(splitSerializer.deserialize(splitSerializer.getVersion(), splitBytes));
            }
            return new CheckpointCountingSplitState(splits);
        }
    }
}

