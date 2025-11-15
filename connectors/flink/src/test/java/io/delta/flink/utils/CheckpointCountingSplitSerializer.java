package io.delta.flink.utils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.flink.core.io.SimpleVersionedSerializer;

/**
 * Serializer for CheckpointCountingSplit.
 */
public class CheckpointCountingSplitSerializer
    implements SimpleVersionedSerializer<CheckpointCountingSplit> {

    private static final int VERSION = 1;

    @Override
    public int getVersion() {
        return VERSION;
    }

    @Override
    public byte[] serialize(CheckpointCountingSplit split) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             DataOutputStream out = new DataOutputStream(baos)) {
            out.writeUTF(split.splitId());
            out.writeInt(split.getSubtaskId());
            out.writeInt(split.getNextValue());
            out.flush();
            return baos.toByteArray();
        }
    }

    @Override
    public CheckpointCountingSplit deserialize(int version, byte[] serialized) throws IOException {
        if (version != VERSION) {
            throw new IOException("Unknown version: " + version);
        }

        try (ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
             DataInputStream in = new DataInputStream(bais)) {
            String splitId = in.readUTF();
            int subtaskId = in.readInt();
            int nextValue = in.readInt();
            return new CheckpointCountingSplit(splitId, subtaskId, nextValue);
        }
    }
}

