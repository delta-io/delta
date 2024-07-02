package io.delta.flinkv2.sink;

import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.*;

public class DeltaSinkWriterState {
    private final String appId;
    private final String writerId;
    private final long checkpointId; // we don't need this at all

    public DeltaSinkWriterState(String appId, String writerId, long checkpointId) {
        this.appId = appId;
        this.writerId = writerId;
        this.checkpointId = checkpointId;
    }

    public String getAppId() {
        return appId;
    }

    public String getWriterId() {
        return writerId;
    }

    public long getCheckpointId() {
        return checkpointId;
    }

    static class Serializer implements SimpleVersionedSerializer<DeltaSinkWriterState> {
        @Override
        public int getVersion() {
            return 1;
        }

        @Override
        public byte[] serialize(DeltaSinkWriterState obj) throws IOException {
            try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
                 ObjectOutputStream out = new ObjectOutputStream(bos)) {
                out.writeUTF(obj.getAppId());
                out.writeUTF(obj.getWriterId());
                out.writeLong(obj.getCheckpointId());
                out.flush();
                return bos.toByteArray();
            } catch (Exception ex) {
                System.out.println("ERROR serializing DeltaSinkWriterState");
                throw ex;
            }
        }

        @Override
        public DeltaSinkWriterState deserialize(int version, byte[] serialized) throws IOException {
            try (ByteArrayInputStream bis = new ByteArrayInputStream(serialized);
                 ObjectInputStream in = new ObjectInputStream(bis)) {
                String appId = in.readUTF();
                String writerId = in.readUTF();
                long checkpointId = in.readLong();
                return new DeltaSinkWriterState(appId, writerId, checkpointId);
            } catch (Exception ex) {
                System.out.println("ERROR deserializing DeltaSinkWriterState");
                throw ex;
            }
        }
    }
}
