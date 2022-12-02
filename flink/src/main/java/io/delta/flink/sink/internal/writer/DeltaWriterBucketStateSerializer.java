/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.delta.flink.sink.internal.writer;

import java.io.IOException;

import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerialization;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Versioned serializer for {@link DeltaWriterBucketState}.
 */
public class DeltaWriterBucketStateSerializer
    implements SimpleVersionedSerializer<DeltaWriterBucketState> {

    private static final Logger LOG =
        LoggerFactory.getLogger(DeltaWriterBucketStateSerializer.class);

    /**
     * Magic number value for sanity check whether the provided bytes where not corrupted
     */
    private static final int MAGIC_NUMBER = 0x1e764b79;

    @Override
    public int getVersion() {
        return 2;
    }

    @Override
    public byte[] serialize(DeltaWriterBucketState state) throws IOException {
        DataOutputSerializer out = new DataOutputSerializer(256);
        out.writeInt(MAGIC_NUMBER);
        serializeV2(state, out);
        return out.getCopyOfBuffer();
    }

    @Override
    public DeltaWriterBucketState deserialize(int version, byte[] serialized) throws IOException {
        DataInputDeserializer in = new DataInputDeserializer(serialized);

        if (version == 1) {
            validateMagicNumber(in);
            return deserializeV1(in);
        }

        if (version == 2) {
            validateMagicNumber(in);
            return deserializeV2(in);
        }

        throw new IOException("Unrecognized version or corrupt state: " + version);
    }

    private void serializeV2(DeltaWriterBucketState state, DataOutputView dataOutputView)
        throws IOException {
        SimpleVersionedSerialization.writeVersionAndSerialize(
            SimpleVersionedStringSerializer.INSTANCE, state.getBucketId(), dataOutputView);
        dataOutputView.writeUTF(state.getBucketPath().toString());
        dataOutputView.writeUTF(state.getAppId());
    }

    private DeltaWriterBucketState deserializeV1(DataInputView in) throws IOException {
        LOG.info(
            "Deserializing obsolete V1 Bucket State. CheckpointId stored in state will be ignored."
        );
        return internalDeserialize(in);
    }

    private DeltaWriterBucketState deserializeV2(DataInputView in) throws IOException {
        return internalDeserialize(in);
    }

    private DeltaWriterBucketState internalDeserialize(DataInputView dataInputView)
        throws IOException {

        String bucketId = SimpleVersionedSerialization.readVersionAndDeSerialize(
            SimpleVersionedStringSerializer.INSTANCE,
            dataInputView
        );

        String bucketPathStr = dataInputView.readUTF();
        String appId = dataInputView.readUTF();

        return new DeltaWriterBucketState(bucketId, new Path(bucketPathStr), appId);
    }

    private void validateMagicNumber(DataInputView in) throws IOException {
        int magicNumber = in.readInt();
        if (magicNumber != MAGIC_NUMBER) {
            throw new IOException(
                String.format("Corrupt data: Unexpected magic number %08X", magicNumber));
        }
    }
}
