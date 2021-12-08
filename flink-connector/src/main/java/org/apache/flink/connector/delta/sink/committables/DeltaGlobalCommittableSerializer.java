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

package org.apache.flink.connector.delta.sink.committables;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.streaming.api.functions.sink.filesystem.InProgressFileWriter;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Versioned serializer for {@link DeltaGlobalCommittable}.
 */
@Internal
public class DeltaGlobalCommittableSerializer
    implements SimpleVersionedSerializer<DeltaGlobalCommittable> {

    /**
     * Magic number value for sanity check whether the provided bytes where not corrupted
     */
    private static final int MAGIC_NUMBER = 0x1e765c80;

    private final DeltaCommittableSerializer deltaCommittableSerializer;

    public DeltaGlobalCommittableSerializer(
        SimpleVersionedSerializer<InProgressFileWriter.PendingFileRecoverable>
            pendingFileSerializer) {
        checkNotNull(pendingFileSerializer);
        deltaCommittableSerializer = new DeltaCommittableSerializer(pendingFileSerializer);
    }

    @Override
    public int getVersion() {
        return 1;
    }

    @Override
    public byte[] serialize(DeltaGlobalCommittable committable) throws IOException {
        DataOutputSerializer out = new DataOutputSerializer(256);
        out.writeInt(MAGIC_NUMBER);
        serializeV1(committable, out);
        return out.getCopyOfBuffer();
    }

    @Override
    public DeltaGlobalCommittable deserialize(int version, byte[] serialized) throws IOException {
        DataInputDeserializer in = new DataInputDeserializer(serialized);

        if (version == 1) {
            validateMagicNumber(in);
            return deserializeV1(in);
        }
        throw new IOException("Unrecognized version or corrupt state: " + version);
    }

    private void serializeV1(DeltaGlobalCommittable committable, DataOutputView dataOutputView)
        throws IOException {
        dataOutputView.writeInt(committable.getDeltaCommittables().size());
        for (DeltaCommittable deltaCommittable : committable.getDeltaCommittables()) {
            deltaCommittableSerializer.serializeV1(deltaCommittable, dataOutputView);
        }
    }

    private DeltaGlobalCommittable deserializeV1(DataInputView dataInputView) throws IOException {
        int deltaCommittablesSize = dataInputView.readInt();
        List<DeltaCommittable> deltaCommittables = new ArrayList<>(deltaCommittablesSize);
        for (int i = 0; i < deltaCommittablesSize; i++) {
            DeltaCommittable deserializedCommittable =
                deltaCommittableSerializer.deserializeV1(dataInputView);
            deltaCommittables.add(deserializedCommittable);
        }
        return new DeltaGlobalCommittable(deltaCommittables);
    }

    private static void validateMagicNumber(DataInputView in) throws IOException {
        int magicNumber = in.readInt();
        if (magicNumber != MAGIC_NUMBER) {
            throw new IOException(
                String.format("Corrupt data: Unexpected magic number %08X", magicNumber));
        }
    }
}
