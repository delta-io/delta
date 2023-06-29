/*
 * Copyright (2023) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.delta.kernel.internal.deletionvectors;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.Optional;
import java.util.zip.CRC32;

import io.delta.kernel.client.FileSystemClient;
import io.delta.kernel.internal.actions.DeletionVectorDescriptor;
import io.delta.kernel.internal.util.InternalUtils;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.Tuple2;
import io.delta.kernel.utils.Utils;
import static io.delta.kernel.internal.util.InternalUtils.checkArgument;

/**
 * Bitmap for a Deletion Vector, implemented as a thin wrapper around a Deletion Vector
 * Descriptor. The bitmap can be empty, inline or on-disk. In case of on-disk deletion
 * vectors, `tableDataPath` must be set to the data path of the Delta table, which is where
 * deletion vectors are stored.
 */
public class DeletionVectorStoredBitmap {

    private final DeletionVectorDescriptor dvDescriptor;
    private final Optional<String> tableDataPath;

    public DeletionVectorStoredBitmap(
            DeletionVectorDescriptor dvDescriptor,
            Optional<String> tableDataPath) {
        checkArgument(tableDataPath.isPresent() || !dvDescriptor.isOnDisk(),
                "Table path is required for on-disk deletion vectors");
        this.dvDescriptor = dvDescriptor;
        this.tableDataPath = tableDataPath;
    }

    // TODO: for now we request 1 stream at a time
    public RoaringBitmapArray load(FileSystemClient fileSystemClient) throws IOException {
        if (dvDescriptor.getCardinality() == 0) { // isEmpty
            return new RoaringBitmapArray();
        } else if (dvDescriptor.isInline()) {
            return RoaringBitmapArray.readFrom(dvDescriptor.inlineData());
        } else { // isOnDisk
            String onDiskPath = dvDescriptor.getAbsolutePath(tableDataPath.get());

            // TODO: this type is TBD
            Tuple2<String, Tuple2<Integer, Integer>> dvToRead =
                    new Tuple2(
                            onDiskPath, // filePath
                            new Tuple2(
                                    dvDescriptor.getOffset().orElse(0), // offset
                                    // we pad 4 bytes in the front for the size
                                    // and 4 bytes at the end for CRC-32 checksum
                                    dvDescriptor.getSizeInBytes() + 8 // size
                            )
                    );

            CloseableIterator<ByteArrayInputStream> streamIter = fileSystemClient.readFiles(
                    Utils.singletonCloseableIterator(dvToRead));
            ByteArrayInputStream stream = InternalUtils.getSingularElement(streamIter).orElseThrow(
                    () -> new IllegalStateException("Iterator should not be empty")
            );
            return loadFromStream(stream);
        }
    }

    /**
     * Read a serialized deletion vector from a data stream.
     */
    private RoaringBitmapArray loadFromStream(ByteArrayInputStream stream) throws IOException {
        DataInputStream dataStream = new DataInputStream(stream);
        try {
            int sizeAccordingToFile = dataStream.readInt();
            if (dvDescriptor.getSizeInBytes() != sizeAccordingToFile) {
                throw new RuntimeException("DV size mismatch");
            }

            byte[] buffer = new byte[sizeAccordingToFile];
            dataStream.readFully(buffer);

            int expectedChecksum = dataStream.readInt();
            int actualChecksum = calculateChecksum(buffer);
            if (expectedChecksum != actualChecksum) {
                throw new RuntimeException("DV checksum mismatch");
            }
            return RoaringBitmapArray.readFrom(buffer);
        } finally {
            stream.close();
            dataStream.close();
        }
    }

    /**
     * Calculate checksum of a serialized deletion vector. We are using CRC32 which has 4bytes size,
     * but CRC32 implementation conforms to Java Checksum interface which requires a long. However,
     * the high-order bytes are zero, so here is safe to cast to Int. This will result in negative
     * checksums, but this is not a problem because we only care about equality.
     */
    private int calculateChecksum(byte[] data) {
        CRC32 crc = new CRC32();
        crc.update(data);
        return (int) crc.getValue();
    }
}
