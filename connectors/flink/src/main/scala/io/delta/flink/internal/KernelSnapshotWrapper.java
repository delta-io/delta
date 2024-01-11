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

package io.delta.standalone.internal;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import io.delta.kernel.data.ColumnVector;

import io.delta.standalone.DeltaScan;
import io.delta.standalone.actions.AddFile;
import io.delta.standalone.actions.Metadata;
import io.delta.standalone.data.CloseableIterator;
import io.delta.standalone.data.RowRecord;
import io.delta.standalone.expressions.Expression;


/**
 * Wrap a {@link io.delta.kernel.Snapshot} such that it conforms to the the {@link
 * io.delta.standalone.Snapshot} interface.
 *
 * NB: Currently only supports the getMetadata and getVersion methods. All others throw exceptions.
 */

public class KernelSnapshotWrapper implements io.delta.standalone.Snapshot {

    // Converting from kernelMetadata to metadata could be expensive, so don't do it until asked,
    // and cache the result.
    private Optional<Metadata> metadata = Optional.empty();
    private io.delta.kernel.internal.SnapshotImpl kernelSnapshot;

    public KernelSnapshotWrapper(io.delta.kernel.internal.SnapshotImpl kernelSnapshot) {
        this.kernelSnapshot = kernelSnapshot;
    }

    // Used for testing
    protected io.delta.kernel.internal.SnapshotImpl getKernelSnapshot() {
        return kernelSnapshot;
    }

    /**
     * Converts the metadata in the kernel snapshot to a compatible Metadata type. The kernel
     * uses more optional types, which are converted to `null` if they are `None` since that's what
     * the standalone Metadata expects.
     */
    private Metadata convertMetadata() {
        io.delta.kernel.internal.actions.Metadata kernelMetadata = kernelSnapshot.getMetadata();

        // Convert the format type
        io.delta.kernel.internal.actions.Format kernelFormat = kernelMetadata.getFormat();
        io.delta.standalone.actions.Format format = new io.delta.standalone.actions.Format(
            kernelFormat.getProvider(),
            java.util.Collections.emptyMap() // TODO: Kernel doesn't currently support options
        );

        // Convert the partition columns from a ColumnVector to a List<String>
        ColumnVector partitionsVec = kernelMetadata.getPartitionColumns().getElements();
        ArrayList<String> partitionColumns = new ArrayList<String>(partitionsVec.getSize());
        for(int i = 0; i < partitionsVec.getSize(); i++) {
            partitionColumns.add(partitionsVec.getString(i));
        }

        // Convert over the schema StructType
        List<io.delta.kernel.types.StructField> kernelFields = kernelMetadata.getSchema().fields();
        io.delta.standalone.types.StructField[] fields =
            new io.delta.standalone.types.StructField[kernelFields.size()];
        int index = 0;
        for (io.delta.kernel.types.StructField kernelField: kernelFields) {
            io.delta.standalone.types.FieldMetadata.Builder metadataBuilder =
                io.delta.standalone.types.FieldMetadata.builder();
            // TODO: Don't use Object and toString here
            for (java.util.Map.Entry<String, Object> entry :
                     kernelField.getMetadata().getEntries().entrySet()) {
                metadataBuilder.putString(entry.getKey(), entry.getValue().toString());
            }
            fields[index] = new io.delta.standalone.types.StructField(
                kernelField.getName(),
                io.delta.standalone.types.DataType.fromJson(
                    kernelField.getDataType().toJson()
                ),
                kernelField.isNullable(),
                metadataBuilder.build()
            );
            index++;
        }
        io.delta.standalone.types.StructType schema =
            new io.delta.standalone.types.StructType(fields);

        return new Metadata(
            kernelMetadata.getId(),
            kernelMetadata.getName().orElse(null),
            kernelMetadata.getDescription().orElse(null),
            format,
            partitionColumns,
            kernelMetadata.getConfiguration(),
            kernelMetadata.getCreatedTime(),
            schema
        );
    }

    /**
     * @return the table metadata for this snapshot.
     *
     */
    @Override
    public Metadata getMetadata() {
        if (!metadata.isPresent()) {
            metadata = Optional.of(convertMetadata());
        }
        return metadata.get();
    }

    /**
     * @return the version for this snapshot
     */
    @Override
    public long getVersion() {
        // WARNING: getVersion in SnapshotImpl currently doesn't use the table client, so we can
        // pass null, but if this changes this code could break
        return kernelSnapshot.getVersion(null);
    }

    /**
     * NOT SUPPORTED
     * @return a {@link DeltaScan} of the files in this snapshot.
     */
    @Override
    public DeltaScan scan() {
        throw new UnsupportedOperationException("not supported");
    }

    /**
     * NOT SUPPORTED
     * @param predicate  the predicate to be used to filter the files in this snapshot.
     * @return a {@link DeltaScan} of the files in this snapshot matching the pushed portion of
     *         {@code predicate}
     */
    @Override
    public DeltaScan scan(Expression predicate) {
        throw new UnsupportedOperationException("not supported");
    }

    /**
     * NOT SUPPORTED
     * @return all of the files present in this snapshot
     */
    @Override
    public List<AddFile> getAllFiles() {
        throw new UnsupportedOperationException("not supported");
    }

    /**
     * NOT SUPPORTED
     * Creates a {@link CloseableIterator} which can iterate over data belonging to this snapshot.
     * It provides no iteration ordering guarantee among data.
     *
     * @return a {@link CloseableIterator} to iterate over data
     */
    @Override
    public CloseableIterator<RowRecord> open() {
        throw new UnsupportedOperationException("not supported");
    }
}
