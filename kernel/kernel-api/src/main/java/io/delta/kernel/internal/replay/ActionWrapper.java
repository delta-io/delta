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
package io.delta.kernel.internal.replay;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.utils.FileStatus;
import io.delta.kernel.internal.checkpoints.CheckpointInstance;
import io.delta.kernel.internal.checkpoints.SidecarFile;
import io.delta.kernel.internal.fs.Path;
import io.delta.kernel.internal.util.FileNames;

/** Internal wrapper class holding information needed to perform log replay. */
class ActionWrapper {
    private ColumnarBatch columnarBatch;
    private final Path checkpointPath;
    private final long version;

    private boolean containsSidecarsInSchema;

    ActionWrapper(
            ColumnarBatch data,
            Path checkpointPath,
            long version,
            boolean containsSidecarsInSchema) {
        this.columnarBatch = data;
        this.checkpointPath = checkpointPath;
        this.version = version;
        this.containsSidecarsInSchema = containsSidecarsInSchema;
    }

    public ColumnarBatch getColumnarBatch() {
        return columnarBatch;
    }

    public boolean isFromCheckpoint() {
        return checkpointPath != null;
    }

    public long getVersion() {
        return version;
    }

    public boolean getContainsSidecarsInSchema() {
        return containsSidecarsInSchema;
    }

    /**
     * Reads SidecarFile actions from ColumnarBatch, appending the SidecarFile actions to the end
     * of the file list and removing sidecar actions from the ColumnarBatch.
     */
    public List<FileWrapper> extractSidecarsFromBatch() {
        // If the source checkpoint for this action does not use sidecars, sidecars will not exist
        // in schema.
        CheckpointInstance sourceCheckpoint =
                new CheckpointInstance(checkpointPath.getName());
        if (!sourceCheckpoint.format.usesSidecars()) {
            return Collections.emptyList();
        }

        // Sidecars will exist in schema. Extract sidecar files, then remove sidecar files from
        // batch output.
        List<FileWrapper> outputFiles = new ArrayList<>();
        int sidecarIndex =
                columnarBatch.getSchema().fieldNames().indexOf(LogReplay.SIDECAR_FIELD_NAME);
        ColumnVector sidecarVector = columnarBatch.getColumnVector(sidecarIndex);
        for (int i = 0; i < columnarBatch.getSize(); i++) {
            SidecarFile f = SidecarFile.fromColumnVector(sidecarVector, i);
            if (f != null) {
                outputFiles.add(
                        new FileWrapper(
                                FileStatus.of(
                                        FileNames.sidecarFile(checkpointPath.getParent(), f.path),
                                        f.sizeInBytes,
                                        f.modificationTime),
                                sourceCheckpoint));
            }
        }

        // Delete SidecarFile actions from the schema.
        columnarBatch = columnarBatch.withDeletedColumnAt(sidecarIndex);
        containsSidecarsInSchema = false;

        return outputFiles;
    }
}
