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

import java.util.List;

import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.FileStatus;
import io.delta.kernel.internal.checkpoints.CheckpointInstance;
import io.delta.kernel.internal.checkpoints.SidecarFile;
import io.delta.kernel.internal.fs.Path;
import io.delta.kernel.internal.util.FileNames;

/** Internal wrapper class holding information needed to perform log replay. */
class ActionWrapper {
    private final ColumnarBatch columnarBatch;
    private final Path checkpointPath;
    private final long version;

    ActionWrapper(ColumnarBatch data, Path checkpointPath, long version) {
        this.columnarBatch = data;
        this.checkpointPath = checkpointPath;
        this.version = version;
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

    /**
     * Reads SidecarFile actions from ColumnarBatch, appending the SidecarFile actions to the end
     * of the file list and removing sidecar actions from the ColumnarBatch.
     */
    public ActionWrapper appendSidecarsFromBatch(List<FileWrapper> files) {
        // If the source checkpoint for this action does not use sidecars, sidecars will not exist
        // in schema.
        CheckpointInstance sourceCheckpoint =
                new CheckpointInstance(checkpointPath.getName());
        if (!sourceCheckpoint.format.usesSidecars()) {
            return this;
        }

        // Sidecars will exist in schema. Extract sidecar files, then remove sidecar files from
        // batch output.
        int sidecarIndex =
                columnarBatch.getSchema().fieldNames().indexOf(LogReplay.SIDECAR_FIELD_NAME);
        CloseableIterator<SidecarFile> sidecars =
                columnarBatch.getRows().map(r -> SidecarFile.fromRow(r, sidecarIndex));
        while (sidecars.hasNext()) {
            SidecarFile f = sidecars.next();
            if (f != null) {
                // It's safe to append sidecars at the end of the file list because checkpoints will
                // come last in the reversed list of files.
                files.add(new FileWrapper(FileStatus.of(
                        FileNames.sidecarFile(checkpointPath.getParent(),
                                f.path), f.sizeInBytes, f.modificationTime), sourceCheckpoint));
            }
        }

        return new ActionWrapper(columnarBatch.withDeletedColumnAt(sidecarIndex),
                checkpointPath, version);
    }
}
