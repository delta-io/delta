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

import io.delta.kernel.internal.fs.Path;
import io.delta.kernel.internal.util.FileNames;
import io.delta.kernel.internal.checkpoints.CheckpointInstance;
import io.delta.kernel.utils.FileStatus;

/** Internal wrapper class holding information needed to perform log replay. */
public class FileWrapper {
    private final FileStatus file;

    private final String fileName;

    private final boolean isSidecar;

    private final CheckpointInstance checkpointInstance;

    // sidecarManifest is the checkpoint manifest file containing the SidecarFile action used to
    // create this FileWrapper. If null, but the file is a checkpoint file, the checkpointInstance
    // is generated from the filepath.
    FileWrapper(FileStatus file, CheckpointInstance sidecarManifest) {
        this.file = file;
        this.fileName = new Path(file.getPath()).getName();
        if (sidecarManifest != null) {
            this.checkpointInstance = sidecarManifest;
            this.isSidecar = true;
        } else {
            this.isSidecar = false;
            if (FileNames.isCheckpointFile(fileName)) {
                this.checkpointInstance = new CheckpointInstance(fileName);
            } else {
                this.checkpointInstance = null;
            }
        }
    }

    public FileStatus getFile() {
        return file;
    }

    public boolean isCommit() {
        return FileNames.isCommitFile(fileName);
    }

    public boolean isMultipartCheckpoint() {
        return checkpointInstance != null &&
                checkpointInstance.format == CheckpointInstance.CheckpointFormat.MULTI_PART;
    }

    public boolean isSinglePartOrV2Checkpoint() {
        return checkpointInstance != null &&
                checkpointInstance.format.usesSidecars();

    }

    public boolean isSidecar() {
        return isSidecar;
    }

    public long getVersion() {
        return checkpointInstance.version;
    }

    public Path getCheckpointInstanceFilepath() {
        return checkpointInstance.filePath.get();
    }
}
