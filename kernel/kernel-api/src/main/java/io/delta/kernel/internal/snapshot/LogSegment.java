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

package io.delta.kernel.internal.snapshot;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import io.delta.kernel.fs.FileStatus;
import io.delta.kernel.internal.fs.Path;

public class LogSegment {
    public final Path logPath;
    public final long version;
    public final List<FileStatus> deltas;
    public final List<FileStatus> checkpoints;
    public final Optional<Long> checkpointVersionOpt;
    public final long lastCommitTimestamp;

    public static LogSegment empty(Path logPath) {
        return new LogSegment(
            logPath,
            -1,
            Collections.emptyList(),
            Collections.emptyList(),
            Optional.empty(),
            -1
        );
    }

    /**
     * Provides information around which files in the transaction log need to be read to create
     * the given version of the log.
     *
     * @param logPath The path to the _delta_log directory
     * @param version The Snapshot version to generate
     * @param deltas The delta commit files (.json) to read
     * @param checkpoints The checkpoint file(s) to read
     * @param checkpointVersionOpt The checkpoint version used to start replay
     * @param lastCommitTimestamp The "unadjusted" timestamp of the last commit within this segment.
     *                            By unadjusted, we mean that the commit timestamps may not
     *                            necessarily be monotonically increasing for the commits within
     *                            this segment.
     */
    public LogSegment(
            Path logPath,
            long version,
            List<FileStatus> deltas,
            List<FileStatus> checkpoints,
            Optional<Long> checkpointVersionOpt,
            long lastCommitTimestamp) {
        this.logPath = logPath;
        this.version = version;
        this.deltas = deltas;
        this.checkpoints = checkpoints;
        this.checkpointVersionOpt = checkpointVersionOpt;
        this.lastCommitTimestamp = lastCommitTimestamp;
    }
}
