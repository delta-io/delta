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
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.delta.kernel.utils.FileStatus;

import io.delta.kernel.internal.fs.Path;
import io.delta.kernel.internal.lang.Lazy;

public class LogSegment {
    public final Path logPath;
    public final long version;
    public final List<FileStatus> deltas;
    public final List<FileStatus> checkpoints;
    public final Optional<Long> checkpointVersionOpt;
    public final long lastCommitTimestamp;

    private final Lazy<List<FileStatus>> allFiles;
    private final Lazy<List<FileStatus>> allFilesReversed;

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
     * @param logPath              The path to the _delta_log directory
     * @param version              The Snapshot version to generate
     * @param deltas               The delta commit files (.json) to read
     * @param checkpoints          The checkpoint file(s) to read
     * @param checkpointVersionOpt The checkpoint version used to start replay
     * @param lastCommitTimestamp  The "unadjusted" timestamp of the last commit within this
     *                             segment.
     *                             By unadjusted, we mean that the commit timestamps may not
     *                             necessarily be monotonically increasing for the commits within
     *                             this segment.
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

        this.allFiles = new Lazy<>(() ->
            Stream.concat(checkpoints.stream(), deltas.stream()).collect(Collectors.toList())
        );

        this.allFilesReversed = new Lazy<>(() ->
            allFiles
                .get()
                .stream()
                .sorted(
                    Comparator
                        .comparing((FileStatus a) -> new Path(a.getPath()).getName())
                        .reversed()
                )
                .collect(Collectors.toList())
        );
    }

    /**
     * @return all deltas (.json) and checkpoint (.checkpoint.parquet) files in this LogSegment,
     *         with no ordering guarantees.
     */
    public List<FileStatus> allLogFilesUnsorted() {
        return allFiles.get();
    }

    /**
     * @return all deltas (.json) and checkpoint (.checkpoint.parquet) files in this LogSegment,
     *         sorted in reverse (00012.json, 00011.json, 00010.checkpoint.parquet) order.
     */
    public List<FileStatus> allLogFilesReversed() {
        return allFilesReversed.get();
    }
}
