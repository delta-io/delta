/*
 * Copyright (2024) The Delta Lake Project Authors.
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

/**
 * Class capturing various metrics during log replay. This class can be used in places where
 * actions from the logs are replayed, and we want to capture some metrics.
 */
public class LogReplayMetrics {
    /**
     * Number of `AddFile` actions seen in log replay both from checkpoint files and delta files.
     */
    private long numAddFilesSeen = 0;

    /**
     * Number of `AddFile` actions seen in log replay from delta files.
     */
    private long numAddFilesSeenFromDeltaFiles = 0;

    /**
     * Number of active `AddFile`s that survived (i.e. belong to the table state) the log replay.
     */
    private long numActiveAddFiles = 0;

    /**
     * Number of `AddFile`s that are duplicates. Same `AddFile` (with the same path and DV) can be
     * present in multiple commit files. This happens when stats collection is run on the table in
     * which the same `AddFile` will be added with `stats` without removing it first.
     */
    private long numDuplicateAddFiles = 0;

    /**
     * Number of `RemoveFile`s seen in log replay both from delta files (not from checkpoint).
     */
    private long numTombstonesSeen = 0;

    public void incNumAddFilesSeen() {
        numAddFilesSeen++;
    }

    public void incNumAddFilesSeenFromDeltaFiles() {
        numAddFilesSeenFromDeltaFiles++;
    }

    public void incNumActiveAddFiles() {
        numActiveAddFiles++;
    }

    public void incNumDuplicateAddFiles() {
        numDuplicateAddFiles++;
    }

    public void incNumTombstonesSeen() {
        numTombstonesSeen++;
    }


    public long getNumAddFilesSeen() {
        return numAddFilesSeen;
    }

    public long getNumAddFilesSeenFromDeltaFiles() {
        return numAddFilesSeenFromDeltaFiles;
    }

    public long getNumActiveAddFiles() {
        return numActiveAddFiles;
    }

    public long getNumDuplicateAddFiles() {
        return numDuplicateAddFiles;
    }

    public long getNumTombstonesSeen() {
        return numTombstonesSeen;
    }

    /**
     * Returns a summary of the metrics.
     */
    @Override
    public String toString() {
        return String.format(
            "Number of AddFiles seen: %d\n" +
            "Number of AddFiles seen from delta files: %d\n" +
            "Number of active AddFiles: %d\n" +
            "Number of duplicate AddFiles: %d\n" +
            "Number of tombstones seen: %d\n",
            numAddFilesSeen,
            numAddFilesSeenFromDeltaFiles,
            numActiveAddFiles,
            numDuplicateAddFiles,
            numTombstonesSeen
        );
    }
}
