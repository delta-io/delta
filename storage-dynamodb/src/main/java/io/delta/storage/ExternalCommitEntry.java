/*
 * Copyright (2021) The Delta Lake Project Authors.
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

package io.delta.storage;

import org.apache.hadoop.fs.Path;

public class ExternalCommitEntry {

    /**
     * Absolute path to this delta table
     */
    public final Path tablePath;

    /**
     * File name of this commit, e.g. "000000N.json"
     */
    public final String fileName;

    /**
     * Path to temp file for this commit, relative to the `_delta_log
     */
    public final String tempPath;

    /**
     * true if delta json file is successfully copied to its destination location, else false
     */
    public final boolean complete;

    /**
     * epoch seconds of time of commit if complete=true, else null
     */
    public final Long commitTime;

    public ExternalCommitEntry(
            Path tablePath,
            String fileName,
            String tempPath,
            boolean complete,
            Long commitTime) {
        this.tablePath = tablePath;
        this.fileName = fileName;
        this.tempPath = tempPath;
        this.complete = complete;
        this.commitTime = commitTime;
    }

    /**
     * Return this entry with `complete=true`
     */
    public ExternalCommitEntry asComplete() {
        return new ExternalCommitEntry(
            this.tablePath,
            this.fileName,
            this.tempPath,
            true,
            System.currentTimeMillis() / 1000L
        );
    }

    public Path absoluteJsonPath() {
        return new Path(new Path(tablePath, "_delta_log"), fileName);
    }

    public Path absoluteTempPath() {
        return new Path(new Path(tablePath, "_delta_log"), tempPath);
    }
}
