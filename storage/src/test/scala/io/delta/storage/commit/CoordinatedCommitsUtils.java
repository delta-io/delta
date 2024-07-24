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

package io.delta.storage.commit;

import io.delta.storage.LogStore;
import io.delta.storage.commit.actions.AbstractMetadata;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.Iterator;
import java.util.Optional;
import java.util.UUID;

public class CoordinatedCommitsUtils {

    private CoordinatedCommitsUtils() {}

    /** The subdirectory in which to store the unbackfilled commit files. */
    final static String COMMIT_SUBDIR = "_commits";

    /** The configuration key for the coordinated commits owner. */
    private static final String COORDINATED_COMMITS_COORDINATOR_CONF_KEY =
            "delta.coordinatedCommits.commitCoordinator-preview";

    /**
     * Returns the path to the backfilled delta file for the given commit version.
     * The path is of the form `tablePath/_delta_log/00000000000000000001.json`.
     */
    public static Path getBackfilledDeltaFilePath(
            Path logPath,
            Long version) {
        return new Path(logPath, String.format("%020d.json", version));
    }

    /**
     * Returns true if the commit is a coordinated commits to filesystem conversion.
     */
    public static boolean isCoordinatedCommitsToFSConversion(
            Long commitVersion,
            UpdatedActions updatedActions) {
        boolean oldMetadataHasCoordinatedCommits =
                !getCoordinator(updatedActions.getOldMetadata()).isEmpty();
        boolean newMetadataHasCoordinatedCommits =
                !getCoordinator(updatedActions.getNewMetadata()).isEmpty();
        return oldMetadataHasCoordinatedCommits && !newMetadataHasCoordinatedCommits && commitVersion > 0;
    }

    /**
     * Get the table path from the provided log path.
     */
    public static Path getTablePath(Path logPath) {
        return logPath.getParent();
    }

    /**
     * Returns the un-backfilled uuid formatted delta (json format) path for a given version.
     *
     * @param logPath The root path of the delta log.
     * @param version The version of the delta file.
     * @return The path to the un-backfilled delta file: logPath/_commits/version.uuid.json
     */
    public static Path getUnbackfilledDeltaFile(
            Path logPath, long version, Optional<String> uuidString) {
        Path basePath = commitDirPath(logPath);
        String uuid = uuidString.orElse(UUID.randomUUID().toString());
        return new Path(basePath, String.format("%020d.%s.json", version, uuid));
    }

    /**
     * Write a UUID-based commit file for the specified version to the table at logPath.
     */
    public static FileStatus writeCommitFile(
            LogStore logStore,
            Configuration hadoopConf,
            String logPath,
            long commitVersion,
            Iterator<String> actions,
            String uuid) throws IOException {
        Path commitPath = new Path(getUnbackfilledDeltaFile(
                new Path(logPath), commitVersion, Optional.of(uuid)).toString());
        FileSystem fs = commitPath.getFileSystem(hadoopConf);
        if (!fs.exists(commitPath.getParent())) {
            fs.mkdirs(commitPath.getParent());
        }
        logStore.write(commitPath, actions, false, hadoopConf);
        return commitPath.getFileSystem(hadoopConf).getFileStatus(commitPath);
    }

    /** Returns path to the directory which holds the unbackfilled commits */
    public static Path commitDirPath(Path logPath) {
        return new Path(logPath, COMMIT_SUBDIR);
    }

    private static String getCoordinator(AbstractMetadata metadata) {
        String coordinator = metadata
                .getConfiguration()
                .get(COORDINATED_COMMITS_COORDINATOR_CONF_KEY);
        return coordinator != null ? coordinator : "";
    }
}
