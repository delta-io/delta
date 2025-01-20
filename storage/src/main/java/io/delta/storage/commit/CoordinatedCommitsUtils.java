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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.delta.storage.LogStore;
import io.delta.storage.commit.actions.AbstractMetadata;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

public class CoordinatedCommitsUtils {

    private CoordinatedCommitsUtils() {}

    /** The subdirectory in which to store the delta log. */
    private static final String LOG_DIR_NAME = "_delta_log";

    /** The subdirectory in which to store the unbackfilled commit files. */
    private static final String COMMIT_SUBDIR = "_commits";

    /** The configuration key for the coordinated commits owner name. */
    private static final String COORDINATED_COMMITS_COORDINATOR_NAME_KEY =
            "delta.coordinatedCommits.commitCoordinator-preview";

    /** The configuration key for the coordinated commits owner conf. */
    private static final String COORDINATED_COMMITS_COORDINATOR_CONF_KEY =
        "delta.coordinatedCommits.commitCoordinatorConf-preview";


    /** The configuration key for the coordinated commits table conf. */
    private static final String COORDINATED_COMMITS_TABLE_CONF_KEY =
        "delta.coordinatedCommits.tableConf-preview";

    /**
     * Creates a new unbackfilled delta file path for the given commit version.
     * The path is of the form `tablePath/_delta_log/_commits/00000000000000000001.uuid.json`.
     */
    public static Path generateUnbackfilledDeltaFilePath(
            Path logPath,
            long version) {
        String uuid = UUID.randomUUID().toString();
        Path basePath = new Path(logPath, COMMIT_SUBDIR);
        return new Path(basePath, String.format("%020d.%s.json", version, uuid));
    }

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
                getCoordinatorName(updatedActions.getOldMetadata()).isPresent();
        boolean newMetadataHasCoordinatedCommits =
                getCoordinatorName(updatedActions.getNewMetadata()).isPresent();
        return oldMetadataHasCoordinatedCommits && !newMetadataHasCoordinatedCommits &&
                commitVersion > 0;
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
    public static FileStatus writeUnbackfilledCommitFile(
            LogStore logStore,
            Configuration hadoopConf,
            String logPath,
            long commitVersion,
            Iterator<String> actions,
            String uuid) throws IOException {
        Path commitPath = new Path(getUnbackfilledDeltaFile(
                new Path(logPath), commitVersion, Optional.of(uuid)).toString());
        // Do not use Put-If-Absent for Unbackfilled Commits files since we assume that UUID-based
        // commit files are globally unique, and so we will never have concurrent writers attempting
        // to write the same commit file.
        logStore.write(commitPath, actions, true /* overwrite */, hadoopConf);
        return commitPath.getFileSystem(hadoopConf).getFileStatus(commitPath);
    }

    /** Returns path to the directory which holds the delta log */
    public static Path logDirPath(Path tablePath) {
        return new Path(tablePath, LOG_DIR_NAME);
    }

    /** Returns path to the directory which holds the unbackfilled commits */
    public static Path commitDirPath(Path logPath) {
        return new Path(logPath, COMMIT_SUBDIR);
    }

    /**
     * Retrieves the coordinator name from the provided abstract metadata.
     * If no coordinator is set, an empty optional is returned.
     *
     * @param metadata The abstract metadata from which to retrieve the coordinator name.
     * @return The coordinator name if set, otherwise an empty optional.
     */
    public static Optional<String> getCoordinatorName(AbstractMetadata metadata) {
        String coordinator = metadata
                .getConfiguration()
                .get(COORDINATED_COMMITS_COORDINATOR_NAME_KEY);
        return Optional.ofNullable(coordinator);
    }

    private static Map<String, String> parseConfFromMetadata(
            AbstractMetadata abstractMetadata,
            String confKey) {
        String conf = abstractMetadata
            .getConfiguration()
            .getOrDefault(confKey, "{}");
        try {
            return new ObjectMapper().readValue(
                conf,
                new TypeReference<Map<String, String>>() {});
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to parse conf: ", e);
        }
    }

    /**
     * Get the coordinated commits owner configuration from the provided abstract metadata.
     */
    public static Map<String, String> getCoordinatorConf(AbstractMetadata abstractMetadata) {
        return parseConfFromMetadata(abstractMetadata, COORDINATED_COMMITS_COORDINATOR_CONF_KEY);
    }

    /**
     * Get the coordinated commits table configuration from the provided abstract metadata.
     */
    public static Map<String, String> getTableConf(AbstractMetadata abstractMetadata) {
        return parseConfFromMetadata(abstractMetadata, COORDINATED_COMMITS_TABLE_CONF_KEY);
    }
}
