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

package io.delta.dynamodbcommitcoordinator;

import org.apache.spark.sql.delta.coordinatedcommits.AbstractMetadata;
import org.apache.spark.sql.delta.coordinatedcommits.UpdatedActions;
import org.apache.hadoop.fs.Path;

import java.util.UUID;

public class CoordinatedCommitsUtils {

    private CoordinatedCommitsUtils() {}

    /** The subdirectory in which to store the unbackfilled commit files. */
    final static String COMMIT_SUBDIR = "_commits";

    /** The configuration key for the coordinated commits owner. */
    private static final String COORDINATED_COMMITS_COORDINATOR_CONF_KEY =
            "delta.coordinatedCommits.commitCoordinator-preview";

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

    private static String getCoordinator(AbstractMetadata metadata) {
        return metadata
            .getConfiguration()
            .get(COORDINATED_COMMITS_COORDINATOR_CONF_KEY)
            .getOrElse(() -> "");
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
}
