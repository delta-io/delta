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

package io.delta.dynamodbcommitstore;

import org.apache.spark.sql.delta.managedcommit.AbstractMetadata;
import org.apache.spark.sql.delta.managedcommit.UpdatedActions;
import org.apache.hadoop.fs.Path;

import java.util.UUID;

public class ManagedCommitUtils {

    private ManagedCommitUtils() {}

    /** The subdirectory in which to store the unbackfilled commit files. */
    final static String COMMIT_SUBDIR = "_commits";

    /** The configuration key for the managed commit owner. */
    private static final String MANAGED_COMMIT_OWNER_CONF_KEY =
            "delta.managedCommits.commitOwner-dev";

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

    private static String getManagedCommitOwner(AbstractMetadata metadata) {
        return metadata
            .getConfiguration()
            .get(MANAGED_COMMIT_OWNER_CONF_KEY)
            .getOrElse(() -> "");
    }

    /**
     * Returns true if the commit is a managed commit to filesystem conversion.
     */
    public static boolean isManagedCommitToFSConversion(
            Long commitVersion,
            UpdatedActions updatedActions) {
        boolean oldMetadataHasManagedCommits =
                !getManagedCommitOwner(updatedActions.getOldMetadata()).isEmpty();
        boolean newMetadataHasManagedCommits =
                !getManagedCommitOwner(updatedActions.getNewMetadata()).isEmpty();
        return oldMetadataHasManagedCommits && !newMetadataHasManagedCommits && commitVersion > 0;
    }
}
