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
package io.delta.kernel.defaults.internal.coordinatedcommits;

import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;

import io.delta.storage.CloseableIterator;
import io.delta.storage.LogStore;
import io.delta.storage.commit.Commit;
import io.delta.storage.commit.CommitCoordinatorClient;
import io.delta.storage.commit.CommitFailedException;
import io.delta.storage.commit.CommitResponse;
import io.delta.storage.commit.GetCommitsResponse;
import io.delta.storage.commit.UpdatedActions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An abstract {@link CommitCoordinatorClient} which triggers backfills every n commits.
 * - every commit version which satisfies `commitVersion % batchSize == 0` will trigger a backfill.
 */
public abstract class AbstractBatchBackfillingCommitCoordinatorClient
        implements CommitCoordinatorClient {

    protected static final Logger logger =
            LoggerFactory.getLogger(AbstractBatchBackfillingCommitCoordinatorClient.class);

    /**
     * Size of batch that should be backfilled. So every commit version which satisfies
     * `commitVersion % batchSize == 0` will trigger a backfill.
     */
    protected long batchSize;

    /**
     * Commit a given `commitFile` to the table represented by given `logPath` at the
     * given `commitVersion`
     */
    protected abstract CommitResponse commitImpl(
            LogStore logStore,
            Configuration hadoopConf,
            Path logPath,
            Map<String, String> coordinatedCommitsTableConf,
            long commitVersion,
            FileStatus commitFile,
            long commitTimestamp) throws CommitFailedException;

    @Override
    public CommitResponse commit(
            LogStore logStore,
            Configuration hadoopConf,
            Path logPath,
            Map<String, String> coordinatedCommitsTableConf,
            long commitVersion,
            Iterator<String> actions,
            UpdatedActions updatedActions) throws CommitFailedException, IOException {
        Path tablePath = CoordinatedCommitsUtils.getTablePath(logPath);
        if (commitVersion == 0) {
            throw new CommitFailedException(
                    false, false, "Commit version 0 must go via filesystem.");
        }
        logger.info("Attempting to commit version {} on table {}", commitVersion, tablePath);
        FileSystem fs = logPath.getFileSystem(hadoopConf);
        if (batchSize <= 1) {
            // Backfill until `commitVersion - 1`
            logger.info("Making sure commits are backfilled until {}" +
                    " version for table {}", commitVersion - 1, tablePath);
            backfillToVersion(
                    logStore,
                    hadoopConf,
                    logPath,
                    coordinatedCommitsTableConf,
                    commitVersion - 1,
                    null);
        }

        // Write new commit file in _commits directory
        FileStatus fileStatus = CoordinatedCommitsUtils.writeCommitFile(
                logStore, hadoopConf, logPath.toString(), commitVersion, actions, generateUUID());

        // Do the actual commit
        long commitTimestamp = updatedActions.getCommitInfo().getCommitTimestamp();
        CommitResponse commitResponse =
                commitImpl(
                        logStore,
                        hadoopConf,
                        logPath,
                        coordinatedCommitsTableConf,
                        commitVersion,
                        fileStatus,
                        commitTimestamp);

        boolean mcToFsConversion = isCoordinatedCommitsToFSConversion(
                commitVersion, updatedActions);
        // Backfill if needed
        if (batchSize <= 1) {
            // Always backfill when batch size is configured as 1
            backfill(logStore, hadoopConf, logPath, commitVersion, fileStatus);
            Path targetFile = CoordinatedCommitsUtils.getHadoopDeltaFile(logPath, commitVersion);
            FileStatus targetFileStatus = fs.getFileStatus(targetFile);
            Commit newCommit = commitResponse.getCommit().withFileStatus(targetFileStatus);
            return new CommitResponse(newCommit);
        } else if (commitVersion % batchSize == 0 || mcToFsConversion) {
            logger.info("Making sure commits are backfilled till {} version for table {}",
                    commitVersion,
                    tablePath);
            backfillToVersion(
                    logStore,
                    hadoopConf,
                    logPath,
                    coordinatedCommitsTableConf,
                    commitVersion,
                    null);
        }
        logger.info("Commit {} done successfully on table {}", commitVersion, tablePath);
        return commitResponse;
    }

    @Override
    public void backfillToVersion(
            LogStore logStore,
            Configuration hadoopConf,
            Path logPath,
            Map<String, String> coordinatedCommitsTableConf,
            long version,
            Long lastKnownBackfilledVersion) throws IOException {
        // Confirm the last backfilled version by checking the backfilled delta file's existence.
        if (lastKnownBackfilledVersion != null) {
            try {
                FileSystem fs = logPath.getFileSystem(hadoopConf);
                if (!fs.exists(CoordinatedCommitsUtils.getHadoopDeltaFile(logPath, version))) {
                    lastKnownBackfilledVersion = null;
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        Long startVersion = null;
        if (lastKnownBackfilledVersion != null) {
            startVersion = lastKnownBackfilledVersion + 1;
        }
        GetCommitsResponse commitsResponse =
                getCommits(logPath,coordinatedCommitsTableConf, startVersion, version);
        for (Commit commit : commitsResponse.getCommits()) {
            backfill(logStore, hadoopConf, logPath, commit.getVersion(), commit.getFileStatus());
        }
    }

    protected String generateUUID() {
        return UUID.randomUUID().toString();
    }

    /** Backfills a given `fileStatus` to `version`.json */
    protected void backfill(
            LogStore logStore,
            Configuration hadoopConf,
            Path logPath,
            long version,
            FileStatus fileStatus) throws IOException {
        Path targetFile = CoordinatedCommitsUtils.getHadoopDeltaFile(logPath, version);
        logger.info("Backfilling commit " + fileStatus.getPath() + " to " + targetFile);
        CloseableIterator<String> commitContentIterator = logStore
                .read(fileStatus.getPath(), hadoopConf);
        try {
            logStore.write(
                    targetFile,
                    commitContentIterator,
                    false,
                    hadoopConf);
            registerBackfill(logPath, version);
        } catch (FileAlreadyExistsException e) {
            logger.info("The backfilled file " + targetFile + " already exists.");
        } finally {
            commitContentIterator.close();
        }
    }

    /**
     * Callback to tell the CommitCoordinator that all commits <= `backfilledVersion` are
     * backfilled.
     */
    protected abstract void registerBackfill(Path logPath, long backfilledVersion);

    private boolean isCoordinatedCommitsToFSConversion(
            long commitVersion, UpdatedActions updatedActions) {
        boolean oldMetadataHasCoordinatedCommits =
                CoordinatedCommitsUtils
                        .getCommitCoordinatorName(updatedActions.getOldMetadata()).isPresent();
        boolean newMetadataHasCoordinatedCommits =
                CoordinatedCommitsUtils
                        .getCommitCoordinatorName(updatedActions.getNewMetadata()).isPresent();
        return oldMetadataHasCoordinatedCommits
                && !newMetadataHasCoordinatedCommits
                && commitVersion > 0;
    }
}
