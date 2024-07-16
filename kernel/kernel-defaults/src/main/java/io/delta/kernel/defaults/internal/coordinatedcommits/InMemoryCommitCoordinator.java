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

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;

import io.delta.storage.LogStore;
import io.delta.storage.commit.Commit;
import io.delta.storage.commit.CommitCoordinatorClient;
import io.delta.storage.commit.CommitFailedException;
import io.delta.storage.commit.CommitResponse;
import io.delta.storage.commit.GetCommitsResponse;
import io.delta.storage.commit.actions.AbstractMetadata;
import io.delta.storage.commit.actions.AbstractProtocol;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import io.delta.kernel.internal.util.Tuple2;

public class InMemoryCommitCoordinator extends AbstractBatchBackfillingCommitCoordinatorClient {

    /**
     * @param maxCommitVersion represents the max commit version known for the table. This is
     *                         initialized at the time of pre-registration and updated whenever a
     *                         commit is successfully added to the commit-coordinator.
     * @param active represents whether this commit-coordinator has ratified any commit or not.
     * |----------------------------|------------------|---------------------------|
     * |        State               | maxCommitVersion |          active           |
     * |----------------------------|------------------|---------------------------|
     * | Table is pre-registered    | currentVersion+1 |          false            |
     * |----------------------------|------------------|---------------------------|
     * | Table is pre-registered    |       X          |          true             |
     * | and more commits are done  |                  |                           |
     * |----------------------------|------------------|---------------------------|
     */
    private ConcurrentHashMap<String, PerTableData> perTableMap;

    public InMemoryCommitCoordinator(long batchSize) {
        this.batchSize = batchSize;
        this.perTableMap = new ConcurrentHashMap<>();
    }

    private class PerTableData {
        private long maxCommitVersion;
        private boolean active;
        private TreeMap<Long, Commit> commitsMap;
        private ReentrantReadWriteLock lock;

        PerTableData(long maxCommitVersion) {
            this(maxCommitVersion, false);
        }

        PerTableData(long maxCommitVersion, boolean active) {
            this.maxCommitVersion = maxCommitVersion;
            this.active = active;
            this.commitsMap = new TreeMap<>();
            this.lock = new ReentrantReadWriteLock();
        }

        public void updateLastRatifiedCommit(long commitVersion) {
            this.active = true;
            this.maxCommitVersion = commitVersion;
        }

        public long lastRatifiedCommitVersion() {
            return this.active ? this.maxCommitVersion : -1;
        }

        public long getMaxCommitVersion() {
            return maxCommitVersion;
        }

        public TreeMap<Long, Commit> getCommitsMap() {
            return commitsMap;
        }
    }

    /**
     * This method acquires a write lock, validates the commit version is next in line,
     * updates commit maps, and releases the lock.
     *
     */
    @Override
    protected CommitResponse commitImpl(
            LogStore logStore,
            Configuration hadoopConf,
            Path logPath,
            Map<String, String> coordinatedCommitsTableConf,
            long commitVersion,
            FileStatus commitFile,
            long commitTimestamp) throws CommitFailedException {
        Tuple2<CommitResponse, CommitFailedException> ret =
                addToMap(logPath, commitVersion, commitFile, commitTimestamp);
        if (ret._2 != null) {
            throw ret._2;
        } else {
            return ret._1;
        }
    }

    private Tuple2<CommitResponse, CommitFailedException> addToMap(
            Path logPath,
            long commitVersion,
            FileStatus commitFile,
            long commitTimestamp) {

        return withWriteLock(logPath, () -> {
            PerTableData tableData = perTableMap.get(logPath.toString());
            long expectedVersion = tableData.maxCommitVersion + 1;
            if (commitVersion != expectedVersion) {
                return new Tuple2<>(null, new CommitFailedException(
                        commitVersion < expectedVersion,
                        commitVersion < expectedVersion,
                        "Commit version " +
                                commitVersion +
                                " is not valid. Expected version: " +
                                expectedVersion));
            }

            Commit commit = new Commit(commitVersion, commitFile, commitTimestamp);
            tableData.commitsMap.put(commitVersion, commit);
            tableData.updateLastRatifiedCommit(commitVersion);

            logger.info("Added commit file " + commitFile.getPath() + " to commit-coordinator.");
            return new Tuple2<>(new CommitResponse(commit), null);
        });
    }

    @Override
    public GetCommitsResponse getCommits(
            Path logPath,
            Map<String, String> coordinatedCommitsTableConf,
            Long startVersion,
            Long endVersion) {
        return withReadLock(logPath, () -> {
            PerTableData tableData = perTableMap.get(logPath.toString());
            Optional<Long> startVersionOpt = Optional.ofNullable(startVersion);
            Optional<Long> endVersionOpt = Optional.ofNullable(endVersion);
            long effectiveStartVersion = startVersionOpt.orElse(0L);
            // Calculate the end version for the range, or use the last key if endVersion is not
            // provided
            long effectiveEndVersion = endVersionOpt.orElseGet(() ->
                    tableData.commitsMap.isEmpty()
                            ? effectiveStartVersion : tableData.commitsMap.lastKey());
            SortedMap<Long, Commit> commitsInRange = tableData.commitsMap.subMap(
                    effectiveStartVersion, effectiveEndVersion + 1);
            return new GetCommitsResponse(
                    new ArrayList<>(commitsInRange.values()),
                    tableData.lastRatifiedCommitVersion());
        });
    }

    @Override
    protected void registerBackfill(Path logPath, long backfilledVersion) {
        withWriteLock(logPath, () -> {
            PerTableData tableData = perTableMap.get(logPath.toString());
            if (backfilledVersion > tableData.lastRatifiedCommitVersion()) {
                throw new IllegalArgumentException(
                        "Unexpected backfill version: " + backfilledVersion + ". " +
                                "Max backfill version: " + tableData.getMaxCommitVersion());
            }
            // Remove keys with versions less than or equal to 'untilVersion'
            Iterator<Long> iterator = tableData.getCommitsMap().keySet().iterator();
            while (iterator.hasNext()) {
                Long version = iterator.next();
                if (version <= backfilledVersion) {
                    iterator.remove();
                } else {
                    break;
                }
            }
            return null;
        });
    }

    @Override
    public Map<String, String> registerTable(
            Path logPath,
            long currentVersion,
            AbstractMetadata currentMetadata,
            AbstractProtocol currentProtocol) {
        PerTableData newPerTableData = new PerTableData(currentVersion + 1);
        perTableMap.compute(logPath.toString(), (key, existingData) -> {
            if (existingData != null) {
                if (existingData.lastRatifiedCommitVersion() != -1) {
                    throw new IllegalStateException(
                            "Table " + logPath + " already exists in the commit-coordinator.");
                }
                // If lastRatifiedCommitVersion is -1 i.e. the commit-coordinator has never
                // attempted any commit for this table => this table was just pre-registered. If
                // there is another pre-registration request for an older version, we reject it and
                // table can't go backward.
                if (currentVersion < existingData.getMaxCommitVersion()) {
                    throw new IllegalStateException(
                            "Table " + logPath + " already registered with commit-coordinator");
                }
            }
            return newPerTableData;
        });
        return Collections.emptyMap();
    }

    @Override
    public Boolean semanticEquals(CommitCoordinatorClient other) {
        return this.equals(other);
    }

    private <T> T withReadLock(Path logPath, Supplier<T> operation) {
        PerTableData tableData = perTableMap.get(logPath.toString());
        if (tableData == null) {
            throw new IllegalArgumentException("Unknown table " + logPath + ".");
        }
        ReentrantReadWriteLock.ReadLock lock = tableData.lock.readLock();
        lock.lock();
        try {
            return operation.get();
        } finally {
            lock.unlock();
        }
    }

    private <T> T withWriteLock(Path logPath, Supplier<T> operation) {
        PerTableData tableData = perTableMap.get(logPath.toString());
        if (tableData == null) {
            throw new IllegalArgumentException("Unknown table " + logPath + ".");
        }
        ReentrantReadWriteLock.WriteLock lock = tableData.lock.writeLock();
        lock.lock();
        try {
            return operation.get();
        } finally {
            lock.unlock();
        }
    }
}
