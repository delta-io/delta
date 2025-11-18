/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.delta.flink.sink.internal.committer;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.StringJoiner;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.delta.flink.internal.ConnectorUtils;
import io.delta.flink.internal.DeltaFlinkHadoopConf;
import io.delta.flink.internal.lang.Lazy;
import io.delta.flink.sink.internal.SchemaConverter;
import io.delta.flink.sink.internal.committables.DeltaCommittable;
import io.delta.storage.CloseableIterator;
import org.apache.flink.streaming.api.functions.sink.filesystem.DeltaPendingFile;
import org.apache.flink.table.types.logical.RowType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.delta.standalone.DeltaLog;
import io.delta.standalone.Operation;
import io.delta.standalone.OptimisticTransaction;
import io.delta.standalone.VersionLog;
import io.delta.standalone.actions.Action;
import io.delta.standalone.actions.AddFile;
import io.delta.standalone.actions.Metadata;
import io.delta.standalone.actions.SetTransaction;
import io.delta.standalone.types.StructType;
import io.delta.standalone.internal.KernelDeltaLogDelegator;

/**
 * FLINK 2.0 IMPLEMENTATION: Global Commit Coordinator
 *
 * <p>This class provides a Flink 2.0-compatible replacement for the removed GlobalCommitter
 * interface. It coordinates DeltaLog commits across all parallel Committer instances.
 *
 * <p>ARCHITECTURE:
 * Since Flink 2.0 removed GlobalCommitter, this coordinator uses a different approach:
 * 1. Committers perform local file commits (rename temp files)
 * 2. This coordinator aggregates committables across all subtasks
 * 3. Commits are made to DeltaLog with exactly-once guarantees
 *
 * <p>USAGE PATTERN:
 * This class can be integrated into the sink in multiple ways:
 * - As a checkpoint completion listener (recommended for Flink 2.0)
 * - As a separate coordination service
 * - As part of a custom sink implementation
 *
 * <p>EXACTLY-ONCE SEMANTICS:
 * - Uses appId + checkpointId for idempotent commits
 * - Deduplicates committables based on file paths
 * - Processes checkpoints in order
 *
 * <p>NOTE: This is a working implementation that provides the functionality previously
 * handled by DeltaGlobalCommitter, adapted for Flink 2.0's architecture.
 *
 * @see DeltaGlobalCommitter for the original Flink 1.x implementation
 * @see DeltaCommitter for local file commits
 */
public class DeltaGlobalCommitCoordinator implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(DeltaGlobalCommitCoordinator.class);

    private static final String APPEND_MODE = "Append";

    private static final String ENGINE_INFO = "Apache Flink";

    /**
     * Thread-safe map to accumulate committables from different subtasks.
     * Key: checkpoint ID, Value: list of committables for that checkpoint
     */
    private final ConcurrentHashMap<Long, List<DeltaCommittable>> pendingCommittables;

    /**
     * Hadoop configuration for DeltaLog access
     */
    private final Configuration hadoopConf;

    /**
     * Path to the Delta table
     */
    private final String tablePath;

    /**
     * RowType object from which the Delta's StructType will be deducted
     */
    private final RowType rowType;

    /**
     * Indicator whether the committer should try to commit unmatching schema
     */
    private final boolean mergeSchema;

    /**
     * Reference to the DeltaLog instance (lazily initialized)
     */
    private transient DeltaLog deltaLog;

    /**
     * Track if this is the first commit (for recovery handling)
     */
    private boolean firstCommit = true;

    public DeltaGlobalCommitCoordinator(
            String tablePath,
            Configuration hadoopConf,
            RowType rowType,
            boolean mergeSchema) {
        this.tablePath = tablePath;
        this.hadoopConf = hadoopConf;
        this.rowType = rowType;
        this.mergeSchema = mergeSchema;
        this.pendingCommittables = new ConcurrentHashMap<>();
    }

    /**
     * Accumulate committables from a Committer subtask.
     * This method is thread-safe and can be called from multiple subtasks.
     *
     * @param checkpointId the checkpoint ID these committables belong to
     * @param committables the committables to accumulate
     */
    public synchronized void accumulateCommittables(
            long checkpointId,
            Collection<DeltaCommittable> committables) {

        if (committables == null || committables.isEmpty()) {
            return;
        }

        LOG.info("Accumulating {} committables for checkpoint {}",
            committables.size(), checkpointId);

        pendingCommittables.computeIfAbsent(checkpointId, k -> new ArrayList<>())
            .addAll(committables);
    }

    /**
     * Commit all accumulated committables for completed checkpoints.
     * This should be called when a checkpoint is confirmed as complete.
     *
     * @param completedCheckpointId the ID of the completed checkpoint
     */
    public synchronized void commitCheckpoint(long completedCheckpointId) {
        long start = System.nanoTime();
        LOG.info("Starting global commit for checkpoint {}", completedCheckpointId);

        // Get committables for this checkpoint
        List<DeltaCommittable> committables =
            pendingCommittables.remove(completedCheckpointId);

        if (committables == null || committables.isEmpty()) {
            LOG.info("No committables to commit for checkpoint {}", completedCheckpointId);
            return;
        }

        // Initialize DeltaLog if needed
        if (deltaLog == null) {
            if (hadoopConf.getBoolean(DeltaFlinkHadoopConf.DELTA_KERNEL_ENABLED, false)) {
                LOG.info("Using delta-kernel for commits");
                deltaLog = KernelDeltaLogDelegator.forTable(hadoopConf, tablePath);
            } else {
                LOG.info("Using delta-standalone for commits");
                deltaLog = DeltaLog.forTable(hadoopConf, new Path(tablePath));
            }
        }

        try {
            // Group by appId and checkpoint
            String appId = resolveAppId(committables);
            if (appId == null) {
                LOG.warn("No appId found in committables, skipping DeltaLog commit");
                return;
            }

            LOG.info("Committing {} committables to DeltaLog for appId={}, checkpointId={}",
                committables.size(), appId, completedCheckpointId);

            // Group committables by checkpoint
            SortedMap<Long, List<CheckpointData>> committablesPerCheckpoint =
                getCommittablesPerCheckpoint(appId, committables, deltaLog);

            // Process checkpoints in order
            for (List<CheckpointData> checkpointData : committablesPerCheckpoint.values()) {
                doCommit(
                    deltaLog.startTransaction(),
                    checkpointData,
                    deltaLog.tableExists());
            }

            long timeElapsed = System.nanoTime() - start;
            LOG.info("Commit finished in {} ns, was first: {}", timeElapsed, firstCommit);
            this.firstCommit = false;

        } catch (Exception e) {
            LOG.error("Failed to commit checkpoint {} to DeltaLog", completedCheckpointId, e);
            // Re-add committables for retry
            pendingCommittables.put(completedCheckpointId, committables);
            throw new RuntimeException("DeltaLog commit failed for checkpoint "
                + completedCheckpointId, e);
        }
    }

    /**
     * Get pending checkpoint IDs that haven't been committed yet.
     *
     * @return sorted list of pending checkpoint IDs
     */
    public List<Long> getPendingCheckpoints() {
        return new ArrayList<>(pendingCommittables.keySet())
            .stream()
            .sorted()
            .collect(Collectors.toList());
    }

    /**
     * Convert committables to CheckpointData and group by checkpoint ID.
     * Handles deduplication for recovery scenarios.
     */
    private SortedMap<Long, List<CheckpointData>> getCommittablesPerCheckpoint(
            String appId,
            List<DeltaCommittable> committables,
            DeltaLog deltaLog) {

        // The last committed table version by THIS flink application.
        Lazy<Long> lastCommittedTableVersion =
            new Lazy<>(() -> deltaLog.startTransaction().txnVersion(appId));

        // Check if we need deduplication (recovery scenario)
        if (!this.firstCommit || lastCommittedTableVersion.get() < 0) {
            // Normal run - no deduplication needed
            return groupCommittablesByCheckpointInterval(committables);
        } else {
            // Processing recovery - deduplicate files
            Collection<CheckpointData> deduplicatedData =
                deduplicateFiles(committables, deltaLog, lastCommittedTableVersion.get());
            return groupCommittablesByCheckpointInterval(deduplicatedData);
        }
    }

    /**
     * Group committables by checkpoint ID.
     */
    private SortedMap<Long, List<CheckpointData>> groupCommittablesByCheckpointInterval(
            List<DeltaCommittable> committables) {

        SortedMap<Long, List<CheckpointData>> committablesPerCheckpoint = new TreeMap<>();

        for (DeltaCommittable committable : committables) {
            final long checkpointId = committable.getCheckpointId();
            final AddFile addFile = committable.getDeltaPendingFile().toAddFile();
            CheckpointData checkpointData = new CheckpointData(committable, addFile);

            committablesPerCheckpoint
                .computeIfAbsent(checkpointId, k -> new LinkedList<>())
                .add(checkpointData);
        }
        return committablesPerCheckpoint;
    }

    /**
     * Group CheckpointData by checkpoint ID.
     */
    private SortedMap<Long, List<CheckpointData>> groupCommittablesByCheckpointInterval(
            Collection<CheckpointData> checkpointData) {

        SortedMap<Long, List<CheckpointData>> result = new TreeMap<>();

        for (CheckpointData data : checkpointData) {
            final long checkpointId = data.committable.getCheckpointId();
            result.computeIfAbsent(checkpointId, k -> new LinkedList<>())
                .add(data);
        }
        return result;
    }

    /**
     * Deduplicate files that were already committed (recovery scenario).
     */
    private Collection<CheckpointData> deduplicateFiles(
            List<DeltaCommittable> committables,
            DeltaLog deltaLog,
            long tableVersion) {

        LOG.info(
            "Processing what seems like a first commit after recovery. "
                + "Deduplicating files from version {}", tableVersion);

        Map<String, CheckpointData> filePathToActionMap = new HashMap<>();

        try {
            for (DeltaCommittable committable : committables) {
                AddFile addFile = committable.getDeltaPendingFile().toAddFile();
                String relativePath = ConnectorUtils.tryRelativizePath(
                    deltaLog.getPath().getFileSystem(hadoopConf),
                    deltaLog.getPath(),
                    new Path(addFile.getPath())
                );
                filePathToActionMap.put(
                    relativePath,
                    new CheckpointData(committable, addFile)
                );
            }
        } catch (IOException e) {
            throw new RuntimeException(
                String.format("Exception during deduplication for table path %s",
                    deltaLog.getPath().toUri().toString()), e);
        }

        // Scan delta log changes to find already committed files
        Iterator<VersionLog> changes = deltaLog.getChanges(tableVersion, true);
        StringJoiner duplicatedFiles = new StringJoiner(", ");

        while (changes.hasNext()) {
            VersionLog versionLog = changes.next();
            try (CloseableIterator<Action> actionsIterator = versionLog.getActionsIterator()) {
                actionsIterator.forEachRemaining(action -> {
                    if (action instanceof AddFile) {
                        CheckpointData removed =
                            filePathToActionMap.remove(((AddFile) action).getPath());
                        if (removed != null) {
                            duplicatedFiles.add(removed.addFile.getPath());
                        }
                    }
                });
            } catch (IOException e) {
                throw new RuntimeException(
                    String.format("Exception iterating over Delta table changes for %s",
                        deltaLog.getPath().toUri().toString()), e);
            }
        }

        LOG.info("Files ignored after deduplication: [{}]", duplicatedFiles);
        return filePathToActionMap.values();
    }

    /**
     * Perform the actual commit to DeltaLog.
     */
    private void doCommit(
            OptimisticTransaction transaction,
            List<CheckpointData> checkpointData,
            boolean tableExists) {

        String appId = checkpointData.get(0).committable.getAppId();
        long checkpointId = checkpointData.get(0).committable.getCheckpointId();

        List<Action> commitActions = new ArrayList<>(checkpointData.size() + 1);
        commitActions.add(prepareSetTransactionAction(appId, transaction.readVersion()));

        Set<String> partitionColumnsSet = null;
        long numOutputRows = 0;
        long numOutputBytes = 0;

        StringJoiner logFiles = new StringJoiner(", ");
        for (CheckpointData data : checkpointData) {
            if (LOG.isDebugEnabled()) {
                logFiles.add(data.addFile.getPath());
            }
            commitActions.add(data.addFile);

            DeltaPendingFile deltaPendingFile = data.committable.getDeltaPendingFile();
            Set<String> currentPartitionCols = deltaPendingFile.getPartitionSpec().keySet();
            if (partitionColumnsSet == null) {
                partitionColumnsSet = currentPartitionCols;
            }
            boolean isPartitionColumnsMetadataRetained = compareKeysOfLinkedSets(
                currentPartitionCols,
                partitionColumnsSet);

            if (!isPartitionColumnsMetadataRetained) {
                throw new RuntimeException(
                    "Partition columns cannot differ for files in the same checkpointId. " +
                        "checkpointId = " + checkpointId + ", " +
                        "file = " + deltaPendingFile.getFileName() + ", " +
                        "partition columns = " +
                        String.join(",", deltaPendingFile.getPartitionSpec().keySet()) +
                        " does not comply with partition columns from other data: " +
                        String.join(",", partitionColumnsSet)
                );
            }

            numOutputRows += deltaPendingFile.getRecordCount();
            numOutputBytes += deltaPendingFile.getFileSize();
        }

        logGlobalCommitterData(appId, checkpointId, logFiles);

        List<String> partitionColumns = partitionColumnsSet == null
            ? Collections.emptyList() : new ArrayList<>(partitionColumnsSet);
        handleMetadataUpdate(tableExists, transaction, partitionColumns);

        Map<String, String> operationMetrics = prepareOperationMetrics(
            commitActions.size() - 1, // subtract SetTransaction action
            numOutputRows,
            numOutputBytes
        );

        Operation operation = prepareDeltaLogOperation(
            partitionColumns,
            operationMetrics
        );

        LOG.info(String.format(
            "Attempting to commit transaction (appId='%s', checkpointId='%s')",
            appId, checkpointId));
        transaction.commit(commitActions, operation, ENGINE_INFO);
        LOG.info(String.format(
            "Successfully committed transaction (appId='%s', checkpointId='%s')",
            appId, checkpointId));
    }

    /**
     * Log global committer information.
     */
    private void logGlobalCommitterData(String appId, long checkpointId, StringJoiner logFiles) {
        if (LOG.isInfoEnabled()) {
            LOG.info("{} files to be committed to Delta table for appId={} checkpointId={}",
                logFiles.length(), appId, checkpointId);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("Files to be committed: appId={} checkpointId={} files=[{}]",
                appId, checkpointId, logFiles);
        }
    }

    /**
     * Handle metadata update (schema evolution).
     */
    private void handleMetadataUpdate(
            boolean tableExists,
            OptimisticTransaction transaction,
            List<String> partitionColumns) {

        Metadata currentMetadata = transaction.metadata();
        if (tableExists && (!partitionColumns.equals(currentMetadata.getPartitionColumns()))) {
            throw new RuntimeException(
                "Stream's partition columns differ from table's partition columns. \n" +
                    "Columns in data files: " + Arrays.toString(partitionColumns.toArray()) + "\n" +
                    "Columns in table: " +
                    Arrays.toString(currentMetadata.getPartitionColumns().toArray()));
        }

        StructType currentTableSchema = currentMetadata.getSchema();
        StructType streamSchema = SchemaConverter.toDeltaDataType(rowType);
        boolean schemasAreMatching = areSchemasEqual(currentTableSchema, streamSchema);

        if (!tableExists || (!schemasAreMatching && mergeSchema)) {
            Metadata updatedMetadata = currentMetadata.copyBuilder()
                .schema(streamSchema)
                .partitionColumns(partitionColumns)
                .build();
            transaction.updateMetadata(updatedMetadata);
        } else if (!schemasAreMatching) {
            String printableCurrentSchema = currentTableSchema == null
                ? "null" : currentTableSchema.toPrettyJson();
            String printableStreamSchema = streamSchema.toPrettyJson();

            throw new RuntimeException(
                "DataStream's schema is different from current table's schema. \n" +
                    "Table schema: " + printableCurrentSchema + "\n" +
                    "Stream schema: " + printableStreamSchema);
        }
    }

    /**
     * Compare two schemas for equality.
     */
    private boolean areSchemasEqual(@Nullable StructType first, @Nullable StructType second) {
        if (first == null || second == null) {
            return false;
        }
        return first.toJson().equals(second.toJson());
    }

    /**
     * Prepare SetTransaction action.
     */
    private SetTransaction prepareSetTransactionAction(String appId, long readVersion) {
        return new SetTransaction(
            appId,
            readVersion + 1, // delta table version after commit will be at least readVersion + 1
            Optional.of(System.currentTimeMillis())
        );
    }

    /**
     * Prepare operation metadata for the commit.
     */
    private Operation prepareDeltaLogOperation(
            List<String> partitionColumns,
            Map<String, String> operationMetrics) {

        Map<String, String> operationParameters = new HashMap<>();
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            operationParameters.put("mode", objectMapper.writeValueAsString(APPEND_MODE));
            // Double JSON encoding for partition columns (Delta Log requirement)
            operationParameters.put("partitionBy", objectMapper.writeValueAsString(
                objectMapper.writeValueAsString(partitionColumns)));
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Cannot map object to JSON", e);
        }
        return new Operation(
            Operation.Name.STREAMING_UPDATE,
            operationParameters,
            operationMetrics);
    }

    /**
     * Prepare operation metrics.
     */
    private Map<String, String> prepareOperationMetrics(
            int numAddedFiles,
            long numOutputRows,
            long numOutputBytes) {

        Map<String, String> operationMetrics = new HashMap<>();
        operationMetrics.put(Operation.Metrics.numRemovedFiles, "0");
        operationMetrics.put(Operation.Metrics.numAddedFiles, String.valueOf(numAddedFiles));
        operationMetrics.put(Operation.Metrics.numOutputRows, String.valueOf(numOutputRows));
        operationMetrics.put(Operation.Metrics.numOutputBytes, String.valueOf(numOutputBytes));
        return operationMetrics;
    }

    /**
     * Compare keys of two linked sets (order matters).
     */
    private boolean compareKeysOfLinkedSets(Set<String> first, Set<String> second) {
        Iterator<String> firstIterator = first.iterator();
        Iterator<String> secondIterator = second.iterator();
        while (firstIterator.hasNext() && secondIterator.hasNext()) {
            if (!firstIterator.next().equals(secondIterator.next())) {
                return false;
            }
        }
        return true;
    }

    /**
     * Resolve appId from committables.
     * All committables should have the same appId.
     *
     * @param committables the committables to extract appId from
     * @return the appId or null if no committables
     */
    private String resolveAppId(List<DeltaCommittable> committables) {
        if (committables == null || committables.isEmpty()) {
            return null;
        }
        return committables.get(0).getAppId();
    }

    /**
     * Clear all pending committables.
     * Should be used carefully, typically only for testing or cleanup.
     */
    public void clear() {
        pendingCommittables.clear();
    }

    /**
     * Get the number of pending committables across all checkpoints.
     *
     * @return total number of pending committables
     */
    public int getPendingCommittablesCount() {
        return pendingCommittables.values().stream()
            .mapToInt(List::size)
            .sum();
    }

    /**
     * Internal class to hold checkpoint data.
     */
    private static class CheckpointData {

        private final AddFile addFile;

        private final DeltaCommittable committable;

        private CheckpointData(DeltaCommittable committable, AddFile addFile) {
            this.addFile = addFile;
            this.committable = committable;
        }
    }
}

