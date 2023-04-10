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
import javax.annotation.Nullable;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.delta.flink.internal.ConnectorUtils;
import io.delta.flink.internal.lang.Lazy;
import io.delta.flink.sink.internal.SchemaConverter;
import io.delta.flink.sink.internal.committables.DeltaCommittable;
import io.delta.flink.sink.internal.committables.DeltaGlobalCommittable;
import io.delta.storage.CloseableIterator;
import org.apache.flink.api.connector.sink.GlobalCommitter;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.functions.sink.filesystem.DeltaPendingFile;
import org.apache.flink.table.types.logical.RowType;
import org.apache.hadoop.conf.Configuration;
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

/**
 * A {@link GlobalCommitter} implementation for
 * {@link io.delta.flink.sink.DeltaSink}.
 * <p>
 * It commits written files to the DeltaLog and provides exactly once semantics by guaranteeing
 * idempotence behaviour of the commit phase. It means that when given the same set of
 * {@link DeltaCommittable} objects (that contain metadata about written files along with unique
 * identifier of the given Flink's job and checkpoint id) it will never commit them multiple times.
 * Such behaviour is achieved by constructing transactional id using mentioned app identifier and
 * checkpointId.
 * <p>
 * Lifecycle of instances of this class is as follows:
 * <ol>
 *     <li>Instances of this class are being created during a (global) commit stage</li>
 *     <li>For given commit stage there is only one singleton instance of
 *         {@link DeltaGlobalCommitter}</li>
 *     <li>Every instance exists only during given commit stage after finishing particular
 *         checkpoint interval. Despite being bundled to a finish phase of a checkpoint interval
 *         a single instance of {@link DeltaGlobalCommitter} may process committables from multiple
 *         checkpoints intervals (it happens e.g. when there was a app's failure and Flink has
 *         recovered committables from previous commit stage to be re-committed.</li>
 * </ol>
 */
public class DeltaGlobalCommitter
    implements GlobalCommitter<DeltaCommittable, DeltaGlobalCommittable> {

    private static final Logger LOG = LoggerFactory.getLogger(DeltaGlobalCommitter.class);

    private static final String APPEND_MODE = "Append";

    private static final String ENGINE_INFO =
        "flink-engine/" + io.delta.flink.sink.internal.committer.Meta.FLINK_VERSION +
        " flink-delta-connector/" + io.delta.flink.sink.internal.committer.Meta.CONNECTOR_VERSION;

    /**
     * Hadoop configuration that is passed to {@link DeltaLog} instance when creating it
     */
    private final Configuration conf;

    /**
     * RowType object from which the Delta's {@link StructType} will be deducted
     */
    private final RowType rowType;

    /**
     * Indicator whether the committer should try to commit unmatching schema
     */
    private final boolean mergeSchema;

    /**
     * Keeping a reference to the DeltaLog will make future `deltaLog.startTransaction()` calls,
     * which internally will call `deltaLog.update()`, cheaper. This is because we don't need to
     * do a full table replay, but instead only need to append the changes to the latest snapshot`.
     */
    private final transient DeltaLog deltaLog;

    private transient boolean firstCommit = true;

    public DeltaGlobalCommitter(
            Configuration conf,
            Path basePath,
            RowType rowType,
            boolean mergeSchema) {

        this.conf = conf;
        this.rowType = rowType;
        this.mergeSchema = mergeSchema;
        this.deltaLog = DeltaLog.forTable(conf,
            new org.apache.hadoop.fs.Path(basePath.toUri()));
    }

    /**
     * Filters committables that will be provided to {@link GlobalCommitter#commit} method.
     * <p>
     * We are always returning all the committables as we do not implement any retry behaviour
     * in {@link GlobalCommitter#commit} method and always want to try to commit all the received
     * committables.
     * <p>
     * If there will be any previous committables from checkpoint intervals other than the most
     * recent one then we will try to commit them in an idempotent manner during
     * {@link DeltaGlobalCommitter#commit} method and not by filtering them.
     *
     * @param globalCommittables list of combined committables objects
     * @return same as input
     */
    @Override
    public List<DeltaGlobalCommittable> filterRecoveredCommittables(
            List<DeltaGlobalCommittable> globalCommittables) {
        return globalCommittables;
    }

    /**
     * Compute an aggregated committable from a list of committables.
     * <p>
     * We just wrap received list of committables inside a {@link DeltaGlobalCommitter} instance
     * as we will do all of the processing in {@link GlobalCommitter#commit} method.
     *
     * @param committables list of committables object that may be coming from multiple checkpoint
     *                     intervals
     * @return {@link DeltaGlobalCommittable} serving as a wrapper class for received committables
     */
    @Override
    public DeltaGlobalCommittable combine(List<DeltaCommittable> committables) {

        if (LOG.isTraceEnabled()) {
            for (DeltaCommittable committable : committables) {
                LOG.trace("Creating global committable object with committable for: " +
                    "appId=" + committable.getAppId() +
                    " checkpointId=" + committable.getCheckpointId() +
                    " deltaPendingFile=" + committable.getDeltaPendingFile()
                );
            }
        }
        return new DeltaGlobalCommittable(committables);
    }

    /**
     * Resolves appId param from the first committable object. It does not matter which object as
     * all committables carry the same appId value. It's ok to return null value here as it would
     * mean that there are no committables (aka no stream events were received) for given
     * checkpoint.
     *
     * @param globalCommittables list of global committables objects
     * @return unique app identifier for given Flink job
     */
    @Nullable
    private String resolveAppId(List<DeltaGlobalCommittable> globalCommittables) {
        for (DeltaGlobalCommittable globalCommittable : globalCommittables) {
            for (DeltaCommittable deltaCommittable : globalCommittable.getDeltaCommittables()) {
                return deltaCommittable.getAppId();
            }
        }
        return null;
    }

    /**
     * Commits already written files to the Delta table using unique identifier for the given Flink
     * job (appId) and checkpointId delivered with every committable object. Those ids together
     * construct transactionId that will be used for verification whether given set of files has
     * already been committed to the Delta table.
     *
     * <p>During commit preparation phase:
     *
     * <ol>
     *   <li>First appId is resolved from any of the provided committables.
     *       If no appId is resolved then it means that no committables were provided and no commit
     *       is performed. Such situations may happen when e.g. there were no stream events received
     *       within given checkpoint interval,
     *   <li>If appId is successfully resolved then the provided set of committables needs to be
     *       flattened (as one {@link DeltaGlobalCommittable} contains a list of
     *       {@link DeltaCommittable}), mapped to {@link AddFile} objects and then grouped by
     *       checkpointId. The grouping part is necessary as committer object may receive
     *       committables from different checkpoint intervals,
     *   <li>We process each of the resolved checkpointId in increasing order,
     *   <li>During processing each of the checkpointId and their committables, we first query
     *       the DeltaLog for last committed transaction version for given appId. Here transaction
     *       version equals checkpointId. We proceed with the transaction only if current
     *       checkpointId is greater than last committed transaction version.
     *   <li>If above condition is met, then we handle the metadata for data in given stream by
     *       comparing the stream's schema with current table snapshot's schema. We proceed with
     *       the transaction only when the schemas are matching or when it was explicitly configured
     *       during creation of the sink that we can try to update the schema.
     *   <li>If above validation passes then we prepare the final set of {@link Action} objects to
     *       be committed along with transaction's metadata and mandatory parameters,
     *   <li>We try to commit the prepared transaction
     *   <li>If the commit fails then we fail the application as well. If it succeeds then we
     *       proceed with the next checkpointId (if any).
     * </ol>
     *
     * @param globalCommittables list of combined committables objects
     * @return always empty collection as we do not want any retry behaviour
     */
    @Override
    public List<DeltaGlobalCommittable> commit(List<DeltaGlobalCommittable> globalCommittables) {
        String appId = resolveAppId(globalCommittables);
        if (appId != null) { // means there are committables to process

            SortedMap<Long, List<CheckpointData>> committablesPerCheckpoint =
                getCommittablesPerCheckpoint(
                    appId,
                    globalCommittables,
                    this.deltaLog);

            // We used SortedMap and SortedMap.values() maintain the sorted order.
            for (List<CheckpointData> checkpointData : committablesPerCheckpoint.values()) {
                doCommit(
                    this.deltaLog.startTransaction(),
                    checkpointData,
                    this.deltaLog.tableExists());
            }
        }

        this.firstCommit = false;
        return Collections.emptyList();
    }

    /**
     * Converts {@link DeltaCommittable} objects from list of {@link DeltaGlobalCommittable} objects
     * to sorted map where key is a checkpoint id and the value is {@link CheckpointData} object
     * created from individual {@link DeltaCommittable}.
     *
     * @param appId              unique identifier of the application
     * @param globalCommittables {@link DeltaGlobalCommittable} to convert and sort.
     * @param deltaLog           {@link DeltaLog} for current delta table.
     * @return sorted map of checkpoint id to {@code List<CheckpointData>) mappings.
     */
    private SortedMap<Long, List<CheckpointData>> getCommittablesPerCheckpoint(
            String appId,
            List<DeltaGlobalCommittable> globalCommittables,
            DeltaLog deltaLog) {

        // The last committed table version by THIS flink application.
        //
        // We can access this value using the thread-unsafe `Lazy::get` because Flink's threading
        // model guarantees that GlobalCommitter::commit will be executed by a single thread.
        Lazy<Long> lastCommittedTableVersion =
            new Lazy<>(() -> deltaLog.startTransaction().txnVersion(appId));

        // Keep `lastCommittedTableVersion.get() < 0` as the second predicate in the OR statement
        // below since it is expensive and we should avoid computing it if possible.
        if (!this.firstCommit || lastCommittedTableVersion.get() < 0) {
            // normal run
            return groupCommittablesByCheckpointInterval(globalCommittables);
        } else {
            // processing recovery, deduplication on recovered committables.
            Collection<CheckpointData> deDuplicateData =
                deduplicateFiles(globalCommittables, deltaLog, lastCommittedTableVersion.get());

            return groupCommittablesByCheckpointInterval(deDuplicateData);
        }
    }

    /**
     * Filters the given list of globalCommittables to exclude any committables already present in
     * the delta log.
     *
     * @param globalCommittables {@link DeltaGlobalCommittable} to deduplicate.
     * @param deltaLog {@link DeltaLog} instance used for deduplication check.
     * @param tableVersion Delta table version to get changes from.
     * @return collection of {@link CheckpointData}
     */
    private Collection<CheckpointData> deduplicateFiles(
            List<DeltaGlobalCommittable> globalCommittables,
            DeltaLog deltaLog,
            long tableVersion) {

        LOG.info(
            "Processing what it seems like, a first commit. This can be first commit ever for "
                + "this job or first commit after recovery.");

        Map<String, CheckpointData> filePathToActionMap = new HashMap<>();

        try {
            for (DeltaGlobalCommittable globalCommittable : globalCommittables) {
                for (DeltaCommittable committable : globalCommittable.getDeltaCommittables()) {
                    AddFile addFile = committable.getDeltaPendingFile().toAddFile();
                    filePathToActionMap.put(
                        ConnectorUtils.tryRelativizePath(
                            deltaLog.getPath().getFileSystem(conf),
                            deltaLog.getPath(),
                            new org.apache.hadoop.fs.Path(addFile.getPath())
                        ),
                        new CheckpointData(committable, addFile)
                    );
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(
                String.format(
                    "Exception in Delta Sink, during iterating over Committable data for table "
                        + "path {%s}",
                    deltaLog.getPath().toUri().toString()), e);
        }

        // failOnDataLoss=true
        Iterator<VersionLog> changes = deltaLog.getChanges(tableVersion, true);

        StringJoiner duplicatedFiles = new StringJoiner(", ");
        while (changes.hasNext()) {
            VersionLog versionLog = changes.next();
            try (CloseableIterator<Action> actionsIterator = versionLog.getActionsIterator()) {
                actionsIterator.forEachRemaining(action -> {
                    if (action instanceof AddFile) {
                        CheckpointData remove =
                            filePathToActionMap.remove(((AddFile) action).getPath());
                        if (remove != null) {
                            // this AddFile has already been committed to the delta log.
                            duplicatedFiles.add(remove.addFile.getPath());
                        }
                    }
                });
            } catch (IOException e) {
                throw new RuntimeException(
                    String.format("Exception in Delta Sink, during iterating over Delta table "
                    + "changes for table path {%s}", deltaLog.getPath().toUri().toString()), e);
            }
        }

        LOG.info(
            "Files ignored after deduplication for first commit [" + duplicatedFiles + "]"
        );
        return filePathToActionMap.values();
    }

    /**
     * Prepares a Delta commit with checkpoint data containing {@link AddFile} actions that should
     * be added to the delta log.
     * <p>
     * Additionally, during the iteration process we also validate whether the checkpointData
     * for the same checkpoint interval have the same set of partition columns and throw a
     * {@link RuntimeException} when this condition is not met. At the final stage we handle the
     * metadata update along with preparing the final set of metrics and perform the actual commit
     * to the {@link DeltaLog}.
     *
     * @param transaction  {@link OptimisticTransaction} instance that will be used for
     *                     committing given checkpoint interval
     * @param checkpointData list of checkpointData for particular checkpoint interval
     * @param tableExists  indicator whether table already exists or will be created with the next
     *                     commit
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
                        " does not comply with partition columns from other checkpointData: " +
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
            commitActions.size() - 1, //taking account one SetTransaction action
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
     * Log based on log level, GlobalCommitter information about data that will be committed to
     * _delta_log.
     */
    private void logGlobalCommitterData(String appId, long checkpointId, StringJoiner logFiles) {
        if (LOG.isInfoEnabled()) {
            LOG.info(
                logFiles.length() + " files to be committed to the Delta table for " +
                    "appId=" + appId +
                    " checkpointId=" + checkpointId + ".");
        }

        // This will log path for all files that should be committed to delta log.
        if (LOG.isDebugEnabled()) {
            LOG.debug("Files to be committed to the Delta table: " +
                "appId=" + appId +
                " checkpointId=" + checkpointId +
                " files [" + logFiles + "].");
        }
    }

    /**
     * Resolves whether to add the {@link Metadata} object to the transaction.
     *
     * <p>During this process:
     * <ol>
     *   <li>first we prepare metadata {@link Action} object using provided {@link RowType} (and
     *       converting it to {@link StructType}) and partition values,
     *   <li>then we compare the schema from above metadata with the current table's schema,
     *   <li>resolved metadata object is added to the transaction only when it's the first commit to
     *       the given Delta table or when the schemas are not matching but the sink was provided
     *       with option {@link DeltaGlobalCommitter#mergeSchema} set to true (the commit
     *       may still fail though if the Delta Standalone Writer will determine that the schemas
     *       are not compatible),
     *   <li>if the schemas are not matching and {@link DeltaGlobalCommitter#mergeSchema}
     *       was set to false then we throw an exception
     *   <li>if the schemas are matching then we do nothing and let the transaction proceed
     * </ol>
     * <p>
     *
     * @param tableExists      indicator whether table already exists or will be created with the
     *                         next commit
     * @param transaction      DeltaLog's transaction object
     * @param partitionColumns list of partitions for the current data stream
     */
    private void handleMetadataUpdate(boolean tableExists,
                                      OptimisticTransaction transaction,
                                      List<String> partitionColumns) {
        Metadata currentMetadata = transaction.metadata();
        if (tableExists && (!partitionColumns.equals(currentMetadata.getPartitionColumns()))) {
            throw new RuntimeException(
                "Stream's partition columns are different from table's partitions columns. \n" +
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
            String printableCurrentTableSchema = currentTableSchema == null ? "null"
                : currentTableSchema.toPrettyJson();
            String printableCommitSchema = streamSchema.toPrettyJson();

            throw new RuntimeException(
                "DataStream's schema is different from current table's schema. \n" +
                    "provided: " + printableCurrentTableSchema + "\n" +
                    "is different from: " + printableCommitSchema);
        }
    }

    private boolean areSchemasEqual(@Nullable StructType first,
                                    @Nullable StructType second) {
        if (first == null || second == null) {
            return false;
        }
        return first.toJson().equals(second.toJson());
    }

    /**
     * Constructs {@link SetTransaction} action for given Delta commit.
     * <p>
     * This SetTransaction will be used during recovery (if such a failure & recovery occurs). If
     * this current transaction T is at readVersion N, then the earliest delta table version this
     * transaction can commit into is N+1. They can't be in version N. So, if we were to recover,
     * and we are unsure if the committables in transaction T were added to the delta log, then
     * the earliest we would need to scan (getChanges) from is version N+1.
     *
     * @param appId       unique identifier of the application
     * @param readVersion current readVersion
     * @return {@link SetTransaction} object for next Delta commit.
     */
    private SetTransaction prepareSetTransactionAction(String appId, long readVersion) {
        return new SetTransaction(
            appId,
            // delta table version after committing this transaction will be at least
            // readVersion + 1;
            readVersion + 1,
            Optional.of(System.currentTimeMillis())
        );
    }

    /**
     * Prepares {@link Operation} object for current transaction
     *
     * @param partitionColumns partition columns for data in current transaction
     * @param operationMetrics resolved operation metrics for current transaction
     * @return {@link Operation} object for current transaction
     */
    private Operation prepareDeltaLogOperation(List<String> partitionColumns,
                                               Map<String, String> operationMetrics) {
        Map<String, String> operationParameters = new HashMap<>();
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            operationParameters.put("mode", objectMapper.writeValueAsString(APPEND_MODE));
            // we need to perform mapping to JSON object twice for partition columns. First to map
            // the list to string type and then again to make this string JSON encoded
            // e.g. java array of ["a", "b"] will be mapped as string "[\"a\",\"c\"]"
            operationParameters.put("partitionBy", objectMapper.writeValueAsString(
                objectMapper.writeValueAsString(partitionColumns)));
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Cannot map object to JSON", e);
        }
        return new Operation(
            Operation.Name.STREAMING_UPDATE, operationParameters, operationMetrics);
    }

    /**
     * Prepares the map of {@link CheckpointData} grouped per checkpointId.
     * <p>
     * During this process we not only group committables by checkpointId but also flatten
     * collection of {@link DeltaGlobalCommittable} objects (each containing its own collection of
     * {@link DeltaCommittable}).
     *
     * @param globalCommittables list of combined {@link DeltaGlobalCommittable} objects
     * @return sorted map of {@link CheckpointData} grouped by checkpoint id (map keys are sorted
     * in ascending order).
     */
    private SortedMap<Long, List<CheckpointData>> groupCommittablesByCheckpointInterval(
            List<DeltaGlobalCommittable> globalCommittables) {

        SortedMap<Long, List<CheckpointData>> committablesPerCheckpoint = new TreeMap<>();

        for (DeltaGlobalCommittable globalCommittable : globalCommittables) {
            for (DeltaCommittable deltaCommittable : globalCommittable.getDeltaCommittables()) {

                final long checkpointId = deltaCommittable.getCheckpointId();
                final AddFile addFile = deltaCommittable.getDeltaPendingFile().toAddFile();
                CheckpointData checkpointData = new CheckpointData(deltaCommittable, addFile);

                if (committablesPerCheckpoint.containsKey(checkpointId)) {
                    committablesPerCheckpoint.get(checkpointId).add(checkpointData);
                } else {
                    List<CheckpointData> addFiles = new LinkedList<>();
                    addFiles.add(checkpointData);
                    committablesPerCheckpoint.put(checkpointId, addFiles);
                }
            }
        }
        return committablesPerCheckpoint;
    }

    /**
     * Prepares the map of {@link CheckpointData} grouped per checkpointId.
     *
     * @param actionsPerCheckpointId collection of {@link CheckpointData} objects.
     * @return sorted map of {@link CheckpointData} grouped by checkpoint id (map keys are sorted in
     * ascending order).
     */
    private SortedMap<Long, List<CheckpointData>> groupCommittablesByCheckpointInterval(
            Collection<CheckpointData> actionsPerCheckpointId) {

        SortedMap<Long, List<CheckpointData>> actionsPerCheckpoint = new TreeMap<>();

        for (CheckpointData action : actionsPerCheckpointId) {
            final long checkpointId = action.committable.getCheckpointId();

            if (actionsPerCheckpoint.containsKey(checkpointId)) {
                actionsPerCheckpoint.get(checkpointId).add(action);
            } else {
                List<CheckpointData> addFiles = new LinkedList<>();
                addFiles.add(action);
                actionsPerCheckpoint.put(checkpointId, addFiles);
            }
        }
        return actionsPerCheckpoint;
    }

    /**
     * Prepares operation metrics to be passed to the constructor of {@link Operation} object for
     * current transaction.
     *
     * @param numAddedFiles  number of added files for current transaction
     * @param numOutputRows  number of rows written for current transaction
     * @param numOutputBytes size in bytes of the written contents for current transaction
     * @return resolved operation metrics for current transaction
     */
    private Map<String, String> prepareOperationMetrics(
            int numAddedFiles,
            long numOutputRows,
            long numOutputBytes) {

        Map<String, String> operationMetrics = new HashMap<>();
        // number of removed files will be supported for different operation modes
        operationMetrics.put(Operation.Metrics.numRemovedFiles, "0");
        operationMetrics.put(Operation.Metrics.numAddedFiles, String.valueOf(numAddedFiles));
        operationMetrics.put(Operation.Metrics.numOutputRows, String.valueOf(numOutputRows));
        operationMetrics.put(Operation.Metrics.numOutputBytes, String.valueOf(numOutputBytes));
        return operationMetrics;
    }

    /**
     * Simple method for comparing the order and equality of keys in two linked sets
     *
     * @param first  instance of linked set to be compared
     * @param second instance of linked set to be compared
     * @return result of the comparison on order and equality of provided sets
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

    @Override
    public void endOfInput() {
    }

    @Override
    public void close() {
    }

    private static class CheckpointData {

        private final AddFile addFile;

        private final DeltaCommittable committable;

        private CheckpointData(DeltaCommittable committable, AddFile addFile) {
            this.addFile = addFile;
            this.committable = committable;
        }
    }
}
