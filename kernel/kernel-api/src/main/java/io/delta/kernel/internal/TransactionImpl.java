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
package io.delta.kernel.internal;

import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.util.*;
import java.util.stream.Collectors;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.delta.kernel.*;
import io.delta.kernel.commit.CommitFailedException;
import io.delta.kernel.commit.UpdatedActions;
import io.delta.kernel.data.Row;
import io.delta.kernel.engine.CommitCoordinatorClientHandler;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.exceptions.ConcurrentWriteException;
import io.delta.kernel.expressions.Column;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterable;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.internal.actions.*;
import io.delta.kernel.internal.data.TransactionStateRow;
import io.delta.kernel.internal.fs.Path;
import io.delta.kernel.internal.replay.ConflictChecker;
import io.delta.kernel.internal.replay.ConflictChecker.TransactionRebaseState;
import io.delta.kernel.internal.util.*;
import static io.delta.kernel.internal.TableConfig.*;
import static io.delta.kernel.internal.actions.SingleAction.*;
import static io.delta.kernel.internal.util.Preconditions.checkArgument;
import static io.delta.kernel.internal.util.Preconditions.checkState;
import static io.delta.kernel.internal.util.Utils.toCloseableIterator;

public class TransactionImpl
        implements Transaction {
    private static final Logger logger = LoggerFactory.getLogger(TransactionImpl.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static final int DEFAULT_READ_VERSION = 1;
    public static final int DEFAULT_WRITE_VERSION = 2;

    /**
     * Number of retries for concurrent write exceptions to resolve conflicts and retry commit. In
     * Delta-Spark, for historical reasons the number of retries is really high (10m). We are
     * starting with a lower number for now. If this is not sufficient we can update it.
     */
    private static final int NUM_TXN_RETRIES = 200;

    private final UUID txnId = UUID.randomUUID();

    private final boolean isNewTable; // the transaction is creating a new table
    private final String engineInfo;
    private final Operation operation;
    private final Path dataPath;
    private final Path logPath;
    private final Protocol protocol;
    private final SnapshotImpl readSnapshot;
    private final Optional<SetTransaction> setTxnOpt;
    private final boolean shouldUpdateProtocol;
    private final Clock clock;
    private Metadata metadata;
    private boolean shouldUpdateMetadata;

    private boolean closed; // To avoid trying to commit the same transaction again.

    public TransactionImpl(
            boolean isNewTable,
            Path dataPath,
            Path logPath,
            SnapshotImpl readSnapshot,
            String engineInfo,
            Operation operation,
            Protocol protocol,
            Metadata metadata,
            Optional<SetTransaction> setTxnOpt,
            boolean shouldUpdateMetadata,
            boolean shouldUpdateProtocol,
            Clock clock) {
        this.isNewTable = isNewTable;
        this.dataPath = dataPath;
        this.logPath = logPath;
        this.readSnapshot = readSnapshot;
        this.engineInfo = engineInfo;
        this.operation = operation;
        this.protocol = protocol;
        this.metadata = metadata;
        this.setTxnOpt = setTxnOpt;
        this.shouldUpdateMetadata = shouldUpdateMetadata;
        this.shouldUpdateProtocol = shouldUpdateProtocol;
        this.clock = clock;
    }

    @Override
    public Row getTransactionState(Engine engine) {
        return TransactionStateRow.of(metadata, dataPath.toString());
    }

    @Override
    public List<String> getPartitionColumns(Engine engine) {
        return VectorUtils.toJavaList(metadata.getPartitionColumns());
    }

    @Override
    public StructType getSchema(Engine engine) {
        return readSnapshot.getSchema(engine);
    }

    @Override
    public TransactionCommitResult commit(Engine engine, CloseableIterable<Row> dataActions)
            throws ConcurrentWriteException {
        try {
            checkState(!closed,
                    "Transaction is already attempted to commit. Create a new transaction.");

            long commitAsVersion = readSnapshot.getVersion(engine) + 1;
            // Generate the commit action with the inCommitTimestamp if ICT is enabled.
            CommitInfo attemptCommitInfo = generateCommitAction(engine);
            updateMetadataWithCoordinatedCommitsConfs(engine);
            updateMetadataWithICTIfRequired(engine, attemptCommitInfo);
            int numRetries = 0;
            do {
                logger.info("Committing transaction as version = {}.", commitAsVersion);
                try {
                    return doCommit(engine, commitAsVersion, attemptCommitInfo, dataActions);
                } catch (FileAlreadyExistsException fnfe) {
                    logger.info("Concurrent write detected when committing as version = {}. " +
                            "Trying to resolve conflicts and retry commit.", commitAsVersion);
                    TransactionRebaseState rebaseState = ConflictChecker
                            .resolveConflicts(engine, readSnapshot, commitAsVersion, this);
                    long newCommitAsVersion = rebaseState.getLatestVersion() + 1;
                    checkArgument(commitAsVersion < newCommitAsVersion,
                            "New commit version %d should be greater than the previous commit " +
                                    "attempt version %d.", newCommitAsVersion, commitAsVersion);
                    commitAsVersion = newCommitAsVersion;
                    Optional<Long> updatedInCommitTimestamp =
                            getUpdatedInCommitTimestampAfterConflict(
                                    rebaseState.getLatestCommitTimestamp(),
                                    attemptCommitInfo.getInCommitTimestamp());
                    if (updatedInCommitTimestamp.isPresent()) {
                        Optional<Metadata> metadataWithICTInfo =
                                getMetadataWithUpdatedICTEnablementInfo(
                                        engine,
                                        updatedInCommitTimestamp.get(),
                                        metadata,
                                        rebaseState.getLatestVersion()
                                );
                        metadataWithICTInfo.ifPresent(this::updateMetadata);
                    }
                    attemptCommitInfo.setInCommitTimestamp(updatedInCommitTimestamp);
                }
                numRetries++;
            } while (numRetries < NUM_TXN_RETRIES);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        } finally {
            closed = true;
        }

        // we have exhausted the number of retries, give up.
        logger.info("Exhausted maximum retries ({}) for committing transaction.", NUM_TXN_RETRIES);
        throw new ConcurrentWriteException();
    }

    private void updateMetadata(Metadata metadata) {
        logger.info(
                "Updated metadata from {} to {}",
                shouldUpdateMetadata ? this.metadata : "-", metadata);
        this.metadata = metadata;
        this.shouldUpdateMetadata = true;
    }

    private void updateMetadataWithICTIfRequired(Engine engine, CommitInfo attemptCommitInfo) {
        // If ICT is enabled for the current transaction, update the metadata with the ICT
        // enablement info.
        attemptCommitInfo.getInCommitTimestamp().ifPresent(
                inCommitTimestamp -> {
                    Optional<Metadata> metadataWithICTInfo =
                            InCommitTimestampUtils.getUpdatedMetadataWithICTEnablementInfo(
                                    engine,
                                    inCommitTimestamp,
                                    readSnapshot,
                                    metadata,
                                    readSnapshot.getVersion(engine) + 1L);
                    metadataWithICTInfo.ifPresent(this::updateMetadata);
                }
        );
    }

    /**
     * This method makes the necessary changes to Metadata based on coordinated-commits: If the
     * table is being converted from file-system to coordinated commits, then it registers the table
     * with the commit-coordinator and updates the Metadata with the necessary configuration
     * information from the commit-coordinator.
     *
     * @return A boolean which represents whether we have updated the table Metadata with
     *         coordinated-commits information. If no changed were made, returns false.
     */
    private void updateMetadataWithCoordinatedCommitsConfs(Engine engine)
            throws JsonProcessingException {
        validateCoordinatedCommitsConfInMetadata();
        Optional<Map<String, String>> newCoordinatedCommitsTableConfOpt =
                registerTableForCoordinatedCommitsIfNeeded(engine, metadata, protocol);
        if (newCoordinatedCommitsTableConfOpt.isPresent()) {
            Map<String, String> newCoordinatedCommitsTableConf =
                    newCoordinatedCommitsTableConfOpt.get();

            // FS to CC conversion
            String coordinatedCommitsTableConfJson = OBJECT_MAPPER.writeValueAsString(
                    newCoordinatedCommitsTableConf);
            String extraKVConfKey = COORDINATED_COMMITS_TABLE_CONF.getKey();
            Map<String, String> coordinatedCommitsTableConfProperty =
                    new HashMap<String, String>() {{
                        put(extraKVConfKey, coordinatedCommitsTableConfJson);
                        }};
            updateMetadata(metadata.withNewConfiguration(coordinatedCommitsTableConfProperty));
        }
    }

    private void validateCoordinatedCommitsConfInMetadata() {
        // Validate that the COORDINATED_COMMITS_COORDINATOR_CONF is JSON parse-able.
        // Also do this validation if this table property has changed.
        String newCoordinatedCommitsConf = metadata.getConfiguration()
                .get(TableConfig.COORDINATED_COMMITS_COORDINATOR_CONF.getKey());
        String oldCoordinatedCommitsConf = readSnapshot.getMetadata().getConfiguration()
                .get(TableConfig.COORDINATED_COMMITS_COORDINATOR_CONF.getKey());
        if (!Objects.equals(newCoordinatedCommitsConf, oldCoordinatedCommitsConf)) {
            TableConfig.COORDINATED_COMMITS_COORDINATOR_CONF.fromMetadata(metadata);
        }
    }

    /**
     * This method registers the table with the commit-coordinator via the
     * {@link CommitCoordinatorClientHandler} if the table is transitioning from file-system based
     * table to coordinated-commits table.
     * @param finalMetadata the effective {@link Metadata} of the table. Note that this refers to
     *                      the new metadata if this commit is updating the table Metadata.
     * @param finalProtocol the effective {@link Protocol} of the table. Note that this refers to
     *                      the new protocol if this commit is updating the table Protocol.
     * @return The new coordinated-commits table metadata if the table is transitioning from
     *         file-system based table to coordinated-commits table. Otherwise, None.
     *         This metadata should be added to the Metadata.configuration before doing the
     *         commit.
     */
    protected Optional<Map<String, String>> registerTableForCoordinatedCommitsIfNeeded(
           Engine engine, Metadata finalMetadata, Protocol finalProtocol) {
        Optional<Map<String, String>> newCoordinatedCommitsTableConf = Optional.empty();

        Tuple2<Optional<String>, Map<String, String>> newCommitCoordinatorInfo =
                CoordinatedCommitsUtils.getCoordinatedCommitsConfs(finalMetadata);
        Optional<String> newCommitCoordinatorName = newCommitCoordinatorInfo._1;
        Map<String, String> newCommitCoordinatorConf = newCommitCoordinatorInfo._2;

        Tuple2<Optional<String>, Map<String, String>> oldCommitCoordinatorInfo =
                CoordinatedCommitsUtils.getCoordinatedCommitsConfs(
                        readSnapshot.getMetadata());
        Optional<String> oldCommitCoordinatorName = oldCommitCoordinatorInfo._1;
        Map<String, String> oldCommitCoordinatorConf = oldCommitCoordinatorInfo._2;

        if (!finalMetadata.getConfiguration().equals(readSnapshot.getMetadata().getConfiguration())
                || readSnapshot.getVersion(engine) == -1L) {
            Optional<CommitCoordinatorClientHandler> oldCommitCoordinatorClientHandlerOpt =
                    readSnapshot.getCommitCoordinatorClientHandlerOpt(engine);
            Optional<CommitCoordinatorClientHandler> newCommitCoordinatorClientHandlerOpt =
                    CoordinatedCommitsUtils
                            .getCommitCoordinatorClientHandler(
                                    engine, finalMetadata, finalProtocol);

            if (newCommitCoordinatorClientHandlerOpt.isPresent() &&
                    !oldCommitCoordinatorClientHandlerOpt.isPresent()) {
                // FS -> CC conversion
                logger.info("Table " + logPath + " transitioning from " +
                        "file-system based table to coordinated-commits table: " +
                        "[commit-coordinator: " + newCommitCoordinatorName.get() +
                        ", conf: " + newCommitCoordinatorConf + "]");
                newCoordinatedCommitsTableConf = Optional.of(newCommitCoordinatorClientHandlerOpt
                        .get()
                        .registerTable(logPath.toString(),
                                readSnapshot.getVersion(engine),
                                CoordinatedCommitsUtils.convertMetadataToAbstractMetadata(
                                        readSnapshot.getMetadata()),
                                CoordinatedCommitsUtils.convertProtocolToAbstractProtocol(
                                        readSnapshot.getProtocol())));
            } else if (!newCommitCoordinatorClientHandlerOpt.isPresent() &&
                    oldCommitCoordinatorClientHandlerOpt.isPresent()) {
                // CC -> FS conversion
                logger.info("Table " + logPath + " transitioning from " +
                        "coordinated-commits table to file-system table: " +
                        "[commit-coordinator: " + oldCommitCoordinatorName.get() +
                        ", conf: " + oldCommitCoordinatorConf + "]");
            } else if (newCommitCoordinatorClientHandlerOpt.isPresent() &&
                    oldCommitCoordinatorClientHandlerOpt.isPresent() &&
                    !oldCommitCoordinatorClientHandlerOpt.get()
                            .semanticEquals(newCommitCoordinatorClientHandlerOpt.get())) {
                // CC1 -> CC2 conversion is not allowed
                String message = "Transition of table " + logPath + " from one commit-coordinator" +
                        " to another commit-coordinator is not allowed: [old commit-coordinator: " +
                        oldCommitCoordinatorName.get() + ", new commit-coordinator: " +
                        newCommitCoordinatorName.get() + ", old commit-coordinator conf: " +
                        oldCommitCoordinatorConf + ", new commit-coordinator conf: " +
                        newCommitCoordinatorConf + "].";
                throw new IllegalStateException(message);
            }
            // no owner change
        }
        return newCoordinatedCommitsTableConf;
    }

    private Optional<Long> getUpdatedInCommitTimestampAfterConflict(
            long winningCommitTimestamp, Optional<Long> attemptInCommitTimestamp) {
        if (attemptInCommitTimestamp.isPresent()) {
            long updatedInCommitTimestamp = Math.max(
                    attemptInCommitTimestamp.get(), winningCommitTimestamp + 1);
            return Optional.of(updatedInCommitTimestamp);
        }
        return attemptInCommitTimestamp;
    }

    private Optional<Metadata> getMetadataWithUpdatedICTEnablementInfo(
            Engine engine,
            Long updatedInCommitTimestamp,
            Metadata attemptMetadata,
            Long lastWinningVersion) {
        long nextAvailableVersion = lastWinningVersion + 1L;
        return InCommitTimestampUtils.getUpdatedMetadataWithICTEnablementInfo(
                engine,
                updatedInCommitTimestamp,
                readSnapshot,
                attemptMetadata,
                nextAvailableVersion);
    }

    private TransactionCommitResult doCommit(
            Engine engine,
            long commitAsVersion,
            CommitInfo attemptCommitInfo,
            CloseableIterable<Row> dataActions)
            throws FileAlreadyExistsException {
        List<Row> metadataActions = new ArrayList<>();
        metadataActions.add(createCommitInfoSingleAction(attemptCommitInfo.toRow()));
        if (shouldUpdateMetadata || isNewTable) {
            metadataActions.add(createMetadataSingleAction(metadata.toRow()));
        }
        if (shouldUpdateProtocol || isNewTable) {
            // In the future, we need to add metadata and action when there are any changes to them.
            metadataActions.add(createProtocolSingleAction(protocol.toRow()));
        }
        setTxnOpt.ifPresent(setTxn -> metadataActions.add(createTxnSingleAction(setTxn.toRow())));

        try (CloseableIterator<Row> stageDataIter = dataActions.iterator()) {
            // Create a new CloseableIterator that will return the metadata actions followed by the
            // data actions.
            CloseableIterator<Row> dataAndMetadataActions =
                    toCloseableIterator(metadataActions.iterator()).combine(stageDataIter);

            if (commitAsVersion == 0) {
                // New table, create a delta log directory
                if (!engine.getFileSystemClient().mkdirs(logPath.toString())) {
                    throw new RuntimeException(
                            "Failed to create delta log directory: " + logPath);
                }
            }

            // Write the staged data to a delta file
            Optional<CommitCoordinatorClientHandler> commitCoordinatorClientHandlerOpt =
                    readSnapshot.getCommitCoordinatorClientHandlerOpt(engine);
            if (commitCoordinatorClientHandlerOpt.isPresent()) {
                commitCoordinatorClientHandlerOpt.get().commit(
                        logPath.toString(),
                        COORDINATED_COMMITS_TABLE_CONF.fromMetadata(readSnapshot.getMetadata()),
                        commitAsVersion,
                        dataAndMetadataActions,
                        new UpdatedActions(
                                CoordinatedCommitsUtils.convertCommitInfoToAbstractCommitInfo(
                                        attemptCommitInfo),
                                CoordinatedCommitsUtils.convertMetadataToAbstractMetadata(metadata),
                                CoordinatedCommitsUtils.convertProtocolToAbstractProtocol(protocol),
                                CoordinatedCommitsUtils.convertMetadataToAbstractMetadata(
                                        readSnapshot.getMetadata()),
                                CoordinatedCommitsUtils.convertProtocolToAbstractProtocol(
                                        readSnapshot.getProtocol())));
            } else {
                // If the table is not registered with the commit-coordinator, then we need to write
                // the commit file to the table.
                engine.getJsonHandler().writeJsonFileAtomically(
                        FileNames.deltaFile(logPath, commitAsVersion),
                        dataAndMetadataActions,
                        false /* overwrite */);
            }

            return new TransactionCommitResult(
                    commitAsVersion,
                    isReadyForCheckpoint(commitAsVersion));
        } catch (FileAlreadyExistsException e) {
            throw e;
        } catch (IOException | CommitFailedException ioe) {
            throw new RuntimeException(ioe);
        }
    }

    public boolean isBlindAppend() {
        // For now, Kernel just supports blind append.
        // Change this when read-after-write is supported.
        return true;
    }

    public Optional<SetTransaction> getSetTxnOpt() {
        return setTxnOpt;
    }

    /**
     * Generates a timestamp which is greater than the commit timestamp of the readSnapshot. This
     * can result in an additional file read and that this will only happen if ICT is enabled.
     */
    private Optional<Long> generateInCommitTimestampForFirstCommitAttempt(
            Engine engine, long currentTimestamp) {
        if (IN_COMMIT_TIMESTAMPS_ENABLED.fromMetadata(metadata)) {
            long lastCommitTimestamp = readSnapshot.getTimestamp(engine);
            return Optional.of(Math.max(currentTimestamp, lastCommitTimestamp + 1));
        } else {
            return Optional.empty();
        }
    }

    private CommitInfo generateCommitAction(Engine engine) {
        long commitAttemptStartTime = clock.getTimeMillis();
        return new CommitInfo(
                generateInCommitTimestampForFirstCommitAttempt(
                        engine, commitAttemptStartTime),
                commitAttemptStartTime, /* timestamp */
                "Kernel-" + Meta.KERNEL_VERSION + "/" + engineInfo, /* engineInfo */
                operation.getDescription(), /* description */
                getOperationParameters(), /* operationParameters */
                isBlindAppend(), /* isBlindAppend */
                txnId.toString() /* txnId */
        );
    }

    private boolean isReadyForCheckpoint(long newVersion) {
        int checkpointInterval = CHECKPOINT_INTERVAL.fromMetadata(metadata);
        return newVersion > 0 && newVersion % checkpointInterval == 0;
    }

    private Map<String, String> getOperationParameters() {
        if (isNewTable) {
            List<String> partitionCols = VectorUtils.toJavaList(metadata.getPartitionColumns());
            String partitionBy = partitionCols.stream()
                    .map(col -> "\"" + col + "\"")
                    .collect(Collectors.joining(",", "[", "]"));
            return Collections.singletonMap("partitionBy", partitionBy);
        }
        return Collections.emptyMap();
    }

    /**
     * Get the part of the schema of the table that needs the statistics to be collected per file.
     *
     * @param engine           {@link Engine} instance to use.
     * @param transactionState State of the transaction
     * @return
     */
    public static List<Column> getStatisticsColumns(Engine engine, Row transactionState) {
        // TODO: implement this once we start supporting collecting stats
        return Collections.emptyList();
    }
}
