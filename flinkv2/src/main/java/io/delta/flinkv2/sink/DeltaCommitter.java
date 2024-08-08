package io.delta.flinkv2.sink;

import io.delta.flinkv2.utils.SchemaUtils;
import io.delta.kernel.*;
import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.exceptions.TableNotFoundException;
import io.delta.kernel.internal.SnapshotImpl;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterable;
import io.delta.kernel.utils.CloseableIterator;
import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.table.types.logical.RowType;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * The Committer is responsible for committing the data staged by the CommittingSinkWriter in the
 * second step of a two-phase commit protocol.
 * <p>
 * A commit must be idempotent: If some failure occurs in Flink during commit phase, Flink will
 * restart from previous checkpoint and re-attempt to commit all committables. Thus, some or all
 * committables may have already been committed.
 */
public class DeltaCommitter implements Committer<DeltaCommittable> {

    private static final Logger LOG = LoggerFactory.getLogger(DeltaCommitter.class);

    static int numFailures = 0;

    private final String appId;
    private final String committerId;
    private final String tablePath;
    private final StructType writeOperatorSchema;
    private final List<String> partitionColumns;

    // use this to determine if we are re-committing committables that may or may not have been
    // written to the delta log already
    private final OptionalLong restoredCheckpointId;

    // if restoredCheckpointId.nonEmpty and !completedFirstCommit then do recovery to prevent duplicate writes
    // maybe even just if !completedFirstCommit

    public DeltaCommitter(String appId, String tablePath, RowType writeOperatorSchema, List<String> partitionColumns, OptionalLong restoredCheckpointId) {
        this.appId = appId;
        this.committerId = java.util.UUID.randomUUID().toString();
        this.tablePath = tablePath;
        this.writeOperatorSchema = SchemaUtils.toDeltaDataType(writeOperatorSchema);
        this.partitionColumns = partitionColumns;
        this.restoredCheckpointId = restoredCheckpointId;

        LOG.info(
            String.format("Scott > DeltaCommitter > constructor :: committerId=%s, restoredCheckpointId=%s", committerId, restoredCheckpointId)
        );
    }

    // TODO: group by checkpointId, commit in increasing order
    @Override
    public void commit(
            Collection<CommitRequest<DeltaCommittable>> committables)
            throws IOException, InterruptedException {
        final TreeMap<Long, List<CommitRequest<DeltaCommittable>>> sortedCommittablesByCheckpointId
            = sortCommittablesByCheckpointId(committables);

        for (Map.Entry<Long, List<CommitRequest<DeltaCommittable>>> entry : sortedCommittablesByCheckpointId.entrySet()) {
            if (shouldCommitCheckpointToDeltaLog(entry.getKey())) {
                commitForSingleCheckpointId(entry.getKey(), entry.getValue());
            }
        }
    }

    @Override
    public void close() throws Exception {

    }

    ////////////////////
    // Helper Methods //
    ////////////////////

    // TODO: optimize not duplicating creating engine and table
    private boolean shouldCommitCheckpointToDeltaLog(long checkpointIdToCommit) {
        if (!restoredCheckpointId.isPresent()) {
            LOG.info(
                "Scott > DeltaCommitter[{}] > shouldCommitCheckpointToDeltaLog :: not a recovery scenario, checkpointIdToCommit={}",
                committerId,
                checkpointIdToCommit
            );
            return true; // not a restore scenario
        }

        if (restoredCheckpointId.getAsLong() != checkpointIdToCommit) {
            LOG.info(
                "Scott > DeltaCommitter[{}] > shouldCommitCheckpointToDeltaLog :: already (previously) hadnled recovery scenario, return true, checkpointIdToCommit={}, restoredCheckpointId={}",
                committerId,
                checkpointIdToCommit,
                restoredCheckpointId.getAsLong()
            );

            return true; // normal operation
        }

        try {
            final Engine engine = DefaultEngine.create(new Configuration());
            final Table table = Table.forPath(engine, tablePath);
            final SnapshotImpl latestSnapshot = (SnapshotImpl) table.getLatestSnapshot(engine);
            final Optional<Long> latestCheckpointIdCommittedToLoad =
                latestSnapshot.getLatestTransactionVersion(engine, appId);

            if (!latestCheckpointIdCommittedToLoad.isPresent()) {
                LOG.info(
                    "Scott > DeltaCommitter[{}] > shouldCommitCheckpointToDeltaLog :: no latest txn id, return true, checkpointIdToCommit={}",
                    committerId,
                    checkpointIdToCommit
                );
                return true;
            }

            LOG.info(
                "Scott > DeltaCommitter[{}] > shouldCommitCheckpointToDeltaLog :: latestTxnId={}, checkpointIdToCommit={}, shouldCommit={}",
                committerId,
                latestCheckpointIdCommittedToLoad.get(),
                checkpointIdToCommit,
                checkpointIdToCommit > latestCheckpointIdCommittedToLoad.get()
            );

            return checkpointIdToCommit > latestCheckpointIdCommittedToLoad.get();
        } catch (TableNotFoundException ex) {
            LOG.info(
                "Scott > DeltaCommitter[{}] > shouldCommitCheckpointToDeltaLog :: table doesn't exist, return true, checkpointIdToCommit={}",
                committerId,
                checkpointIdToCommit
            );
            return true;
        }
    }

    private void commitForSingleCheckpointId(
            long checkpointId, List<CommitRequest<DeltaCommittable>> committables) {
        LOG.info(
            String.format("Scott > DeltaCommitter[%s] > commitSingleCheckpointId :: checkpointId=%s, committablesSize=%s", checkpointId, committerId, committables.size())
        );

        final Engine engine = DefaultEngine.create(new Configuration());
        final Table table = Table.forPath(engine, tablePath);
        boolean isCreateNewTable;

        try {
            final Snapshot latestSnapshot = table.getLatestSnapshot(engine);
            final StructType tableSchema = latestSnapshot.getSchema(engine);
            if (!tableSchema.equivalent(writeOperatorSchema)) {
                throw new RuntimeException(
                    String.format(
                        "Table Schema does not match Write Operator Schema.\nTable schema: %s\nWrite Operator schema: %s",
                        tableSchema,
                        writeOperatorSchema
                    )
                );
            }
            isCreateNewTable = false;
        } catch (Exception e) {
            isCreateNewTable = true;
        }

        LOG.info(
            String.format("Scott > DeltaCommitter[%s] > commitSingleCheckpointId :: isCreateNewTable=%s", committerId, isCreateNewTable)
        );

        TransactionBuilder txnBuilder =
            table.createTransactionBuilder(
                engine,
                "FlinkV2",
                isCreateNewTable ? Operation.CREATE_TABLE : Operation.WRITE
            )
            .withTransactionId(engine, appId, checkpointId);;

        if (isCreateNewTable) {
            // For a new table set the table schema in the transaction builder
            txnBuilder = txnBuilder
                .withSchema(engine, writeOperatorSchema)
                .withPartitionColumns(engine, partitionColumns);
        }

        final Transaction txn = txnBuilder.build(engine);

        final Set<Long> checkpointIds = new HashSet<>();
        final Set<String> writerIds = new HashSet<>();

        final CloseableIterable<Row> dataActions = new CloseableIterable<Row>() {
            @Override
            public CloseableIterator<Row> iterator() {
                return new CloseableIterator<Row>() {
                    private final Iterator<CommitRequest<DeltaCommittable>> iter = committables.iterator();

                    @Override
                    public void close() throws IOException {
                        // Nothing to close
                    }

                    @Override
                    public boolean hasNext() {
                        return iter.hasNext();
                    }

                    @Override
                    public Row next() {
                        if (!hasNext()) throw new NoSuchElementException();

                        final CommitRequest<DeltaCommittable> commitRequest = iter.next();
                        final DeltaCommittable deltaCommittable = commitRequest.getCommittable();

                        LOG.info(
                            String.format("Scott > DeltaCommitter[%s] > commitSingleCheckpointId :: retryNumber=%s, %s", committerId, commitRequest.getNumberOfRetries(), deltaCommittable)
                        );

                        checkpointIds.add(deltaCommittable.getCheckpointId());
                        writerIds.add(deltaCommittable.getWriterId());

                        return deltaCommittable.getKernelActionRow();
                    }
                };
            }

            @Override
            public void close() throws IOException {
                // Nothing to close
            }
        };

//        if (java.util.concurrent.ThreadLocalRandom.current().nextInt() % 5 == 0) {
//            throw new RuntimeException(
//                String.format("!!!!! RANDOM FAILURE ---- Scott > DeltaCommitter[%s] > commitSingleCheckpointId [BEFORE txn.commit]", committerId)
//            );
//        }

        TransactionCommitResult result = txn.commit(engine, dataActions);

        LOG.info(
            String.format("Scott > DeltaCommitter[%s] > commitSingleCheckpointId :: resultVersion=%s, checkpointIds=%s, writerIds=%s", committerId, result.getVersion(), checkpointIds, writerIds)
        );

//        if (numFailures < 1 && result.getVersion() > 0 && java.util.concurrent.ThreadLocalRandom.current().nextInt() % 2 == 0) {
//            numFailures++;
//            throw new RuntimeException(
//                String.format("!!!!! RANDOM FAILURE ---- Scott > DeltaCommitter[%s] > commitSingleCheckpointId [AFTER txn.commit] :: resultVersion=%s, checkpointIds=%s, writerIds=%s", committerId, result.getVersion(), checkpointIds, writerIds)
//            );
//        }

    }

    private static TreeMap<Long, List<CommitRequest<DeltaCommittable>>> sortCommittablesByCheckpointId(
            Collection<CommitRequest<DeltaCommittable>> committables) {
        return committables
            .stream()
            .collect(Collectors.groupingBy(
                commitRequest -> commitRequest.getCommittable().getCheckpointId(),
                TreeMap::new,
                Collectors.toList())
            );
    }
}
