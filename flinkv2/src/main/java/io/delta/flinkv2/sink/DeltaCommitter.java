package io.delta.flinkv2.sink;

import io.delta.flinkv2.utils.SchemaUtils;
import io.delta.kernel.*;
import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
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

    private final String committerId;
    private final String tablePath;
    private final StructType writeOperatorSchema;
    private final List<String> partitionColumns;

    // use this to determine if we are re-committing committables that may or may not have been
    // written to the delta log already
    private final OptionalLong restoredCheckpointId;

    private boolean completedFirstCommit = false;

    // if restoredCheckpointId.nonEmpty and !completedFirstCommit then do recovery to prevent duplicate writes
    // maybe even just if !completedFirstCommit

    public DeltaCommitter(String tablePath, RowType writeOperatorSchema, List<String> partitionColumns, OptionalLong restoredCheckpointId) {
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
        LOG.info(
            String.format("Scott > DeltaCommitter[%s] > commit :: committablesSize=%s", committerId, committables.size())
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
            String.format("Scott > DeltaCommitter[%s] > commit :: isCreateNewTable=%s", committerId, isCreateNewTable)
        );

        TransactionBuilder txnBuilder =
            table.createTransactionBuilder(
                engine,
                "FlinkV2",
                isCreateNewTable ? Operation.CREATE_TABLE : Operation.WRITE
            );

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
                            String.format("Scott > DeltaCommitter[%s] > commit :: retryNumber=%s, %s", committerId, commitRequest.getNumberOfRetries(), deltaCommittable)
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
//                String.format("!!!!! RANDOM FAILURE ---- Scott > DeltaCommitter[%s] > commit [BEFORE txn.commit]", committerId)
//            );
//        }

        TransactionCommitResult result = txn.commit(engine, dataActions);

        LOG.info(
            String.format("Scott > DeltaCommitter[%s] > commit :: resultVersion=%s, checkpointIds=%s, writerIds=%s", committerId, result.getVersion(), checkpointIds, writerIds)
        );

//        if (numFailures < 1 && result.getVersion() > 0 && java.util.concurrent.ThreadLocalRandom.current().nextInt() % 2 == 0) {
//            numFailures++;
//            throw new RuntimeException(
//                String.format("!!!!! RANDOM FAILURE ---- Scott > DeltaCommitter[%s] > commit [AFTER txn.commit] :: resultVersion=%s, checkpointIds=%s, writerIds=%s", committerId, result.getVersion(), checkpointIds, writerIds)
//            );
//        }
    }

    @Override
    public void close() throws Exception {

    }
}
