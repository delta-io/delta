package io.delta.flinkv2.sink;

import io.delta.kernel.*;
import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterable;
import io.delta.kernel.utils.CloseableIterator;
import org.apache.flink.api.connector.sink2.Committer;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * The Committer is responsible for committing the data staged by the CommittingSinkWriter in the
 * second step of a two-phase commit protocol.
 * <p>
 * A commit must be idempotent: If some failure occurs in Flink during commit phase, Flink will
 * restart from previous checkpoint and re-attempt to commit all committables. Thus, some or all
 * committables may have already been committed.
 */
public class DeltaCommitter implements Committer<Row> {

    private final String committerId;
    private final String tablePath;
    private final StructType writeOperatorSchema;
    private final List<String> partitionColumns;

    public DeltaCommitter(String tablePath, StructType writeOperatorSchema, List<String> partitionColumns) {
        this.committerId = java.util.UUID.randomUUID().toString();
        this.tablePath = tablePath;
        this.writeOperatorSchema = writeOperatorSchema;
        this.partitionColumns = partitionColumns;

        System.out.println(
            String.format("Scott > DeltaCommitter > constructor :: committerId=%s", committerId)
        );
    }

    @Override
    public void commit(
            Collection<CommitRequest<Row>> committables)
            throws IOException, InterruptedException {
        System.out.println(
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

        System.out.println(
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

        final CloseableIterable<Row> dataActions = new CloseableIterable<Row>() {
            @Override
            public CloseableIterator<Row> iterator() {
                return new CloseableIterator<Row>() {
                    private final Iterator<CommitRequest<Row>> iter = committables.iterator();

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

                        return iter.next().getCommittable();
                    }
                };
            }

            @Override
            public void close() throws IOException {
                // Nothing to close
            }
        };

        TransactionCommitResult result = txn.commit(engine, dataActions);

        System.out.println(
            String.format("Scott > DeltaCommitter[%s] > commit :: resultVersion=%s", committerId, result.getVersion())
        );
    }

    @Override
    public void close() throws Exception {

    }
}
