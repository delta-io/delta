package io.delta.flinkv2.sink;

import io.delta.flinkv2.utils.SchemaUtils;
import io.delta.kernel.*;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.types.StructType;
import org.apache.flink.api.connector.sink2.*;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.util.List;

public class DeltaSink implements Sink<RowData>, SupportsCommitter<DeltaCommittable> {

    ///////////////////////
    // Public static API //
    ///////////////////////

    public static DeltaSink forRowData(final Path tablePath, final RowType rowType, final List<String> userProvidedPartitionColumns) {
        return new DeltaSink(tablePath, rowType, userProvidedPartitionColumns);
    }

    ///////////////////
    // Member fields //
    ///////////////////

    private final Engine engine;
    private final Table table;

    private final StructType writeOperatorSchema;
    private boolean isCreateNewTable;
    private final Transaction txn;

    private DeltaSink(final Path tablePath, final RowType rowType, final List<String> userProvidedPartitionColumns) {
        System.out.println(
            String.format(
                "Scott > DeltaSink > constructor :: tablePath=%s, rowType=%s, userProvidedPartitionColumns=%s",
                tablePath,
                rowType,
                userProvidedPartitionColumns)
        );

        this.engine = DefaultEngine.create(new Configuration());
        this.table = Table.forPath(engine, tablePath.getPath());

        this.writeOperatorSchema = SchemaUtils.toDeltaDataType(rowType);

        System.out.println(
            String.format("Scott > DeltaSink > constructor :: writeOperatorSchema=%s", writeOperatorSchema)
        );

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
            this.isCreateNewTable = false;
        } catch (Exception e) {
           this.isCreateNewTable = true;
        }

        TransactionBuilder txnBuilder =
            table.createTransactionBuilder(
                engine,
                "FlinkV2",
                this.isCreateNewTable ? Operation.CREATE_TABLE : Operation.WRITE
            );

        if (isCreateNewTable) {
            // For a new table set the table schema in the transaction builder
            txnBuilder = txnBuilder
                .withSchema(engine, writeOperatorSchema)
                .withPartitionColumns(engine, userProvidedPartitionColumns);
        }

        this.txn = txnBuilder.build(engine);
    }

    /////////////////
    // Public APIs //
    /////////////////

    @Override
    public SinkWriter<RowData> createWriter(InitContext context) throws IOException {
        System.out.println("Scott > DeltaSink > createWriter");
        return new DeltaSinkWriter();
    }

    @Override
    public Committer<DeltaCommittable> createCommitter(CommitterInitContext context) throws IOException {
        System.out.println("Scott > DeltaSink > createCommitter");
        return new DeltaCommitter();
    }

    @Override
    public SimpleVersionedSerializer<DeltaCommittable> getCommittableSerializer() {
        return null;
    }
}
