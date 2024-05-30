package io.delta.flinkv2.sink;

import io.delta.flinkv2.utils.SchemaUtils;
import io.delta.kernel.*;
import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.defaults.internal.json.JsonUtils;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.actions.SingleAction;
import io.delta.kernel.types.StructType;
import org.apache.flink.api.connector.sink2.*;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.connector.sink2.SupportsPreCommitTopology;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;

public class DeltaSink implements Sink<RowData>, SupportsCommitter<Row>, SupportsPreCommitTopology<Row, Row>, Serializable {

    ///////////////////////
    // Public static API //
    ///////////////////////

    public static DeltaSink forRowData(final String tablePath, final RowType rowType, final List<String> userProvidedPartitionColumns) {
        return new DeltaSink(tablePath, rowType, userProvidedPartitionColumns);
    }

    ///////////////////
    // Member fields //
    ///////////////////

    private final Engine engine;
    private final Table table;

    private final StructType writeOperatorSchema;
    private final List<String> userProvidedPartitionColumns;
    private boolean isCreateNewTable;
    private final Transaction mockTxn;

    private DeltaSink(final String tablePath, final RowType rowType, final List<String> userProvidedPartitionColumns) {
        System.out.println(
            String.format(
                "Scott > DeltaSink > constructor :: tablePath=%s, rowType=%s, userProvidedPartitionColumns=%s",
                tablePath,
                rowType,
                userProvidedPartitionColumns)
        );

        this.engine = DefaultEngine.create(new Configuration());
        this.table = Table.forPath(engine, tablePath);
        this.writeOperatorSchema = SchemaUtils.toDeltaDataType(rowType);
        this.userProvidedPartitionColumns = userProvidedPartitionColumns;

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

        this.mockTxn = txnBuilder.build(engine);
    }

    /////////////////
    // Public APIs //
    /////////////////

    @Override
    public SinkWriter<RowData> createWriter(InitContext context) throws IOException {
        System.out.println("Scott > DeltaSink > createWriter");
        return new DeltaSinkWriter(
            mockTxn.getTransactionState(engine),
            mockTxn.getPartitionColumns(engine)
        );
    }

    @Override
    public Committer<Row> createCommitter(CommitterInitContext context) throws IOException {
        System.out.println("Scott > DeltaSink > createCommitter");
        return new DeltaCommitter(table.getPath(engine), writeOperatorSchema, userProvidedPartitionColumns);
    }

    @Override
    public DataStream<CommittableMessage<Row>> addPreCommitTopology(
            DataStream<CommittableMessage<Row>> committables) {
        System.out.println("Scott > DeltaSink > addPreCommitTopology");
        // Sets the partitioning of the DataStream so that the output values all go to the first
        // instance of the next processing operator
        return committables.global();
    }

    @Override
    public SimpleVersionedSerializer<Row> getWriteResultSerializer() {
        return new SimpleVersionedSerializer<Row>() {
            @Override
            public int getVersion() {
                return 1;
            }

            @Override
            public byte[] serialize(Row obj) throws IOException {
                return JsonUtils.rowToJson(obj).getBytes(java.nio.charset.StandardCharsets.UTF_8);
            }

            @Override
            public Row deserialize(int version, byte[] serialized) throws IOException {
                final String json = new String(serialized, java.nio.charset.StandardCharsets.UTF_8);
                return JsonUtils.rowFromJson(json, SingleAction.FULL_SCHEMA);
            }
        };
    }

    @Override
    public SimpleVersionedSerializer<Row> getCommittableSerializer() {
        return getWriteResultSerializer();
    }
}
