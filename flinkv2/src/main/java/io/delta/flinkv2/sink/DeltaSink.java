package io.delta.flinkv2.sink;

import io.delta.flinkv2.utils.DataUtils;
import io.delta.flinkv2.utils.SchemaUtils;
import io.delta.kernel.*;
import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.defaults.internal.json.JsonUtils;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.exceptions.TableNotFoundException;
import io.delta.kernel.internal.SnapshotImpl;
import io.delta.kernel.internal.actions.SingleAction;
import io.delta.kernel.types.StructType;
import org.apache.flink.api.connector.sink2.*;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.connector.sink2.SupportsPreCommitTopology;
import org.apache.flink.streaming.api.connector.sink2.SupportsPreWriteTopology;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class DeltaSink implements Sink<RowData>,
    SupportsCommitter<Row>,
    SupportsPreCommitTopology<Row, Row>,
    SupportsPreWriteTopology<RowData>,
    Serializable {

    ///////////////////////
    // Public static API //
    ///////////////////////

    public static DeltaSink forRowData(final String tablePath, final RowType rowType, final List<String> userProvidedPartitionColumns) {
        return new DeltaSink(tablePath, rowType, userProvidedPartitionColumns);
    }

    /////////////////////////////////////////////
    // Member fields that MUST be Serializable //
    /////////////////////////////////////////////

    private final String tablePath;
    private final RowType writeOperatorFlinkSchema;
    private final List<String> userProvidedPartitionColumns;
    private Set<String> tablePartitionColumns; // non-final since we set it in try-catch block

    private DeltaSink(final String tablePath, final RowType rowType, final List<String> userProvidedPartitionColumns) {
        System.out.println(
            String.format(
                "Scott > DeltaSink > constructor :: tablePath=%s, rowType=%s, userProvidedPartitionColumns=%s",
                tablePath,
                rowType,
                userProvidedPartitionColumns)
        );

        this.tablePath = tablePath;
        this.writeOperatorFlinkSchema = rowType;
        this.userProvidedPartitionColumns = userProvidedPartitionColumns;

        System.out.println(
            String.format("Equivalent Delta schema is: %s", SchemaUtils.toDeltaDataType(rowType))
        );

        try {
            final Engine engine = DefaultEngine.create(new Configuration());
            final Table table = Table.forPath(engine, tablePath);
            final Snapshot latestSnapshot = table.getLatestSnapshot(engine);
            final StructType tableSchema = latestSnapshot.getSchema(engine);
            final StructType writeOperatorDeltaSchema = SchemaUtils.toDeltaDataType(rowType);
            if (!tableSchema.equivalent(writeOperatorDeltaSchema)) {
                throw new RuntimeException(
                    String.format(
                        "Table Schema does not match Write Operator Delta Schema.\nTable schema: %s\nWrite Operator Delta schema: %s",
                        tableSchema,
                        writeOperatorDeltaSchema
                    )
                );
            }
            this.tablePartitionColumns = ((SnapshotImpl) latestSnapshot).getMetadata().getPartitionColNames();
        } catch (TableNotFoundException ex) {
            // table doesn't exist
            this.tablePartitionColumns = new HashSet<>(userProvidedPartitionColumns);
        }
    }

    /////////////////
    // Public APIs //
    /////////////////

    @Override
    public SinkWriter<RowData> createWriter(InitContext context) throws IOException {
        System.out.println("Scott > DeltaSink > createWriter");
        return new DeltaSinkWriter(tablePath, writeOperatorFlinkSchema, userProvidedPartitionColumns);
    }

    @Override
    public Committer<Row> createCommitter(CommitterInitContext context) throws IOException {
        System.out.println("Scott > DeltaSink > createCommitter");
        return new DeltaCommitter(tablePath, writeOperatorFlinkSchema, userProvidedPartitionColumns);
    }

    /**
     * This method ensures that all rows with the same partitionHash will be sent to the same
     * {@link DeltaSinkWriter}. It makes no promises about how many unique partitionHash's that
     * a {@link DeltaSinkWriter} will handle (it may even be 0).
     */
    @Override
    public DataStream<RowData> addPreWriteTopology(DataStream<RowData> inputDataStream) {
        System.out.println("Scott > DeltaSink > addPreWriteTopology");

        return inputDataStream.keyBy(new KeySelector<RowData, Integer>() {
            @Override
            public Integer getKey(RowData value) throws Exception {
                return DataUtils.flinkRowToPartitionValues(writeOperatorFlinkSchema, value, tablePartitionColumns).hashCode();
            }
        });
    }

    @Override
    public DataStream<CommittableMessage<Row>> addPreCommitTopology(
            DataStream<CommittableMessage<Row>> committables) {
        System.out.println("Scott > DeltaSink > addPreCommitTopology");
        // Sets the partitioning of the DataStream so that the output values all go to the first
        // instance of the next processing operator
        //
        // This essentially is what gives us a global committer.
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
