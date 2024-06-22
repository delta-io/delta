package io.delta.flink.table.it.suite;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import io.delta.flink.utils.CheckpointCountingSource;
import io.delta.flink.utils.CheckpointCountingSource.RowProducer;
import io.delta.flink.utils.DeltaTestUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.util.DataFormatConverters;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.RowType.RowField;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.rules.TemporaryFolder;
import static io.delta.flink.utils.DeltaTestUtils.buildClusterResourceConfig;
import static io.delta.flink.utils.DeltaTestUtils.getTestStreamEnv;
import static io.delta.flink.utils.DeltaTestUtils.verifyDeltaTable;
import static io.delta.flink.utils.ExecutionITCaseTestConstants.LARGE_TABLE_ALL_COLUMN_NAMES;
import static io.delta.flink.utils.ExecutionITCaseTestConstants.LARGE_TABLE_ALL_COLUMN_TYPES;
import static org.assertj.core.api.Assertions.assertThat;

public abstract class DeltaEndToEndTableTestSuite {

    private static final int PARALLELISM = 2;

    protected static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

    private String sourceTableDdl;

    @RegisterExtension
    private static final MiniClusterExtension miniClusterResource =  new MiniClusterExtension(
        buildClusterResourceConfig(PARALLELISM)
    );

    private String sinkTablePath;

    @BeforeAll
    public static void beforeAll() throws IOException {
        TEMPORARY_FOLDER.create();
    }

    @AfterAll
    public static void afterAll() {
        TEMPORARY_FOLDER.delete();
    }

    @BeforeEach
    public void setUp() {

        // Schema for this table has only
        // {@link ExecutionITCaseTestConstants#LARGE_TABLE_ALL_COLUMN_NAMES} of type
        // {@link ExecutionITCaseTestConstants#LARGE_TABLE_ALL_COLUMN_TYPES} columns.
        // Column types are long, long, String
        String nonPartitionedLargeTablePath;
        try {
            nonPartitionedLargeTablePath = TEMPORARY_FOLDER.newFolder().getAbsolutePath();
            sinkTablePath = TEMPORARY_FOLDER.newFolder().getAbsolutePath();
            DeltaTestUtils.initTestForNonPartitionedLargeTable(nonPartitionedLargeTablePath);
            assertThat(sinkTablePath).isNotEqualToIgnoringCase(nonPartitionedLargeTablePath);
        } catch (Exception e) {
            throw new RuntimeException("Weren't able to setup the test dependencies", e);
        }

        sourceTableDdl = String.format("CREATE TABLE sourceTable ("
                + "col1 BIGINT,"
                + "col2 BIGINT,"
                + "col3 VARCHAR"
                + ") WITH ("
                + " 'connector' = 'delta',"
                + " 'table-path' = '%s'"
                + ")",
            nonPartitionedLargeTablePath);
    }

    @Test
    public void shouldReadAndWriteDeltaTable() throws Exception {

        // streamingMode = false
        StreamTableEnvironment tableEnv = setupTableEnvAndDeltaCatalog(false);

        String sinkTableDdl =
            String.format("CREATE TABLE sinkTable ("
                    + "col1 BIGINT,"
                    + "col2 BIGINT,"
                    + "col3 VARCHAR"
                    + ") WITH ("
                    + " 'connector' = 'delta',"
                    + " 'table-path' = '%s'"
                    + ")",
                sinkTablePath);

        readWriteTable(tableEnv, sinkTableDdl);

        // Execute SELECT on sink table and validate TableResult.
        TableResult tableResult = tableEnv.executeSql("SELECT * FROM sinkTable");
        List<Row> result = readRowsFromQuery(tableResult, 1100);
        for (Row row : result) {
            assertThat(row.getField("col1")).isInstanceOf(Long.class);
            assertThat(row.getField("col2")).isInstanceOf(Long.class);
            assertThat(row.getField("col3")).isInstanceOf(String.class);
        }
    }

    /**
     * End-to-End test where Delta sink table is created using Flink's LIKE statement.
     */
    @Test
    public void shouldReadAndWriteDeltaTable_LikeTable() throws Exception {

        // streamingMode = false
        StreamTableEnvironment tableEnv = setupTableEnvAndDeltaCatalog(false);

        String sinkTableDdl = String.format(""
                + "CREATE TABLE sinkTable "
                + "WITH ("
                + "'connector' = 'delta',"
                + "'table-path' = '%s'"
                + ") LIKE sourceTable",
            sinkTablePath);

        readWriteTable(tableEnv, sinkTableDdl);

        // Execute SELECT on sink table and validate TableResult.
        TableResult tableResult = tableEnv.executeSql("SELECT * FROM sinkTable");
        List<Row> result = readRowsFromQuery(tableResult, 1100);
        for (Row row : result) {
            assertThat(row.getField("col1")).isInstanceOf(Long.class);
            assertThat(row.getField("col2")).isInstanceOf(Long.class);
            assertThat(row.getField("col3")).isInstanceOf(String.class);
        }
    }

    /**
     * End-to-End test where Delta sin table is created using Flink's AS SELECT statement.
     */
    @Test
    public void shouldReadAndWriteDeltaTable_AsSelect() throws Exception {
        // streamingMode = false
        StreamTableEnvironment tableEnv = setupTableEnvAndDeltaCatalog(false);

        String sinkTableDdl = String.format(""
                + "CREATE TABLE sinkTable "
                + "WITH ("
                + "'connector' = 'delta',"
                + "'table-path' = '%s'"
                + ") AS SELECT * FROM sourceTable",
            sinkTablePath);

        tableEnv.executeSql(this.sourceTableDdl).await(10, TimeUnit.SECONDS);
        tableEnv.executeSql(sinkTableDdl).await(10, TimeUnit.SECONDS);

        RowType rowType = RowType.of(LARGE_TABLE_ALL_COLUMN_TYPES, LARGE_TABLE_ALL_COLUMN_NAMES);
        verifyDeltaTable(this.sinkTablePath, rowType, 1100);

        // Execute SELECT on sink table and validate TableResult.
        TableResult tableResult = tableEnv.executeSql("SELECT * FROM sinkTable");
        List<Row> result = readRowsFromQuery(tableResult, 1100);
        for (Row row : result) {
            assertThat(row.getField("col1")).isInstanceOf(Long.class);
            assertThat(row.getField("col2")).isInstanceOf(Long.class);
            assertThat(row.getField("col3")).isInstanceOf(String.class);
        }
    }

    @Test
    public void shouldWriteAndReadNestedStructures() throws Exception {
        String deltaSinkTableDdl =
            String.format("CREATE TABLE deltaSinkTable ("
                    + " col1 INT,"
                    + " col2 ROW <a INT, b INT>"
                    + ") WITH ("
                    + " 'connector' = 'delta',"
                    + " 'table-path' = '%s'"
                    + ")",
                sinkTablePath);

        StreamExecutionEnvironment streamEnv = getTestStreamEnv(true);// streamingMode = true
        StreamTableEnvironment tableEnv = setupTableEnvAndDeltaCatalog(streamEnv);

        // We are running this in a streaming mode Delta global committer will be lagging one
        // commit behind comparing to the rest of the pipeline. At the same time we want to
        // always commit to delta_log everything that test source produced. Because of this we
        // need to use a source that will wait one extra Flin checkpoint after sending all its data
        // before shutting off. This is why we are using our CheckpointCountingSource.
        // Please note that we don't have its Table API version hence we are using combination of
        // Streaming and Table API.
        DataStreamSource<RowData> streamSource = streamEnv.addSource(
            //recordsPerCheckpoint =1, numberOfCheckpoints = 5
            new CheckpointCountingSource(1, 5, new NestedRowColumnRowProducer())
        ).setParallelism(1);
        Table sourceTable = tableEnv.fromDataStream(streamSource);
        tableEnv.createTemporaryView("sourceTable", sourceTable);
        tableEnv.executeSql(deltaSinkTableDdl).await(10, TimeUnit.SECONDS);

        tableEnv.executeSql("INSERT INTO deltaSinkTable SELECT * FROM sourceTable")
            .await(10, TimeUnit.SECONDS);

        // Execute SELECT on sink table and validate TableResult.
        TableResult tableResult =
            tableEnv.executeSql("SELECT col2.a AS innerA, col2.b AS innerB FROM deltaSinkTable");
        tableResult.await();
        List<Row> result = readRowsFromQuery(tableResult, 5);
        for (Row row : result) {
            assertThat(row.getField("innerA")).isInstanceOf(Integer.class);
            assertThat(row.getField("innerB")).isInstanceOf(Integer.class);
        }
    }

    private void readWriteTable(StreamTableEnvironment tableEnv, String sinkTableDdl)
        throws Exception {

        String selectToInsertSql = "INSERT INTO sinkTable SELECT * FROM sourceTable";
        tableEnv.executeSql(this.sourceTableDdl);
        tableEnv.executeSql(sinkTableDdl);

        tableEnv.executeSql(selectToInsertSql).await(10, TimeUnit.SECONDS);

        RowType rowType = RowType.of(LARGE_TABLE_ALL_COLUMN_TYPES, LARGE_TABLE_ALL_COLUMN_NAMES);
        verifyDeltaTable(this.sinkTablePath, rowType, 1100);
    }

    private List<Row> readRowsFromQuery(TableResult tableResult, int expectedRowsCount)
        throws Exception {

        List<Row> result = new ArrayList<>();
        try (CloseableIterator<Row> collect = tableResult.collect()) {
            while (collect.hasNext()) {
                result.add(collect.next());
            }
        }

        assertThat(result).hasSize(expectedRowsCount);
        return result;
    }

    private StreamTableEnvironment setupTableEnvAndDeltaCatalog(boolean streamingMode) {
        return setupTableEnvAndDeltaCatalog(getTestStreamEnv(streamingMode));
    }

    private StreamTableEnvironment setupTableEnvAndDeltaCatalog(
            StreamExecutionEnvironment streamingExecutionEnv) {
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(
            streamingExecutionEnv
        );
        setupDeltaCatalog(tableEnv);
        return tableEnv;
    }

    /**
     * A {@link RowProducer} implementation used by {@link CheckpointCountingSource}.
     * This implementation will produce records with schema containing a nested row.
     * The produced schema:
     * <pre>
     *     Row&lt;"col1"[Int], "col2"[Row&lt;"a"[Int], "b[Int]&gt;]&gt;
     * </pre>
     *
     * Columns {@code col1} and {@code col2.a} will have a sequence value.
     * <p>
     * Column {@code col2.b} will have a value equal to {@code col2.a" * 2}.
     */
    private static class NestedRowColumnRowProducer implements RowProducer {

        private final RowType nestedRowType = new RowType(Arrays.asList(
            new RowType.RowField("col1", new IntType()),
            new RowType.RowField("col2", new RowType(
                Arrays.asList(
                    new RowType.RowField("a", new IntType()),
                    new RowType.RowField("b", new IntType())
                ))
            ))
        );

        @SuppressWarnings("unchecked")
        private final DataFormatConverters.DataFormatConverter<RowData, Row>
            rowTypeConverter = DataFormatConverters.getConverterForDataType(
            TypeConversions.fromLogicalToDataType(nestedRowType)
        );

        @Override
        public int emitRecordsBatch(int nextValue, SourceContext<RowData> ctx, int batchSize) {
            for (int i = 0; i < batchSize; ++i) {
                RowData row = rowTypeConverter.toInternal(
                    Row.of(
                        nextValue,
                        Row.of(nextValue, nextValue * 2)
                    )
                );
                ctx.collect(row);
                nextValue++;
            }

            return nextValue;
        }

        @Override
        public TypeInformation<RowData> getProducedType() {
            LogicalType[] fieldTypes = nestedRowType.getFields().stream()
                .map(RowField::getType).toArray(LogicalType[]::new);
            String[] fieldNames = nestedRowType.getFieldNames().toArray(new String[0]);
            return InternalTypeInfo.of(RowType.of(fieldTypes, fieldNames));
        }
    }

    public abstract void setupDeltaCatalog(TableEnvironment tableEnv);
}
