package io.delta.flink.table.it;

import java.io.IOException;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.rules.TemporaryFolder;
import static io.delta.flink.utils.DeltaTestUtils.buildCluster;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests that ensures that Flink Delta table SQL cannot be used without Delta Catalog.
 */
public class FlinkSqlTestITCase {

    private static final int PARALLELISM = 2;

    private static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

    private final MiniClusterWithClientResource miniClusterResource = buildCluster(PARALLELISM);

    public TableEnvironment tableEnv;

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
        try {
            miniClusterResource.before();
            tableEnv = StreamTableEnvironment.create(getTestStreamEnv());
        } catch (Exception e) {
            throw new RuntimeException("Weren't able to setup the test dependencies", e);
        }
    }

    @AfterEach
    public void afterEach() {
        miniClusterResource.after();
    }

    @Test
    public void shouldThrow_selectDeltaTable_noDeltaCatalog() throws Exception {

        // GIVEN
        String sourceTablePath = TEMPORARY_FOLDER.newFolder().getAbsolutePath();

        String sourceTableSql = String.format(
            "CREATE TABLE sourceTable ("
                + " col1 VARCHAR,"
                + " col2 VARCHAR,"
                + " col3 INT"
                + ") "
                + "WITH ("
                + " 'connector' = 'delta',"
                + " 'table-path' = '%s'"
                + ")",
            sourceTablePath);

        tableEnv.executeSql(sourceTableSql);

        String sinkTableSql = "CREATE TABLE sinkTable ("
            + " col1 VARCHAR,"
            + " col2 VARCHAR,"
            + " col3 INT"
            + ") WITH ("
            + "  'connector' = 'blackhole'"
            + ");";

        tableEnv.executeSql(sinkTableSql);

        // WHEN
        String selectSql = "SELECT * FROM sourceTable";

        // THEN
        ValidationException validationException =
            assertThrows(ValidationException.class, () -> tableEnv.executeSql(selectSql));

        assertThat(
            validationException.getCause().getMessage())
            .withFailMessage(
                "Query Delta table should not be possible without Delta catalog.")
            .contains("Delta Table SQL/Table API was used without Delta Catalog.");
    }

    @Test
    public void shouldThrow_insertToDeltaTable_noDeltaCatalog() throws Exception {

        // GIVEN
        String targetTablePath = TEMPORARY_FOLDER.newFolder().getAbsolutePath();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(getTestStreamEnv());

        String sourceTableSql = "CREATE TABLE sourceTable ("
            + " col1 VARCHAR,"
            + " col2 VARCHAR,"
            + " col3 INT"
            + ") WITH ("
            + "'connector' = 'datagen',"
            + "'rows-per-second' = '1',"
            + "'fields.col3.kind' = 'sequence',"
            + "'fields.col3.start' = '1',"
            + "'fields.col3.end' = '5'"
            + ")";

        tableEnv.executeSql(sourceTableSql);

        String sinkTableSql = String.format(
            "CREATE TABLE sinkTable ("
                + " col1 VARCHAR,"
                + " col2 VARCHAR,"
                + " col3 INT"
                + ") "
                + "WITH ("
                + " 'connector' = 'delta',"
                + " 'table-path' = '%s'"
                + ")",
            targetTablePath);

        tableEnv.executeSql(sinkTableSql);

        // WHEN
        String insertSql = "INSERT INTO sinkTable SELECT * FROM sourceTable";

        // THEN
        ValidationException validationException =
            assertThrows(ValidationException.class, () -> tableEnv.executeSql(insertSql));

        assertThat(
            validationException.getCause().getMessage())
            .withFailMessage(
                "Query Delta table should not be possible without Delta catalog.")
            .contains("Delta Table SQL/Table API was used without Delta Catalog.");
    }

    private StreamExecutionEnvironment getTestStreamEnv() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setRestartStrategy(RestartStrategies.noRestart());
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.enableCheckpointing(100, CheckpointingMode.EXACTLY_ONCE);
        return env;
    }
}
