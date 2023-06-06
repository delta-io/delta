package io.delta.flink.table.it.suite;

import java.io.File;
import java.io.IOException;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;

import io.delta.flink.internal.table.TestTableData;
import io.delta.flink.utils.DeltaTestUtils;
import org.apache.commons.io.FileUtils;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.StringUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.rules.TemporaryFolder;
import static io.delta.flink.utils.DeltaTestUtils.buildClusterResourceConfig;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.delta.standalone.DeltaLog;
import io.delta.standalone.actions.Metadata;
import io.delta.standalone.types.BinaryType;
import io.delta.standalone.types.BooleanType;
import io.delta.standalone.types.DecimalType;
import io.delta.standalone.types.DoubleType;
import io.delta.standalone.types.FloatType;
import io.delta.standalone.types.IntegerType;
import io.delta.standalone.types.LongType;
import io.delta.standalone.types.ShortType;
import io.delta.standalone.types.StringType;
import io.delta.standalone.types.StructField;
import io.delta.standalone.types.StructType;
import io.delta.standalone.types.TimestampType;

/**
 * Test suite class for Delta Catalog integration tests. Tests from this class will be executed for
 * various implementations of Metastore such as 'In Memory' or Hive.
 * <p>
 * Implementations of this class must implement {@code DeltaCatalogTestSuite#setupDeltaCatalog}
 * method.
 */
public abstract class DeltaCatalogTestSuite {

    private static final int PARALLELISM = 2;

    private static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

    @RegisterExtension
    private static final MiniClusterExtension miniClusterResource =  new MiniClusterExtension(
        buildClusterResourceConfig(PARALLELISM)
    );

    private TableEnvironment tableEnv;

    private String tablePath;

    @BeforeAll
    public static void beforeAll() throws IOException {
        TEMPORARY_FOLDER.create();
    }

    @AfterAll
    public static void afterAll() {
        TEMPORARY_FOLDER.delete();
    }

    @BeforeEach
    public void setUp() throws IOException {
        tablePath = TEMPORARY_FOLDER.newFolder().getAbsolutePath();
        tableEnv = TableEnvironment.create(EnvironmentSettings.newInstance().build());
        setupDeltaCatalog(tableEnv);
    }

    @Test
    public void shouldCreateTable_deltaLogDoesNotExists() throws Exception {

        // GIVEN
        DeltaLog deltaLog =
            DeltaLog.forTable(DeltaTestUtils.getHadoopConf(), tablePath);

        assertThat(deltaLog.tableExists())
            .withFailMessage("There should be no Delta table files in test folder before test.")
            .isFalse();

        String deltaTable =
            String.format("CREATE TABLE sourceTable ("
                    + "col1 BYTES,"
                    + "col2 SMALLINT,"
                    + "col3 INT,"
                    + "col4 DOUBLE,"
                    + "col5 FLOAT,"
                    + "col6 BIGINT,"
                    + "col7 DECIMAL,"
                    + "col8 TIMESTAMP,"
                    + "col9 VARCHAR,"
                    + "col10 BOOLEAN"
                    + ") "
                    + "PARTITIONED BY (col1)"
                    + "WITH ("
                    + " 'connector' = 'delta',"
                    + " 'table-path' = '%s',"
                    + " 'delta.appendOnly' = 'false',"
                    + " 'userCustomProp' = 'myVal'"
                    + ")",
                tablePath);

        // WHEN
        tableEnv.executeSql(deltaTable).await();

        // THEN
        Metadata metadata = deltaLog.update().getMetadata();
        StructType actualSchema = metadata.getSchema();

        assertThat(actualSchema).isNotNull();
        assertThat(actualSchema.getFields())
            .withFailMessage(() -> schemaDoesNotMatchMessage(actualSchema))
            .containsExactly(
                new StructField("col1", new BinaryType()),
                new StructField("col2", new ShortType()),
                new StructField("col3", new IntegerType()),
                new StructField("col4", new DoubleType()),
                new StructField("col5", new FloatType()),
                new StructField("col6", new LongType()),
                new StructField("col7", DecimalType.USER_DEFAULT),
                new StructField("col8", new TimestampType()),
                new StructField("col9", new StringType()),
                new StructField("col10", new BooleanType())
            );

        assertThat(metadata.getPartitionColumns()).containsExactly("col1");
        assertThat(metadata.getName()).isEqualTo("sourceTable");
        assertThat(metadata.getConfiguration())
            .containsExactly(
                new SimpleEntry<>("delta.appendOnly", "false"),
                new SimpleEntry<>("userCustomProp", "myVal")
            );
    }

    @ParameterizedTest(name = "table property = {0}, partition column = {1}")
    @CsvSource( value = {
        ",", // no extra table properties and no partition columns.
        "delta.appendOnly, ", // table property but no partition columns.
        ", col1", // no extra table properties and one partition column.
        "delta.appendOnly, col1", // one extra table property and one partition column.
        "user.option,", // user defined table property but no partition columns.
        "user.option, col1", // one extra user defined table property and one partition column.
    })
    public void shouldCreateTable_deltaLogExists(
        String tableProperty,
        String partitionColumn) throws Exception {

        Map<String, String> tableProperties = (StringUtils.isNullOrWhitespaceOnly(tableProperty))
            ? Collections.emptyMap() : Collections.singletonMap(tableProperty, "true");

        List<String> partitionColumns = (StringUtils.isNullOrWhitespaceOnly(partitionColumn))
            ? Collections.emptyList() : Collections.singletonList(partitionColumn);

        DeltaLog deltaLog = DeltaTestUtils.setupDeltaTable(
            tablePath,
            tableProperties,
            Metadata.builder()
                .schema(new StructType(TestTableData.DELTA_FIELDS))
                .partitionColumns(partitionColumns)
                .build()
        );

        assertThat(deltaLog.tableExists())
            .withFailMessage(
                "There should be Delta table files in test folder before calling DeltaCatalog.")
            .isTrue();

        String deltaTable =
            String.format("CREATE TABLE sourceTable ("
                    + "col1 BOOLEAN,"
                    + "col2 INT,"
                    + "col3 VARCHAR"
                    + ") "
                    + ((partitionColumns.isEmpty())
                    ? ""
                    : String.format("PARTITIONED BY (%s)", String.join(", ", partitionColumns)))
                    + "WITH ("
                    + " 'connector' = 'delta',"
                    + " 'table-path' = '%s'"
                    + ")",
                tablePath);

        // WHEN
        tableEnv.executeSql(deltaTable).await();

        // THEN
        Metadata metadata = deltaLog.update().getMetadata();
        StructType schema = metadata.getSchema();

        assertThat(schema).isNotNull();
        assertThat(schema.getFields())
            .withFailMessage(() -> schemaDoesNotMatchMessage(schema))
            .containsExactly(TestTableData.DELTA_FIELDS);
        assertThat(metadata.getConfiguration()).containsExactlyEntriesOf(tableProperties);
        assertThat(metadata.getPartitionColumns())
            .containsExactlyInAnyOrderElementsOf(partitionColumns);
        assertThat(metadata.getName()).isNull();
    }

    @Test
    public void shouldThrow_createTable_computedColumns() {

        // GIVEN
        DeltaLog deltaLog =
            DeltaLog.forTable(DeltaTestUtils.getHadoopConf(), tablePath);

        assertThat(deltaLog.tableExists())
            .withFailMessage("There should be no Delta table files in test folder before test.")
            .isFalse();

        String deltaTable =
            String.format("CREATE TABLE sourceTable ("
                    + "col1 BIGINT,"
                    + "col2 BIGINT,"
                    + "col3 VARCHAR,"
                    + "col4 AS col1 * col2," // computed column, should not be added to _delta_log
                    + "col5 AS CONCAT(col3, '_hello')," // computed column
                    + "col6 AS CAST(col1 AS VARCHAR)" // computed column
                    + ") "
                    + "PARTITIONED BY (col1)"
                    + "WITH ("
                    + " 'connector' = 'delta',"
                    + " 'table-path' = '%s'"
                    + ")",
                tablePath);

        // WHEN
        RuntimeException exception =
            assertThrows(RuntimeException.class, () -> tableEnv.executeSql(deltaTable).await());

        assertThat(exception.getCause().getMessage())
            .isEqualTo(""
                + "Table definition contains unsupported column types. Currently, only physical "
                + "columns are supported by Delta Flink connector.\n"
                + "Invalid columns and types:\n"
                + "col4 -> ComputedColumn\n"
                + "col5 -> ComputedColumn\n"
                + "col6 -> ComputedColumn"
            );
    }

    @Test
    public void shouldThrow_createTable_metadataColumns() {

        // GIVEN
        DeltaLog deltaLog =
            DeltaLog.forTable(DeltaTestUtils.getHadoopConf(), tablePath);

        assertThat(deltaLog.tableExists())
            .withFailMessage("There should be no Delta table files in test folder before test.")
            .isFalse();

        String deltaTable =
            String.format("CREATE TABLE sourceTable ("
                    + "col1 BIGINT,"
                    + "col2 BIGINT,"
                    + "col3 VARCHAR,"
                    + "record_time TIMESTAMP_LTZ(3) METADATA FROM 'timestamp' " // metadata column
                    + ") "
                    + "PARTITIONED BY (col1)"
                    + "WITH ("
                    + " 'connector' = 'delta',"
                    + " 'table-path' = '%s'"
                    + ")",
                tablePath);

        // WHEN
        RuntimeException exception =
            assertThrows(RuntimeException.class, () -> tableEnv.executeSql(deltaTable).await());

        assertThat(exception.getCause().getMessage())
            .isEqualTo(""
                + "Table definition contains unsupported column types. Currently, only physical "
                + "columns are supported by Delta Flink connector.\n"
                + "Invalid columns and types:\n"
                + "record_time -> MetadataColumn"
            );
    }

    /**
     * Verifies that CREATE TABLE will throw exception when _delta_log exists under table-path but
     * has different schema that specified in DDL.
     */
    @ParameterizedTest(name = "DDL schema = {0}")
    @ValueSource(strings = {
        "name VARCHAR, surname VARCHAR", // missing column
        "name VARCHAR, surname VARCHAR, age INT, extraCol INT", // extra column
        "name VARCHAR, surname VARCHAR, differentName INT", // different name for third column
        "name INT, surname VARCHAR, age INT", // different type for first column
        "name VARCHAR NOT NULL, surname VARCHAR, age INT" // all columns should be nullable
    })
    public void shouldThrowIfSchemaDoesNotMatch(String ddlSchema) throws Exception {

        // GIVEN
        DeltaTestUtils.initTestForNonPartitionedTable(tablePath);

        DeltaLog deltaLog =
            DeltaLog.forTable(DeltaTestUtils.getHadoopConf(), tablePath);

        assertThat(deltaLog.tableExists())
            .withFailMessage(
                "There should be Delta table files in test folder before calling DeltaCatalog.")
            .isTrue();

        String deltaTable =
            String.format("CREATE TABLE sourceTable ("
                    + "%s"
                    + ") "
                    + "WITH ("
                    + " 'connector' = 'delta',"
                    + " 'table-path' = '%s'"
                    + ")",
                ddlSchema, tablePath);

        // WHEN
        RuntimeException exception =
            assertThrows(RuntimeException.class, () -> tableEnv.executeSql(deltaTable).await());

        // THEN
        assertThat(exception.getCause().getMessage()).contains(
            "has different schema or partition spec than one defined in CREATE TABLE DDL");

        // Check if there were no changes made to existing _delta_log
        Metadata metadata = deltaLog.update().getMetadata();
        verifyThatSchemaAndPartitionSpecNotChanged(metadata);
        assertThat(metadata.getConfiguration()).isEmpty();
    }

    /**
     * Verifies that CREATE TABLE will throw exception when DDL contains not allowed table
     * properties.
     */
    @Test
    public void shouldThrow_createTable_invalidTableProperties() throws Exception {

        String invalidOptions = ""
            + "'spark.some.option' = 'aValue',\n"
            + "'delta.logStore' = 'myLog',\n"
            + "'io.delta.storage.S3DynamoDBLogStore.ddb.region' = 'Poland',\n"
            + "'parquet.writer.max-padding' = '10'\n";

        String expectedValidationMessage = ""
            + "DDL contains invalid properties. DDL can have only delta table properties or "
            + "arbitrary user options only.\n"
            + "Invalid options used:\n"
            + " - 'spark.some.option'\n"
            + " - 'delta.logStore'\n"
            + " - 'io.delta.storage.S3DynamoDBLogStore.ddb.region'\n"
            + " - 'parquet.writer.max-padding'";

        ddlOptionValidation(invalidOptions, expectedValidationMessage);
    }

    /**
     * Verifies that CREATE TABLE will throw exception when DDL contains job-specific options.
     */
    @Test
    public void shouldThrow_createTable_jobSpecificOptions() throws Exception {

        // This test will not check if options are mutual excluded.
        // This is covered by table Factory and Source builder tests.
        String invalidOptions = ""
            + "'startingVersion' = '10',\n"
            + "'startingTimestamp' = '2022-02-24T04:55:00.001',\n"
            + "'updateCheckIntervalMillis' = '1000',\n"
            + "'updateCheckDelayMillis' = '1000',\n"
            + "'ignoreDeletes' = 'true',\n"
            + "'ignoreChanges' = 'true',\n"
            + "'versionAsOf' = '10',\n"
            + "'timestampAsOf' = '2022-02-24T04:55:00.001'";

        String expectedValidationMessage = ""
            + "DDL contains invalid properties. DDL can have only delta table properties or "
            + "arbitrary user options only.\n"
            + "DDL contains job-specific options. Job-specific options can be used only via Query"
            + " hints.\n"
            + "Used job-specific options:\n"
            + " - 'startingTimestamp'\n"
            + " - 'ignoreDeletes'\n"
            + " - 'updateCheckIntervalMillis'\n"
            + " - 'startingVersion'\n"
            + " - 'ignoreChanges'\n"
            + " - 'versionAsOf'\n"
            + " - 'updateCheckDelayMillis'\n"
            + " - 'timestampAsOf'";

        ddlOptionValidation(invalidOptions, expectedValidationMessage);
    }

    /**
     * Verifies that CREATE TABLE will throw exception when DDL contains job-specific options.
     */
    @Test
    public void shouldThrow_createTable_jobSpecificOptions_and_invalidTableProperties()
        throws Exception {

        // This test will not check if options are mutual excluded.
        // This is covered by table Factory and Source builder tests.
        String invalidOptions = ""
            + "'startingVersion' = '10',\n"
            + "'startingTimestamp' = '2022-02-24T04:55:00.001',\n"
            + "'updateCheckIntervalMillis' = '1000',\n"
            + "'updateCheckDelayMillis' = '1000',\n"
            + "'ignoreDeletes' = 'true',\n"
            + "'ignoreChanges' = 'true',\n"
            + "'versionAsOf' = '10',\n"
            + "'timestampAsOf' = '2022-02-24T04:55:00.001',\n"
            + "'spark.some.option' = 'aValue',\n"
            + "'delta.logStore' = 'myLog',\n"
            + "'io.delta.storage.S3DynamoDBLogStore.ddb.region' = 'Poland',\n"
            + "'parquet.writer.max-padding' = '10'\n";

        String expectedValidationMessage = ""
            + "DDL contains invalid properties. DDL can have only delta table properties or "
            + "arbitrary user options only.\n"
            + "Invalid options used:\n"
            + " - 'spark.some.option'\n"
            + " - 'delta.logStore'\n"
            + " - 'io.delta.storage.S3DynamoDBLogStore.ddb.region'\n"
            + " - 'parquet.writer.max-padding'\n"
            + "DDL contains job-specific options. Job-specific options can be used only via Query"
            + " hints.\n"
            + "Used job-specific options:\n"
            + " - 'startingTimestamp'\n"
            + " - 'ignoreDeletes'\n"
            + " - 'updateCheckIntervalMillis'\n"
            + " - 'startingVersion'\n"
            + " - 'ignoreChanges'\n"
            + " - 'versionAsOf'\n"
            + " - 'updateCheckDelayMillis'\n"
            + " - 'timestampAsOf'";

        ddlOptionValidation(invalidOptions, expectedValidationMessage);
    }

    private void ddlOptionValidation(String invalidOptions, String expectedValidationMessage)
        throws IOException {
        tablePath = TEMPORARY_FOLDER.newFolder().getAbsolutePath();
        String deltaTable =
            String.format("CREATE TABLE sourceTable ("
                    + "col1 INT,"
                    + "col2 INT,"
                    + "col3 INT"
                    + ") WITH ("
                    + " 'connector' = 'delta',"
                    + " 'table-path' = '%s',"
                    + "%s"
                    + ")",
                tablePath,
                invalidOptions
            );

        // WHEN
        RuntimeException exception =
            assertThrows(RuntimeException.class, () -> tableEnv.executeSql(deltaTable).await());

        // THEN
        assertThat(exception.getCause().getMessage()).isEqualTo(expectedValidationMessage);

        // Check if there were no changes made to existing _delta_log
        assertThat(
            DeltaLog.forTable(DeltaTestUtils.getHadoopConf(), tablePath).tableExists())
            .isFalse();
    }

    /**
     * Verifies that CREATE TABLE will throw exception when _delta_log exists under table-path but
     * has different partition spec that specified in DDL.
     */
    @Test
    public void shouldThrowIfPartitionSpecDoesNotMatch() throws Exception {

        // GIVEN
        DeltaTestUtils.initTestForNonPartitionedTable(tablePath);

        DeltaLog deltaLog =
            DeltaLog.forTable(DeltaTestUtils.getHadoopConf(), tablePath);

        assertThat(deltaLog.tableExists())
            .withFailMessage(
                "There should be Delta table files in test folder before calling DeltaCatalog.")
            .isTrue();

        String deltaTable =
            String.format("CREATE TABLE sourceTable ("
                    + "name VARCHAR,"
                    + "surname VARCHAR,"
                    + "age INT"
                    + ") "
                    + "PARTITIONED BY (name)"
                    + "WITH ("
                    + " 'connector' = 'delta',"
                    + " 'table-path' = '%s'"
                    + ")",
                tablePath);

        // WHEN
        RuntimeException exception =
            assertThrows(RuntimeException.class, () -> tableEnv.executeSql(deltaTable).await());

        // THEN
        assertThat(exception.getCause().getMessage()).contains(
            "has different schema or partition spec than one defined in CREATE TABLE DDL");

        // Check if there were no changes made to existing _delta_log
        Metadata metadata = deltaLog.update().getMetadata();
        verifyThatSchemaAndPartitionSpecNotChanged(metadata);
        assertThat(metadata.getConfiguration()).isEmpty();
    }

    @Test
    public void shouldThrowIfTableSchemaAndPartitionSpecDoNotMatch() throws IOException {
        // GIVEN
        DeltaTestUtils.initTestForNonPartitionedTable(tablePath);

        DeltaLog deltaLog =
            DeltaLog.forTable(DeltaTestUtils.getHadoopConf(), tablePath);

        assertThat(deltaLog.tableExists())
            .withFailMessage(
                "There should be Delta table files in test folder before calling DeltaCatalog.")
            .isTrue();

        String deltaTable =
            String.format("CREATE TABLE sourceTable ("
                    + "bogusColumn VARCHAR," // this column does not exist in _delta_log
                    + "surname VARCHAR,"
                    + "age INT"
                    + ") "
                    + "PARTITIONED BY (surname)"
                    + "WITH ("
                    + " 'connector' = 'delta',"
                    + " 'table-path' = '%s'"
                    + ")",
                tablePath);

        // WHEN
        RuntimeException exception =
            assertThrows(RuntimeException.class, () -> tableEnv.executeSql(deltaTable).await());

        // THEN
        assertThat(exception.getCause().getMessage()).contains(
            "has different schema or partition spec than one defined in CREATE TABLE DDL");

        // Check if there were no changes made to existing _delta_log
        Metadata metadata = deltaLog.update().getMetadata();
        verifyThatSchemaAndPartitionSpecNotChanged(metadata);
        assertThat(metadata.getConfiguration()).isEmpty();
    }

    /**
     * Verifies that CREATE TABLE will throw exception when _delta_log exists under table-path but
     * has different delta table properties that specified in DDL.
     */
    @Test
    public void shouldThrowIfDeltaTablePropertiesDoNotMatch() throws Exception {

        // GIVEN
        DeltaTestUtils.initTestForNonPartitionedTable(tablePath);

        Map<String, String> configuration = new HashMap<>();
        configuration.put("delta.appendOnly", "false");
        configuration.put("user.property", "false");

        // Set delta table property. DDL will try to override it with different value
        DeltaLog deltaLog = DeltaTestUtils.setupDeltaTableWithProperties(tablePath, configuration);

        assertThat(deltaLog.tableExists())
            .withFailMessage(
                "There should be Delta table files in test folder before calling DeltaCatalog.")
            .isTrue();

        String deltaTable =
            String.format("CREATE TABLE sourceTable ("
                    + "name VARCHAR,"
                    + "surname VARCHAR,"
                    + "age INT"
                    + ") "
                    + "WITH ("
                    + " 'connector' = 'delta',"
                    + " 'table-path' = '%s',"
                    + " 'delta.appendOnly' = 'true',"
                    + " 'user.property' = 'true'"
                    + ")",
                tablePath);

        // WHEN
        RuntimeException exception =
            assertThrows(RuntimeException.class, () -> tableEnv.executeSql(deltaTable).await());

        // THEN
        assertThat(exception.getCause().getMessage())
            .isEqualTo(""
                + "Invalid DDL options for table [default.sourceTable]. DDL options for Delta "
                + "table connector cannot override table properties already defined in _delta_log"
                + ".\n"
                + "DDL option name | DDL option value | Delta option value \n"
                + "delta.appendOnly | true | false\n"
                + "user.property | true | false");

        // Check if there were no changes made to existing _delta_log
        Metadata metadata = deltaLog.update().getMetadata();
        verifyThatSchemaAndPartitionSpecNotChanged(metadata);
        assertThat(metadata.getConfiguration()).containsExactlyEntriesOf(configuration);
    }

    @Test
    public void shouldDescribeTable() throws Exception {

        // GIVEN
        DeltaTestUtils.initTestForPartitionedTable(tablePath);

        DeltaLog deltaLog =
            DeltaLog.forTable(DeltaTestUtils.getHadoopConf(), tablePath);

        assertThat(deltaLog.tableExists())
            .withFailMessage("There should be Delta table files in test folder before test.")
            .isTrue();

        String deltaTable =
            String.format("CREATE TABLE sourceTable ("
                    + "name VARCHAR,"
                    + "surname VARCHAR,"
                    + "age INT,"
                    + "col1 VARCHAR," // partition column
                    + "col2 VARCHAR" // partition column
                    + ") "
                    + "PARTITIONED BY (col1, col2)"
                    + "WITH ("
                    + " 'connector' = 'delta',"
                    + " 'table-path' = '%s'"
                    + ")",
                tablePath);

        // WHEN
        tableEnv.executeSql(deltaTable).await();
        TableResult describeResult = tableEnv.executeSql("DESCRIBE sourceTable");

        List<String> describeRows = new ArrayList<>();
        try (CloseableIterator<Row> collect = describeResult.collect()) {
            while (collect.hasNext()) {
                Row row = collect.next();
                StringJoiner sj = new StringJoiner(";");
                for (int i = 0; i < row.getArity(); i++) {
                    sj.add(String.valueOf(row.getField(i)));
                }
                describeRows.add(sj.toString());
            }
        }

        // column name; column type; is nullable; primary key; comments; watermark
        assertThat(describeRows).containsExactly(
            "name;VARCHAR(1);true;null;null;null",
            "surname;VARCHAR(1);true;null;null;null",
            "age;INT;true;null;null;null",
            "col1;VARCHAR(1);true;null;null;null",
            "col2;VARCHAR(1);true;null;null;null"
        );
    }

    @Test
    public void shouldAlterTableName() throws Exception {

        // GIVEN
        DeltaTestUtils.initTestForPartitionedTable(tablePath);

        DeltaLog deltaLog =
            DeltaLog.forTable(DeltaTestUtils.getHadoopConf(), tablePath);

        assertThat(deltaLog.tableExists())
            .withFailMessage("There should be Delta table files in test folder before test.")
            .isTrue();

        String deltaTable =
            String.format("CREATE TABLE sourceTable ("
                    + "name VARCHAR,"
                    + "surname VARCHAR,"
                    + "age INT,"
                    + "col1 VARCHAR," // partition column
                    + "col2 VARCHAR" // partition column
                    + ") "
                    + "PARTITIONED BY (col1, col2)"
                    + "WITH ("
                    + " 'connector' = 'delta',"
                    + " 'table-path' = '%s'"
                    + ")",
                tablePath);

        // WHEN
        tableEnv.executeSql(deltaTable).await();
        tableEnv.executeSql("ALTER TABLE sourceTable RENAME TO newSourceTable");

        TableResult tableResult = tableEnv.executeSql("SHOW TABLES;");
        List<String> catalogTables = new ArrayList<>();
        try (CloseableIterator<Row> collect = tableResult.collect()) {
            while (collect.hasNext()) {
                catalogTables.add(((String) collect.next().getField(0)).toLowerCase());
            }
        }

        assertThat(catalogTables).containsExactly("newsourcetable");
    }

    @Test
    public void shouldAlterTableProperties() throws Exception {

        DeltaLog deltaLog = DeltaTestUtils.setupDeltaTable(
            tablePath,
            Collections.singletonMap("delta.appendOnly", "false"),
            Metadata.builder()
                .schema(new StructType(TestTableData.DELTA_FIELDS))
                .partitionColumns(Collections.emptyList())
                .build()
        );

        assertThat(deltaLog.tableExists())
            .withFailMessage("There should be Delta table files in test folder before test.")
            .isTrue();
        assertThat(deltaLog.update().getMetadata().getConfiguration())
            .containsEntry("delta.appendOnly", "false");

        String deltaTable =
            String.format("CREATE TABLE sourceTable ("
                    + "col1 BOOLEAN,"
                    + "col2 INT,"
                    + "col3 VARCHAR"
                    + ") "
                    + "WITH ("
                    + " 'connector' = 'delta',"
                    + " 'table-path' = '%s'"
                    + ")",
                tablePath);

        // WHEN
        tableEnv.executeSql(deltaTable).await();

        // Add new property.
        tableEnv.executeSql("ALTER TABLE sourceTable SET ('userCustomProp'='myVal1')").await();
        assertThat(deltaLog.update().getMetadata().getConfiguration())
            .containsEntry("userCustomProp", "myVal1")
            .containsEntry("delta.appendOnly", "false");

        // Change existing property.
        tableEnv.executeSql("ALTER TABLE sourceTable SET ('userCustomProp'='myVal2')").await();
        assertThat(deltaLog.update().getMetadata().getConfiguration())
            .containsEntry("userCustomProp", "myVal2")
            .containsEntry("delta.appendOnly", "false");

        // Change existing Delta property.
        tableEnv.executeSql("ALTER TABLE sourceTable SET ('delta.appendOnly'='true')").await();
        assertThat(deltaLog.update().getMetadata().getConfiguration())
            .containsEntry("userCustomProp", "myVal2")
            .containsEntry("delta.appendOnly", "true");
    }

    @Test
    public void shouldThrow_whenDeltaLogWasDeleted() throws Exception {
        DeltaTestUtils.setupDeltaTable(
            tablePath,
            Collections.emptyMap(),
            Metadata.builder()
                .schema(new StructType(TestTableData.DELTA_FIELDS))
                .build()
        );

        String deltaTable =
            String.format("CREATE TABLE sourceTable ("
                    + "col1 BOOLEAN,"
                    + "col2 INT,"
                    + "col3 VARCHAR"
                    + ") WITH ("
                    + " 'connector' = 'delta',"
                    + " 'table-path' = '%s'"
                    + ")",
                tablePath);

        // WHEN
        tableEnv.executeSql(deltaTable).await();

        // We want to execute any query, so DeltaLog instance for tablePath will be initialized and
        // added to DeltaCatalog's cache.
        String queryStatement = "SELECT * FROM sourceTable";
        TableResult tableResult = tableEnv.executeSql(queryStatement);
        tableResult.collect().close();

        // delete _delta_log from tablePath.
        FileUtils.cleanDirectory(new File(tablePath));

        // Now queryStatement should throw an exception. If not, then it could mean that
        // DeltaCatalog cache entry was not refreshed.
        assertThrows(ValidationException.class, () -> tableEnv.executeSql(queryStatement));
    }

    private void verifyThatSchemaAndPartitionSpecNotChanged(Metadata metadata) {
        StructType schema = metadata.getSchema();
        assertThat(schema).isNotNull();
        assertThat(schema.getFields())
            .withFailMessage(() -> schemaDoesNotMatchMessage(schema))
            .containsExactly(
                new StructField("name", new StringType()),
                new StructField("surname", new StringType()),
                new StructField("age", new IntegerType())
            );

        // we assume that there were no partition columns. In the future we might
        // have change this for different test setups.
        assertThat(metadata.getPartitionColumns()).isEmpty();
    }

    private String schemaDoesNotMatchMessage(StructType schema) {
        return String.format(
            "Schema from _delta_log does not match schema from DDL.\n"
                + "The actual schema was:\n [%s]", schema.getTreeString()
        );
    }

    public abstract void setupDeltaCatalog(TableEnvironment tableEnv);
}
