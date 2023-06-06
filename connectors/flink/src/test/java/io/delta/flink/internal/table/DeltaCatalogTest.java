package io.delta.flink.internal.table;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.delta.flink.internal.table.DeltaCatalog.DeltaLogCacheKey;
import io.delta.flink.utils.DeltaTestUtils;
import org.apache.commons.io.FileUtils;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.rules.TemporaryFolder;
import org.mockito.junit.jupiter.MockitoExtension;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.delta.standalone.DeltaLog;
import io.delta.standalone.actions.Metadata;
import io.delta.standalone.types.StructType;

@ExtendWith(MockitoExtension.class)
class DeltaCatalogTest {

    private static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

    private static final boolean ignoreIfExists = false;

    private static final String DATABASE = "default";

    public static final String CATALOG_NAME = "testCatalog";

    private DeltaCatalog deltaCatalog;

    private GenericInMemoryCatalog decoratedCatalog;

    // Resets every test.
    private Map<String, String> ddlOptions;

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
        this.decoratedCatalog = new GenericInMemoryCatalog(CATALOG_NAME, DATABASE);
        this.decoratedCatalog.open();
        this.deltaCatalog = new DeltaCatalog(
            CATALOG_NAME,
            this.decoratedCatalog,
            new Configuration()
        );
        this.ddlOptions = new HashMap<>();
        this.ddlOptions.put(
            DeltaTableConnectorOptions.TABLE_PATH.key(),
            TEMPORARY_FOLDER.newFolder().getAbsolutePath()
        );
    }

    @ParameterizedTest
    @NullSource  // pass a null value
    @ValueSource(strings = {"", " "})
    public void shouldThrow_createTable_invalidTablePath(String deltaTablePath) {

        DeltaCatalogBaseTable deltaCatalogTable = setUpCatalogTable(
            (deltaTablePath == null) ? Collections.emptyMap() : Collections.singletonMap(
                DeltaTableConnectorOptions.TABLE_PATH.key(),
                deltaTablePath
            )
        );

        CatalogException exception = assertThrows(CatalogException.class, () ->
            deltaCatalog.createTable(deltaCatalogTable, ignoreIfExists)
        );

        assertThat(exception.getMessage())
            .isEqualTo("Path to Delta table cannot be null or empty.");
    }

    @Test
    public void shouldThrow_createTable_invalidTableOption() {

        Map<String, String> invalidOptions = Stream.of(
                "spark.some.option",
                "delta.logStore",
                "io.delta.storage.S3DynamoDBLogStore.ddb.region",
                "parquet.writer.max-padding"
            )
            .collect(Collectors.toMap(optionName -> optionName, s -> "aValue"));

        String expectedValidationMessage = ""
            + "DDL contains invalid properties. DDL can have only delta table properties or "
            + "arbitrary user options only.\n"
            + "Invalid options used:\n"
            + " - 'spark.some.option'\n"
            + " - 'delta.logStore'\n"
            + " - 'io.delta.storage.S3DynamoDBLogStore.ddb.region'\n"
            + " - 'parquet.writer.max-padding'";

        validateCreateTableOptions(invalidOptions, expectedValidationMessage);
    }

    @Test
    public void shouldThrow_createTable_jobSpecificOption() {

        // This test will not check if options are mutual excluded.
        // This is covered by table Factory and Source builder tests.
        Map<String, String> invalidOptions = Stream.of(
                "startingVersion",
                "startingTimestamp",
                "updateCheckIntervalMillis",
                "updateCheckDelayMillis",
                "ignoreDeletes",
                "ignoreChanges",
                "versionAsOf",
                "timestampAsOf",
                // This will be treated as arbitrary user-defined table property and will not be
                // part of the exception message since we don't
                // do case-sensitive checks.
                "TIMESTAMPASOF"
            )
            .collect(Collectors.toMap(optionName -> optionName, s -> "aValue"));

        String expectedValidationMessage = ""
            + "DDL contains invalid properties. DDL can have only delta table properties or "
            + "arbitrary user options only.\n"
            + "DDL contains job-specific options. Job-specific options can be used only via Query"
            + " hints.\n"
            + "Used job-specific options:\n"
            + " - 'ignoreDeletes'\n"
            + " - 'startingTimestamp'\n"
            + " - 'updateCheckIntervalMillis'\n"
            + " - 'startingVersion'\n"
            + " - 'ignoreChanges'\n"
            + " - 'versionAsOf'\n"
            + " - 'updateCheckDelayMillis'\n"
            + " - 'timestampAsOf'";

        validateCreateTableOptions(invalidOptions, expectedValidationMessage);
    }

    @Test
    public void shouldThrow_createTable_jobSpecificOption_and_invalidTableOptions() {

        // This test will not check if options are mutual excluded.
        // This is covered by table Factory and Source builder tests.
        Map<String, String> invalidOptions = Stream.of(
                "spark.some.option",
                "delta.logStore",
                "io.delta.storage.S3DynamoDBLogStore.ddb.region",
                "parquet.writer.max-padding",
                "startingVersion",
                "startingTimestamp",
                "updateCheckIntervalMillis",
                "updateCheckDelayMillis",
                "ignoreDeletes",
                "ignoreChanges",
                "versionAsOf",
                "timestampAsOf"
            )
            .collect(Collectors.toMap(optionName -> optionName, s -> "aValue"));

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

        validateCreateTableOptions(invalidOptions, expectedValidationMessage);
    }

    @Test
    public void shouldThrow_mismatchedDdlOption_and_deltaTableProperty() {

        String tablePath = this.ddlOptions.get(
            DeltaTableConnectorOptions.TABLE_PATH.key()
        );

        Map<String, String> configuration = Collections.singletonMap("delta.appendOnly", "false");

        DeltaLog deltaLog = DeltaTestUtils.setupDeltaTable(
            tablePath,
            configuration,
            Metadata.builder()
                .schema(new StructType(TestTableData.DELTA_FIELDS))
                .build()
        );

        assertThat(deltaLog.tableExists())
            .withFailMessage(
                "There should be Delta table files in test folder before calling DeltaCatalog.")
            .isTrue();

        Map<String, String> mismatchedOptions =
            Collections.singletonMap("delta.appendOnly", "true");


        String expectedValidationMessage = ""
            + "Invalid DDL options for table [default.testTable]. DDL options for Delta table"
            + " connector cannot override table properties already defined in _delta_log.\n"
            + "DDL option name | DDL option value | Delta option value \n"
            + "delta.appendOnly | true | false";

        validateCreateTableOptions(mismatchedOptions, expectedValidationMessage);
    }

    /**
     * This test verifies that cached DeltaLog instance will be refreshed making getTable() method
     * throws an exception in subsequent getTable() calls after deleting _delta_log folder.
     */
    @Test
    public void shouldRefreshDeltaCache_deltaLogDeleted() throws Exception {

        String tablePath = this.ddlOptions.get(
            DeltaTableConnectorOptions.TABLE_PATH.key()
        );

        // GIVEN setup _delta_log on disk.
        DeltaLog deltaLog = DeltaTestUtils.setupDeltaTable(
            tablePath,
            Collections.emptyMap(),
            Metadata.builder()
                .schema(new StructType(TestTableData.DELTA_FIELDS))
                .build()
        );

        assertThat(deltaLog.tableExists())
            .withFailMessage(
                "There should be Delta table files in test folder before calling DeltaCatalog.")
            .isTrue();

        // Get table from DeltaCatalog. This is the first call for this table, so it will initialize
        // the DeltaLog entry in catalog's cache.
        // As stated in DeltaCatalog::getTable Javadoc, this method assumes that table is already
        // present in metastore, hence no extra metastore checks are done and "createTable"
        // method doesn't have to be called prior to this method.
        DeltaCatalogBaseTable baseTable = setUpCatalogTable(ddlOptions);
        assertThat(deltaCatalog.getTable(baseTable)).isNotNull();

        // Remove _delta_log files
        FileUtils.cleanDirectory(new File(tablePath));

        // If DeltaCatalog DeltaLog instance would not be refreshed, this method would not throw
        // an exception since DeltaLog instance would not know that _delta_log was deleted.
        TableNotExistException exception =
            assertThrows(TableNotExistException.class, () -> deltaCatalog.getTable(baseTable));

        assertThat(exception.getCause().getMessage())
            .contains(
                "Table default.testTable exists in metastore but "
                    + "_delta_log was not found under path");

    }

    /**
     * This test verifies that cached DeltaLog instance will be refreshed making getTable() method
     * stop throwing an exception in subsequent getTable() calls after creating _delta_log folder.
     */
    @Test
    public void shouldRefreshDeltaCache_createDeltaLog() throws Exception {

        // GIVEN setup _delta_log on disk.
        String tablePath = this.ddlOptions.get(
            DeltaTableConnectorOptions.TABLE_PATH.key()
        );

        assertThat(DeltaLog.forTable(DeltaTestUtils.getHadoopConf(), tablePath).tableExists())
            .withFailMessage(
                "There should be no Delta table files in test folder before calling DeltaCatalog.")
            .isFalse();

        DeltaCatalogBaseTable baseTable = setUpCatalogTable(ddlOptions);
        // Get table from DeltaCatalog. This is the first call for this table, so it will initialize
        // the DeltaLog entry in catalog's cache.
        // As stated in DeltaCatalog::getTable Javadoc, this method assumes that table is already
        // present in metastore, hence no extra metastore checks are done and "createTable"
        // method doesn't have to be called prior to this method.
        TableNotExistException exception =
            assertThrows(TableNotExistException.class, () -> deltaCatalog.getTable(baseTable));

        // Since there was no _delta_log on the filesystem we except exception when calling
        // getTable()
        assertThat(exception.getCause().getMessage())
            .contains(
                "Table default.testTable exists in metastore but "
                    + "_delta_log was not found under path");

        // setup _delta_log on disk.
        DeltaTestUtils.setupDeltaTable(
            tablePath,
            Collections.emptyMap(),
            Metadata.builder()
                .schema(new StructType(TestTableData.DELTA_FIELDS))
                .build()
        );

        // If DeltaCatalog DeltaLog instance would not be refreshed, this method would throw
        // an exception since DeltaLog instance would not know that _delta_log was created.
        assertThat(deltaCatalog.getTable(baseTable)).isNotNull();
    }

    @Test
    public void shouldAddTableToMetastoreAndCache() throws Exception {
        String tablePath = this.ddlOptions.get(
            DeltaTableConnectorOptions.TABLE_PATH.key()
        );

        // GIVEN setup _delta_log on disk.
        DeltaTestUtils.setupDeltaTable(
            tablePath,
            Collections.emptyMap(),
            Metadata.builder()
                .schema(new StructType(TestTableData.DELTA_FIELDS))
                .build()
        );

        DeltaCatalogBaseTable baseTable = setUpCatalogTable(ddlOptions);
        ObjectPath tableCatalogPath = baseTable.getTableCatalogPath();
        deltaCatalog.createTable(baseTable, false); // ignoreIfExists = false

        // Validate entry in metastore
        CatalogBaseTable metastoreTable = decoratedCatalog.getTable(tableCatalogPath);
        assertThat(metastoreTable).withFailMessage("Missing table entry in metastore.").isNotNull();
        assertThat(metastoreTable.getOptions())
            .withFailMessage(
                "Metastore should contain only connector and table-path options for table.")
            .containsOnlyKeys(Arrays.asList("connector", "table-path"));
        assertThat(metastoreTable.getUnresolvedSchema())
            .withFailMessage("Metastore contains non-empty schema information.")
            .isEqualTo(Schema.newBuilder().build());

        // Validate that cache entry was created for Delta table.
        assertThat(deltaCatalog.getDeltaLogCache().getIfPresent(
            new DeltaLogCacheKey(tableCatalogPath, tablePath))
        ).isNotNull();
    }

    @Test
    public void shouldRemoveFromCacheAfterTableDrop() throws Exception {
        String tablePath = this.ddlOptions.get(
            DeltaTableConnectorOptions.TABLE_PATH.key()
        );

        // GIVEN setup _delta_log on disk.
        DeltaTestUtils.setupDeltaTable(
            tablePath,
            Collections.emptyMap(),
            Metadata.builder()
                .schema(new StructType(TestTableData.DELTA_FIELDS))
                .build()
        );

        DeltaCatalogBaseTable baseTable = setUpCatalogTable(ddlOptions);
        ObjectPath tableCatalogPath = baseTable.getTableCatalogPath();

        deltaCatalog.createTable(baseTable, false); // ignoreIfExists = false
        assertThat(decoratedCatalog.getTable(tableCatalogPath))
            .withFailMessage("Metastore is missing created table.")
            .isNotNull();
        assertThat(deltaCatalog.getDeltaLogCache().getIfPresent(
            new DeltaLogCacheKey(tableCatalogPath, tablePath)))
            .withFailMessage("DeltaCatalog Cache has no entry for created Table.")
            .isNotNull();

        // Drop table
        deltaCatalog.dropTable(baseTable, false); // ignoreIfExists = false
        assertThrows(
            TableNotExistException.class,
            () -> decoratedCatalog.getTable(tableCatalogPath)
        );
        assertThat(deltaCatalog.getDeltaLogCache().getIfPresent(
            new DeltaLogCacheKey(tableCatalogPath, tablePath)))
            .withFailMessage("DeltaCatalog cache contains entry for dropped table.")
            .isNull();
    }

    private void validateCreateTableOptions(
        Map<String, String> invalidOptions,
        String expectedValidationMessage) {
        ddlOptions.putAll(invalidOptions);
        DeltaCatalogBaseTable deltaCatalogTable = setUpCatalogTable(ddlOptions);

        CatalogException exception = assertThrows(CatalogException.class, () ->
            deltaCatalog.createTable(deltaCatalogTable, ignoreIfExists)
        );

        assertThat(exception.getMessage()).isEqualTo(expectedValidationMessage);
    }

    private DeltaCatalogBaseTable setUpCatalogTable(Map<String, String> options) {

        CatalogTable catalogTable = CatalogTable.of(
            Schema.newBuilder()
                .fromFields(TestTableData.COLUMN_NAMES, TestTableData.COLUMN_TYPES)
                .build(),
            "comment",
            Collections.emptyList(), // partitionKeys
            options // options
        );

        return new DeltaCatalogBaseTable(
            new ObjectPath(DATABASE, "testTable"),
            new ResolvedCatalogTable(
                catalogTable,
                ResolvedSchema.physical(TestTableData.COLUMN_NAMES, TestTableData.COLUMN_TYPES)
            )
        );
    }
}
