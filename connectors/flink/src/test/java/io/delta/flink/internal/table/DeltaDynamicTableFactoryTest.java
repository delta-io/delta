package io.delta.flink.internal.table;

import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.delta.flink.utils.DeltaTestUtils;
import org.apache.flink.core.testutils.CommonTestUtils;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableFactory.Context;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.hadoop.conf.Configuration;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

class DeltaDynamicTableFactoryTest {

    private static final Logger LOG = LoggerFactory.getLogger(DeltaDynamicTableFactoryTest.class);

    public static final ResolvedSchema SCHEMA =
        ResolvedSchema.of(
            Column.physical("a", DataTypes.STRING()),
            Column.physical("b", DataTypes.INT()),
            Column.physical("c", DataTypes.BOOLEAN()));

    private DeltaDynamicTableFactory tableFactory;

    private Map<String, String> options;

    private Map<String, String> originalEnvVariables;

    @BeforeEach
    public void setUp() {
        this.tableFactory = DeltaDynamicTableFactory.fromCatalog();
        this.options = new HashMap<>();
        this.options.put(FactoryUtil.CONNECTOR.key(), "delta");
        this.originalEnvVariables = System.getenv();
    }

    @AfterEach
    public void afterEach() {
        CommonTestUtils.setEnv(originalEnvVariables, true);
    }

    @Test
    void shouldLoadHadoopConfFromHadoopHomeEnv() {

        String path = "src/test/resources/hadoop-conf";
        File file = new File(path);
        String confDir = file.getAbsolutePath();

        options.put("table-path", "file://some/path");
        Context tableContext = DeltaTestUtils.createTableContext(SCHEMA, options);

        CommonTestUtils.setEnv(Collections.singletonMap("HADOOP_HOME", confDir), true);

        DeltaDynamicTableSink dynamicTableSink =
            (DeltaDynamicTableSink) tableFactory.createDynamicTableSink(tableContext);

        Configuration sourceHadoopConf = dynamicTableSink.getHadoopConf();
        Assertions.assertThat(sourceHadoopConf.get("dummy.property1", "noValue_asDefault"))
            .isEqualTo("false-value");
        Assertions.assertThat(sourceHadoopConf.get("dummy.property2", "noValue_asDefault"))
            .isEqualTo("11");

        Configuration sinkHadoopConf = dynamicTableSink.getHadoopConf();
        Assertions.assertThat(sinkHadoopConf.get("dummy.property1", "noValue_asDefault"))
            .isEqualTo("false-value");
        Assertions.assertThat(sinkHadoopConf.get("dummy.property2", "noValue_asDefault"))
            .isEqualTo("11");
    }

    @Test
    void shouldValidateMissingTablePathOption() {

        Context tableContext = DeltaTestUtils.createTableContext(SCHEMA, Collections.emptyMap());

        ValidationException validationException = assertThrows(
            ValidationException.class,
            () -> tableFactory.createDynamicTableSink(tableContext)
        );

        LOG.info(validationException.getMessage());
    }

    @Test
    void shouldThrowIfUsedUnexpectedOption() {
        options.put("table-path", "file://some/path");
        options.put("invalid-Option", "MyTarget");
        Context tableContext = DeltaTestUtils.createTableContext(SCHEMA, options);

        ValidationException sinkValidationException = assertThrows(
            ValidationException.class,
            () -> tableFactory.createDynamicTableSink(tableContext)
        );

        ValidationException sourceValidationException = assertThrows(
            ValidationException.class,
            () -> tableFactory.createDynamicTableSource(tableContext)
        );

        assertThat(sinkValidationException.getMessage())
            .isEqualTo(""
                + "Currently no job-specific options are allowed in INSERT SQL statements.\n"
                + "Invalid options used:\n"
                + " - 'invalid-Option'"
            );
        assertThat(sourceValidationException.getMessage())
            .isEqualTo(""
                + "Only job-specific options are allowed in SELECT SQL statement.\n"
                + "Invalid options used: \n"
                + " - 'invalid-Option'\n"
                + "Allowed options:\n"
                + " - 'mode'\n"
                + " - 'startingTimestamp'\n"
                + " - 'ignoreDeletes'\n"
                + " - 'updateCheckIntervalMillis'\n"
                + " - 'startingVersion'\n"
                + " - 'ignoreChanges'\n"
                + " - 'versionAsOf'\n"
                + " - 'updateCheckDelayMillis'\n"
                + " - 'timestampAsOf'"
            );
    }

    // Verifies that none Delta tables, DeltaDynamicTableFactory will return table factory proper
    // for connector type.
    @Test
    public void shouldReturnNonDeltaSinkAndSourceFactory() {

        // Table Sink
        this.options.put(FactoryUtil.CONNECTOR.key(), "blackhole");
        Context tableContext = DeltaTestUtils.createTableContext(SCHEMA, options);

        DynamicTableSink dynamicTableSink =
            tableFactory.createDynamicTableSink(tableContext);
        // verify that we have a "blackHole" connector table factory.
        assertThat(dynamicTableSink.asSummaryString()).isEqualTo("BlackHole");

        // Table Source
        this.options.put(FactoryUtil.CONNECTOR.key(), "datagen");
        tableContext = DeltaTestUtils.createTableContext(SCHEMA, options);

        // verify that we have a "datagen" connector table factory.
        DynamicTableSource dynamicTableSource =
            tableFactory.createDynamicTableSource(tableContext);
        assertThat(dynamicTableSource.asSummaryString()).isEqualTo("DataGenTableSource");
    }

    // Verifies if Table Factory throws exception when used for creation of Delta Sink
    // or source and factory instance was created from public default constructor. Factory should be
    @Test
    public void shouldThrowIfNotFromCatalog() {
        this.tableFactory = new DeltaDynamicTableFactory();

        this.options.put(FactoryUtil.CONNECTOR.key(), "delta");
        Context tableContext = DeltaTestUtils.createTableContext(SCHEMA, options);

        RuntimeException sourceException = assertThrows(RuntimeException.class,
            () -> this.tableFactory.createDynamicTableSource(tableContext));

        RuntimeException sinkException = assertThrows(RuntimeException.class,
            () -> this.tableFactory.createDynamicTableSink(tableContext));

        assertThrowsNotUsingCatalog(sourceException);
        assertThrowsNotUsingCatalog(sinkException);
    }

    @Test
    public void shouldThrowIfInvalidJobSpecificOptionsUsed() {

        options.put("table-path", "file://some/path");
        Map<String, String> invalidOptions = Stream.of(
                "SPARK.some.option",
                "spark.some.option",
                "delta.logStore",
                "io.delta.storage.S3DynamoDBLogStore.ddb.region",
                "parquet.writer.max-padding"
            )
            .collect(Collectors.toMap(optionName -> optionName, s -> "aValue"));
        this.options.putAll(invalidOptions);
        Context tableContext = DeltaTestUtils.createTableContext(SCHEMA, this.options);

        ValidationException sinkValidationException = assertThrows(
            ValidationException.class,
            () -> tableFactory.createDynamicTableSink(tableContext)
        );

        ValidationException sourceValidationException = assertThrows(
            ValidationException.class,
            () -> tableFactory.createDynamicTableSource(tableContext)
        );

        assertThat(sinkValidationException.getMessage())
            .isEqualTo(""
                + "Currently no job-specific options are allowed in INSERT SQL statements.\n"
                + "Invalid options used:\n"
                + " - 'SPARK.some.option'\n"
                + " - 'spark.some.option'\n"
                + " - 'delta.logStore'\n"
                + " - 'io.delta.storage.S3DynamoDBLogStore.ddb.region'\n"
                + " - 'parquet.writer.max-padding'"
            );
        assertThat(sourceValidationException.getMessage())
            .isEqualTo(""
                + "Only job-specific options are allowed in SELECT SQL statement.\n"
                + "Invalid options used: \n"
                + " - 'SPARK.some.option'\n"
                + " - 'spark.some.option'\n"
                + " - 'delta.logStore'\n"
                + " - 'io.delta.storage.S3DynamoDBLogStore.ddb.region'\n"
                + " - 'parquet.writer.max-padding'\n"
                + "Allowed options:\n"
                + " - 'mode'\n"
                + " - 'startingTimestamp'\n"
                + " - 'ignoreDeletes'\n"
                + " - 'updateCheckIntervalMillis'\n"
                + " - 'startingVersion'\n"
                + " - 'ignoreChanges'\n"
                + " - 'versionAsOf'\n"
                + " - 'updateCheckDelayMillis'\n"
                + " - 'timestampAsOf'"
            );
    }

    private void assertThrowsNotUsingCatalog(RuntimeException exception) {
        assertThat(exception.getMessage())
            .contains("Delta Table SQL/Table API was used without Delta Catalog.");
    }
}
