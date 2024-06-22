package io.delta.flink.internal.table;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import io.delta.flink.internal.table.DeltaFlinkJobSpecificOptions.QueryMode;
import io.delta.flink.source.DeltaSource;
import io.delta.flink.source.internal.DeltaSourceOptions;
import io.delta.flink.utils.DeltaTestUtils;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.table.connector.source.ScanTableSource.ScanContext;
import org.apache.flink.table.connector.source.SourceProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.connector.source.ScanRuntimeProviderContext;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.rules.TemporaryFolder;
import static org.assertj.core.api.Assertions.assertThat;

public class DeltaDynamicTableSourceTest {

    private static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

    private DeltaDynamicTableSource tableSource;

    private Configuration hadoopConf;

    private String tablePath;

    private ScanContext context;

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
        this.context = new ScanRuntimeProviderContext();
        this.hadoopConf = DeltaTestUtils.getHadoopConf();
        this.tablePath = TEMPORARY_FOLDER.newFolder().getAbsolutePath();
    }

    @SuppressWarnings("unchecked")
    @ParameterizedTest
    // those two options are mutually exclusive hence they have to be tested separately.
    // option name, option value
    @CsvSource(value = {
        "versionAsOf, 2",
        "timestampAsOf, 2022-02-24 04:55:00"
    })
    public void shouldCreateBoundedSourceWithOptions(String name, String value) throws IOException {

        DeltaTestUtils.initTestForVersionedTable(tablePath);

        if (name.equals("timestampAsOf")) {
            DeltaTestUtils.changeDeltaLogLastModifyTimestamp(tablePath, new String[] {value});
        }

        QueryOptions queryOptions = new QueryOptions(
            tablePath,
            QueryMode.BATCH,
            Collections.singletonMap(name, value)
        );

        this.tableSource = new DeltaDynamicTableSource(
            hadoopConf,
            queryOptions,
            Collections.singletonList("col1") // as user would call SELECT col1 FROM ...
        );

        SourceProvider provider = (SourceProvider) this.tableSource.getScanRuntimeProvider(context);
        DeltaSource<RowData> deltaSource = (DeltaSource<RowData>) provider.createSource();

        assertThat(deltaSource.getBoundedness()).isEqualTo(Boundedness.BOUNDED);

        // The getSourceConfiguration().getUsedOptions() will also contain
        // loadedSchemaSnapshotVersion option.
        // Please see DeltaSourceOptions.LOADED_SCHEMA_SNAPSHOT_VERSION for more information
        // about this option.
        assertThat(deltaSource.getSourceConfiguration().getUsedOptions())
            .containsExactlyInAnyOrder(
                name,
                DeltaSourceOptions.LOADED_SCHEMA_SNAPSHOT_VERSION.key()
            );
    }

    @SuppressWarnings("unchecked")
    @ParameterizedTest
    // those two options are mutually exclusive hence they have to be tested separately.
    @CsvSource(value = {
        "startingVersion, 2",
        "startingTimestamp, 2022-02-24 04:55:00"
    })
    public void shouldCreateContinuousSourceWithOptions(String name, String value)
        throws IOException {

        DeltaTestUtils.initTestForVersionedTable(tablePath);

        // Continuous mode has more options that can be used together comparing to BATCH mode.
        Map<String, String> jobOptions = new HashMap<>();
        jobOptions.put("updateCheckIntervalMillis", "1000");
        jobOptions.put("updateCheckDelayMillis", "1000");
        jobOptions.put("ignoreDeletes", "true");
        jobOptions.put("ignoreChanges", "true");
        jobOptions.put(name, value);

        if (name.equals("timestampAsOf")) {
            DeltaTestUtils.changeDeltaLogLastModifyTimestamp(tablePath, new String[] {value});
        }

        QueryOptions queryOptions = new QueryOptions(
            tablePath,
            QueryMode.STREAMING,
            jobOptions
        );

        this.tableSource = new DeltaDynamicTableSource(
            hadoopConf,
            queryOptions,
            Collections.singletonList("col1") // as user would call SELECT col1 FROM ...
        );

        SourceProvider provider = (SourceProvider) this.tableSource.getScanRuntimeProvider(context);
        DeltaSource<RowData> deltaSource = (DeltaSource<RowData>) provider.createSource();

        assertThat(deltaSource.getBoundedness()).isEqualTo(Boundedness.CONTINUOUS_UNBOUNDED);

        // The getSourceConfiguration().getUsedOptions() will also contain
        // loadedSchemaSnapshotVersion option.
        // Please see DeltaSourceOptions.LOADED_SCHEMA_SNAPSHOT_VERSION for more information
        // about this option.
        assertThat(deltaSource.getSourceConfiguration().getUsedOptions())
            .containsExactlyInAnyOrder(
                name,
                "updateCheckIntervalMillis",
                "updateCheckDelayMillis",
                "ignoreDeletes",
                "ignoreChanges",
                DeltaSourceOptions.LOADED_SCHEMA_SNAPSHOT_VERSION.key()
            );
    }

}
