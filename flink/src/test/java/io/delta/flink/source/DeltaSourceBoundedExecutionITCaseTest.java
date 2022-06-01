package io.delta.flink.source;

import java.io.File;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import io.delta.flink.DeltaTestUtils;
import io.delta.flink.source.ContinuousTestDescriptor.Descriptor;
import io.delta.flink.source.RecordCounterToFail.FailCheck;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;

@RunWith(Parameterized.class)
public class DeltaSourceBoundedExecutionITCaseTest extends DeltaSourceITBase {

    private final FailoverType failoverType;

    public DeltaSourceBoundedExecutionITCaseTest(FailoverType failoverType) {
        this.failoverType = failoverType;
    }

    @Parameters(name = "{index}: FailoverType = [{0}]")
    public static Collection<Object[]> param() {
        return Arrays.asList(new Object[][]{
            {FailoverType.NONE}, {FailoverType.TASK_MANAGER}, {FailoverType.JOB_MANAGER}
        });
    }

    @Before
    public void setup() {
        super.setup();
    }

    @After
    public void after() {
        super.after();
    }

    @Test
    public void shouldReadDeltaTableUsingDeltaLogSchema() throws Exception {
        DeltaSource<RowData> deltaSource =
            initBoundedSourceAllColumns(nonPartitionedLargeTablePath);

        shouldReadDeltaTable(deltaSource);
    }

    @Test
    // NOTE that this test can take some time to finish since we are restarting JM here.
    // It can be around 30 seconds or so.
    // Test if SplitEnumerator::addSplitsBack works well,
    // meaning if splits were added back to the Enumerator's state and reassigned to new TM.
    public void shouldReadDeltaTableUsingUserSchema() throws Exception {

        DeltaSource<RowData> deltaSource =
            initBoundedSourceForColumns(nonPartitionedLargeTablePath, LARGE_TABLE_COLUMN_NAMES);

        shouldReadDeltaTable(deltaSource);
    }

    /**
     * This test verifies that Delta source is reading the same snapshot that was used by Source
     * builder for schema discovery.
     * <p>
     * The Snapshot is created two times, first time in builder for schema discovery and second
     * time during source enumerator object initialization, which happens when job is deployed on a
     * Flink cluster. We need to make sure that the same snapshot will be used in both cases.
     * <p>
     * Test scenario:
     * <ul>
     *     <li>
     *         Create source object. In this step, source will get Delta table head snapshot
     *         (version 0) and build schema from its metadata.
     *     </li>
     *     <li>
     *         Update Delta table by adding one extra row. This will change head Snapshot to
     *         version 1.
     *     </li>
     *     <li>
     *         Start the pipeline, Delta source will start reading Delta table.
     *     </li>
     *     <li>
     *         Expectation is that Source should read the version 0, the one that was used for
     *         creating format schema. Version 0 has 2 records in it.
     *     </li>
     * </ul>
     *
     */
    @Test
    // TODO PR 11 - no need to test this in all Failover scenarios will be fixed after migrating
    //  this class to Junit5 in PR 11.
    public void shouldReadLoadedSchemaVersion() throws Exception {

        // Create a Delta source instance. In this step, builder discovered Delta table schema
        // and create Table format based on this schema acquired from snapshot.
        DeltaSource<RowData> source = initBoundedSourceAllColumns(nonPartitionedTablePath);

        // Updating table with new data, changing head  Snapshot version.
        Descriptor update = new Descriptor(
            RowType.of(true, SMALL_TABLE_COLUMN_TYPES, SMALL_TABLE_COLUMN_NAMES),
            Collections.singletonList(Row.of("John-K", "Wick-P", 1410))
        );

        DeltaTableUpdater tableUpdater = new DeltaTableUpdater(nonPartitionedTablePath);
        tableUpdater.writeToTable(update);

        // Starting pipeline and reading the data. Source should read Snapshot version used for
        // schema discovery in buildr, so before table update.
        List<RowData> rowData = testBoundDeltaSource(source);

        // We are expecting to read version 0, before table update.
        assertThat(rowData.size(), equalTo(SMALL_TABLE_COUNT));

    }

    private void shouldReadDeltaTable(
        DeltaSource<RowData> deltaSource) throws Exception {
        // WHEN
        // Fail TaskManager or JobManager after half of the records or do not fail anything if
        // FailoverType.NONE.
        List<RowData> resultData = testBoundDeltaSource(failoverType, deltaSource,
            (FailCheck) readRows -> readRows == LARGE_TABLE_RECORD_COUNT / 2);

        Set<Long> actualValues =
            resultData.stream().map(row -> row.getLong(0)).collect(Collectors.toSet());

        // THEN
        assertThat("Source read different number of rows that Delta table have.",
            resultData.size(),
            equalTo(LARGE_TABLE_RECORD_COUNT));
        assertThat("Source Must Have produced some duplicates.", actualValues.size(),
            equalTo(LARGE_TABLE_RECORD_COUNT));
    }

    // TODO PR 11 ADD Partition tests in later PRs

    /**
     * Initialize a Delta source in bounded mode that should take entire Delta table schema
     * from Delta's metadata.
     */
    private DeltaSource<RowData> initBoundedSourceAllColumns(String tablePath) {

        Configuration hadoopConf = DeltaTestUtils.getHadoopConf();

        return DeltaSource.forBoundedRowData(
                Path.fromLocalFile(new File(tablePath)),
                hadoopConf
            )
            .build();
    }

    /**
     * Initialize a Delta source in bounded mode that should take only user defined columns
     * from Delta's metadata.
     */
    private DeltaSource<RowData> initBoundedSourceForColumns(
            String tablePath,
            String[] columnNames) {

        Configuration hadoopConf = DeltaTestUtils.getHadoopConf();

        return DeltaSource.forBoundedRowData(
                Path.fromLocalFile(new File(tablePath)),
                hadoopConf
            )
            .columnNames(Arrays.asList(columnNames))
            .build();
    }
}
