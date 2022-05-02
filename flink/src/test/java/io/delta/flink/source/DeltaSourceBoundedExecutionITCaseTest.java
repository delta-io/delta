package io.delta.flink.source;

import java.io.File;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import io.delta.flink.DeltaTestUtils;
import io.delta.flink.source.RecordCounterToFail.FailCheck;
import io.delta.flink.source.internal.DeltaSourceConfiguration;
import io.delta.flink.source.internal.enumerator.BoundedSplitEnumeratorProvider;
import io.delta.flink.source.internal.file.DeltaFileEnumerator;
import io.delta.flink.source.internal.state.DeltaSourceSplit;
import org.apache.flink.connector.file.src.assigners.LocalityAwareSplitAssigner;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.ParquetColumnarRowInputFormat;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
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
    // NOTE that this test can take some time to finish since we are restarting JM here.
    // It can be around 30 seconds or so.
    // Test if SplitEnumerator::addSplitsBack works well,
    // meaning if splits were added back to the Enumerator's state and reassigned to new TM.
    public void shouldReadDeltaTable() throws Exception {

        DeltaSource<RowData> deltaSource =
            initBoundedSource(
                Path.fromLocalFile(new File(nonPartitionedLargeTablePath)),
                LARGE_TABLE_COLUMN_NAMES,
                LARGE_TABLE_COLUMN_TYPES);

        // WHEN
        // Fail TaskManager or JobManager after half of the records or do not fail anything if
        // FailoverType.NONE.
        List<RowData> resultData = testBoundDeltaSource(failoverType, deltaSource,
            (FailCheck) readRows -> readRows == LARGE_TABLE_RECORD_COUNT / 2);

        Set<Long> actualValues =
            resultData.stream().map(row -> row.getLong(0)).collect(Collectors.toSet());

        // THEN
        assertThat("Source read different number of rows that Delta table have.", resultData.size(),
            equalTo(LARGE_TABLE_RECORD_COUNT));
        assertThat("Source Must Have produced some duplicates.", actualValues.size(),
            equalTo(LARGE_TABLE_RECORD_COUNT));
    }

    // TODO PR 8 ADD Partition tests in later PRs

    // TODO PR 9 for future PRs
    //  This is a temporary method for creating DeltaSource.
    //  The Desired state is to use DeltaSourceBuilder which was not included in this PR.
    //  For reference how DeltaSource creation will look like please go to:
    //  https://github.com/delta-io/connectors/pull/256/files#:~:text=testWithoutPartitions()

    private DeltaSource<RowData> initBoundedSource(Path nonPartitionedTablePath,
        String[] columnNames, LogicalType[] columnTypes) {

        Configuration hadoopConf = DeltaTestUtils.getHadoopConf();

        ParquetColumnarRowInputFormat<DeltaSourceSplit>
            fileSourceSplitParquetColumnarRowInputFormat = new ParquetColumnarRowInputFormat<>(
            hadoopConf,
            RowType.of(columnTypes, columnNames),
            2048, // Parquet Reader batchSize
            true, // isUtcTimestamp
            true);// isCaseSensitive

        return DeltaSource.forBulkFileFormat(
            nonPartitionedTablePath,
            fileSourceSplitParquetColumnarRowInputFormat,
            new BoundedSplitEnumeratorProvider(
                LocalityAwareSplitAssigner::new, DeltaFileEnumerator::new),
            hadoopConf, new DeltaSourceConfiguration());
    }
}
