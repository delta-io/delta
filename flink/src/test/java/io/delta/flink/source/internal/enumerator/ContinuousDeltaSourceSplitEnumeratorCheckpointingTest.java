package io.delta.flink.source.internal.enumerator;

import java.net.URI;
import java.util.Collections;
import static java.util.Collections.singletonMap;

import io.delta.flink.sink.utils.DeltaSinkTestUtils;
import io.delta.flink.source.internal.DeltaSourceConfiguration;
import io.delta.flink.source.internal.DeltaSourceOptions;
import io.delta.flink.source.internal.file.AddFileEnumerator;
import io.delta.flink.source.internal.state.DeltaEnumeratorStateCheckpoint;
import io.delta.flink.source.internal.state.DeltaSourceSplit;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.connector.file.src.assigners.FileSplitAssigner;
import org.apache.flink.connector.testutils.source.reader.TestingSplitEnumeratorContext;
import org.apache.flink.core.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.delta.standalone.DeltaLog;
import io.delta.standalone.Snapshot;
import io.delta.standalone.VersionLog;
import io.delta.standalone.actions.AddFile;

/**
 * Tests in this class verifies if enumerator's state checkpoint created from {@link
 * org.apache.flink.api.connector.source.SplitEnumerator#snapshotState(long)} contains a valid
 * {@link Snapshot} version so enumerator can be recovered using this checkpoint via {@link
 * ContinuousSplitEnumeratorProvider#createEnumeratorForCheckpoint(DeltaEnumeratorStateCheckpoint,
 * Configuration, SplitEnumeratorContext, DeltaSourceConfiguration)} method and resume work from
 * correct snapshot version.
 */
@RunWith(MockitoJUnitRunner.class)
public class ContinuousDeltaSourceSplitEnumeratorCheckpointingTest {

    private static final String TABLE_PATH = "s3://some/path/";

    private static final AddFile ADD_FILE =
        new AddFile(TABLE_PATH + "file.parquet", Collections.emptyMap(), 100,
            System.currentTimeMillis(), true, "", Collections.emptyMap());

    private static final long HEAD_VERSION = 10;

    @Mock
    private Path tablePath;

    @Mock
    private DeltaLog deltaLog;

    @Mock
    private Snapshot headSnapshot;

    @Mock
    private FileSplitAssigner splitAssigner;

    @Mock
    private FileSplitAssigner.Provider splitAssignerProvider;

    @Mock
    private AddFileEnumerator<DeltaSourceSplit> fileEnumerator;

    @Mock
    private AddFileEnumerator.Provider<DeltaSourceSplit> fileEnumeratorProvider;

    @Captor
    private ArgumentCaptor<Long> monitorVersionCaptor;

    private MockedStatic<DeltaLog> deltaLogStatic;

    private ContinuousSplitEnumeratorProvider splitEnumeratorProvider;

    @Before
    public void setUp() {
        when(headSnapshot.getVersion()).thenReturn(HEAD_VERSION);
        when(deltaLog.getPath()).thenReturn(new org.apache.hadoop.fs.Path(TABLE_PATH));

        deltaLogStatic = Mockito.mockStatic(DeltaLog.class);
        deltaLogStatic.when(() -> DeltaLog.forTable(any(Configuration.class), anyString()))
            .thenReturn(this.deltaLog);

        when(tablePath.toUri()).thenReturn(URI.create(TABLE_PATH));

        when(fileEnumeratorProvider.create()).thenReturn(fileEnumerator);
        when(splitAssignerProvider.create(any())).thenReturn(splitAssigner);

        splitEnumeratorProvider =
            new ContinuousSplitEnumeratorProvider(splitAssignerProvider, fileEnumeratorProvider);
    }

    @After
    public void after() {
        deltaLogStatic.close();
    }

    /**
     * This test verifies checkpoint and recovery from it for scenario:
     * <ul>
     *     <li>Read Snapshot for version N</li>
     *     <li>Read Changes from version N + 1</li>
     *     <li>Create a checkpoint</li>
     *     <li>Create a new {@link ContinuousDeltaSourceSplitEnumerator} from checkpoint</li>
     *     <li>Verifies that enumerator resumed monitoring for changes for version N + 2</li>
     * </ul>
     */
    @Test
    public void shouldCheckpointStateAfterSnapshotReadAndFirstChangeVersion() throws Exception {
        long firstMonitorVersion = HEAD_VERSION + 1;
        when(deltaLog.getSnapshotForVersionAsOf(HEAD_VERSION)).thenReturn(headSnapshot);
        when(deltaLog.getChanges(firstMonitorVersion, true)).thenReturn(
            Collections.singletonList(
                    new VersionLog(firstMonitorVersion, Collections.singletonList(ADD_FILE)))
                .iterator());
        when(deltaLog.getChanges(firstMonitorVersion + 1, true)).thenReturn(
            Collections.singletonList(
                    new VersionLog(firstMonitorVersion + 1, Collections.singletonList(ADD_FILE)))
                .iterator());

        TestingSplitEnumeratorContext<DeltaSourceSplit> enumContext =
            new TestingSplitEnumeratorContext<>(1);

        DeltaSourceConfiguration sourceConfiguration = new DeltaSourceConfiguration(
            singletonMap(DeltaSourceOptions.LOADED_SCHEMA_SNAPSHOT_VERSION.key(),
                headSnapshot.getVersion()));

        ContinuousDeltaSourceSplitEnumerator enumerator =
            splitEnumeratorProvider.createInitialStateEnumerator(tablePath,
                DeltaSinkTestUtils.getHadoopConf(), enumContext, sourceConfiguration);

        enumerator.start();

        // Verify that we try to read data from Snapshot
        verify(headSnapshot).getAllFiles();
        verify(deltaLog, never()).getChanges(anyLong(), anyBoolean());

        // start stub for work discovery thread
        enumContext.getExecutorService().triggerPeriodicScheduledTasks();

        verify(deltaLog).getChanges(monitorVersionCaptor.capture(), anyBoolean());
        assertThat(monitorVersionCaptor.getValue(), equalTo(firstMonitorVersion));

        DeltaEnumeratorStateCheckpoint<DeltaSourceSplit>
            checkpoint = enumerator.snapshotState(1);

        assertThat(checkpoint.getSnapshotVersion(), equalTo(firstMonitorVersion + 1));
        assertThat(checkpoint.isMonitoringForChanges(), equalTo(true));

        reset(headSnapshot);

        // reset enumContext and restore enumerator from checkpoint
        enumContext = new TestingSplitEnumeratorContext<>(1);
        splitEnumeratorProvider.createEnumeratorForCheckpoint(checkpoint,
                DeltaSinkTestUtils.getHadoopConf(), enumContext, new DeltaSourceConfiguration())
            .start();

        enumContext.getExecutorService().triggerPeriodicScheduledTasks();
        verify(deltaLog, times(2)).getChanges(monitorVersionCaptor.capture(), anyBoolean());

        assertThat(monitorVersionCaptor.getValue(), equalTo(firstMonitorVersion + 1));
    }

    /**
     * This test verifies checkpoint and recovery from it for scenario:
     * <ul>
     *     <li>Read Changes only from version N</li>
     *     <li>Create a checkpoint</li>
     *     <li>Create a new {@link ContinuousDeltaSourceSplitEnumerator} from checkpoint</li>
     *     <li>Verifies that enumerator resumed monitoring for changes for version N + 1</li>
     * </ul>
     */
    @Test
    public void shouldCheckpointStateAfterChangesProcessForChangesOnlyStream() throws Exception {
        when(deltaLog.getSnapshotForVersionAsOf(HEAD_VERSION)).thenReturn(headSnapshot);
        when(deltaLog.getChanges(HEAD_VERSION, true)).thenReturn(
            Collections.singletonList(
                    new VersionLog(HEAD_VERSION, Collections.singletonList(ADD_FILE)))
                .iterator());

        TestingSplitEnumeratorContext<DeltaSourceSplit> enumContext =
            new TestingSplitEnumeratorContext<>(1);

        DeltaSourceConfiguration sourceConfiguration = new DeltaSourceConfiguration();
        sourceConfiguration.addOption(DeltaSourceOptions.STARTING_VERSION, "latest");
        sourceConfiguration.addOption(
            DeltaSourceOptions.LOADED_SCHEMA_SNAPSHOT_VERSION,
            headSnapshot.getVersion()
        );

        ContinuousDeltaSourceSplitEnumerator enumerator =
            splitEnumeratorProvider.createInitialStateEnumerator(tablePath,
                DeltaSinkTestUtils.getHadoopConf(), enumContext, sourceConfiguration);

        enumerator.start();

        // Verify that we try to read data from Snapshot
        verify(deltaLog, never()).getChanges(anyLong(), anyBoolean());

        // start work discovery thread
        enumContext.getExecutorService().triggerPeriodicScheduledTasks();

        verify(deltaLog).getChanges(monitorVersionCaptor.capture(), anyBoolean());
        assertThat(monitorVersionCaptor.getValue(), equalTo(HEAD_VERSION));

        DeltaEnumeratorStateCheckpoint<DeltaSourceSplit>
            checkpoint = enumerator.snapshotState(1);

        assertThat(checkpoint.getSnapshotVersion(), equalTo(HEAD_VERSION + 1));
        assertThat(checkpoint.isMonitoringForChanges(), equalTo(true));

        // reset enumContext and restore enumerator from checkpoint
        enumContext = new TestingSplitEnumeratorContext<>(1);
        splitEnumeratorProvider.createEnumeratorForCheckpoint(checkpoint,
                DeltaSinkTestUtils.getHadoopConf(), enumContext, new DeltaSourceConfiguration())
            .start();

        enumContext.getExecutorService().triggerPeriodicScheduledTasks();
        verify(deltaLog, times(2)).getChanges(monitorVersionCaptor.capture(), anyBoolean());

        assertThat(monitorVersionCaptor.getValue(), equalTo(HEAD_VERSION + 1));
    }

    /**
     * This test verifies checkpoint and recovery from it for scenario:
     * <ul>
     *     <li>Read Snapshot for version N</li>
     *     <li>Create a checkpoint</li>
     *     <li>Create a new {@link ContinuousDeltaSourceSplitEnumerator} from checkpoint</li>
     *     <li>Read Changes from version N + 1</li>
     * </ul>
     */
    @Test
    public void shouldCheckpointStateAfterSnapshotReadAndBeforeFirstChangeVersion()
        throws Exception {
        when(deltaLog.getSnapshotForVersionAsOf(HEAD_VERSION)).thenReturn(headSnapshot);

        ContinuousDeltaSourceSplitEnumerator enumerator =
            splitEnumeratorProvider.createInitialStateEnumerator(
                tablePath,
                DeltaSinkTestUtils.getHadoopConf(),
                new TestingSplitEnumeratorContext<>(1),
                new DeltaSourceConfiguration(
                    singletonMap(DeltaSourceOptions.LOADED_SCHEMA_SNAPSHOT_VERSION.key(),
                        headSnapshot.getVersion()))
            );

        enumerator.start();
        DeltaEnumeratorStateCheckpoint<DeltaSourceSplit> checkpoint = enumerator.snapshotState(1);

        verify(deltaLog, never()).getChanges(anyLong(), anyBoolean());
        assertThat(checkpoint.getSnapshotVersion(), equalTo(HEAD_VERSION + 1));
        assertThat(checkpoint.isMonitoringForChanges(), equalTo(true));

        TestingSplitEnumeratorContext<DeltaSourceSplit> enumContext =
            new TestingSplitEnumeratorContext<>(1);

        ContinuousDeltaSourceSplitEnumerator recoveredEnumerator =
            splitEnumeratorProvider.createEnumeratorForCheckpoint(
                checkpoint,
                DeltaSinkTestUtils.getHadoopConf(),
                enumContext,
                new DeltaSourceConfiguration(
                    singletonMap(DeltaSourceOptions.LOADED_SCHEMA_SNAPSHOT_VERSION.key(),
                    headSnapshot.getVersion()))
            );
        recoveredEnumerator.start();

        enumContext.getExecutorService().triggerPeriodicScheduledTasks();
        verify(deltaLog).getChanges(monitorVersionCaptor.capture(), anyBoolean());

        assertThat(monitorVersionCaptor.getValue(), equalTo(HEAD_VERSION + 1));
    }

}
