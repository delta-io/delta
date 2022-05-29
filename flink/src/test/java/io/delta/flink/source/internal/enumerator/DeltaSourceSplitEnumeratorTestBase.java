package io.delta.flink.source.internal.enumerator;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import io.delta.flink.sink.utils.DeltaSinkTestUtils;
import io.delta.flink.source.internal.DeltaSourceConfiguration;
import io.delta.flink.source.internal.DeltaSourceOptions;
import io.delta.flink.source.internal.file.AddFileEnumerator;
import io.delta.flink.source.internal.file.AddFileEnumerator.SplitFilter;
import io.delta.flink.source.internal.file.AddFileEnumeratorContext;
import io.delta.flink.source.internal.state.DeltaEnumeratorStateCheckpoint;
import io.delta.flink.source.internal.state.DeltaSourceSplit;
import org.apache.flink.api.connector.source.ReaderInfo;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.connector.file.src.assigners.FileSplitAssigner;
import org.apache.flink.core.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import static io.delta.flink.source.internal.enumerator.SourceSplitEnumeratorTestUtils.mockFileEnumerator;
import static io.delta.flink.source.internal.enumerator.SourceSplitEnumeratorTestUtils.mockSplits;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.delta.standalone.DeltaLog;
import io.delta.standalone.Snapshot;
import io.delta.standalone.actions.AddFile;

/**
 * A base test class that covers common logic for both {@link BoundedDeltaSourceSplitEnumerator} and
 * {@link ContinuousDeltaSourceSplitEnumerator}. Tests here have same setup and assertions for both
 * {@code SourceSplitEnumerator} implementations.
 *
 * @implNote The child class has to implement the {@link #createEnumerator()} method, which returns
 * concrete {@link DeltaSourceSplitEnumerator} implementation.
 */
public abstract class DeltaSourceSplitEnumeratorTestBase {

    private static final String TABLE_PATH = "s3://some/path/";

    @Mock
    protected Path deltaTablePath;

    @Mock
    protected AddFileEnumerator<DeltaSourceSplit> fileEnumerator;

    @Mock
    protected AddFileEnumerator.Provider<DeltaSourceSplit> fileEnumeratorProvider;

    @Mock
    protected FileSplitAssigner.Provider splitAssignerProvider;

    @Mock
    protected FileSplitAssigner splitAssigner;

    @Mock
    protected SplitEnumeratorContext<DeltaSourceSplit> enumContext;

    @Mock
    protected DeltaLog deltaLog;

    @Mock
    protected Snapshot headSnapshot;

    @Mock
    protected Snapshot checkpointedSnapshot;

    @Mock
    protected ReaderInfo readerInfo;

    @Mock
    private DeltaSourceSplit split;

    @Captor
    private ArgumentCaptor<List<FileSourceSplit>> splitsCaptor;

    protected MockedStatic<DeltaLog> deltaLogStatic;

    protected DeltaSourceConfiguration sourceConfiguration;

    private DeltaSourceSplitEnumerator enumerator;

    protected void setUp() throws URISyntaxException {
        sourceConfiguration = new DeltaSourceConfiguration();
        deltaLogStatic = Mockito.mockStatic(DeltaLog.class);
        deltaLogStatic.when(() -> DeltaLog.forTable(any(Configuration.class), anyString()))
            .thenReturn(this.deltaLog);

        when(deltaTablePath.toUri()).thenReturn(new URI(TABLE_PATH));
    }

    protected void after() {
        deltaLogStatic.close();
    }

    @Test
    public void shouldHandleFailedReader() {
        enumerator = setUpEnumeratorWithHeadSnapshot();

        // Mock reader failure.
        when(enumContext.registeredReaders()).thenReturn(Collections.emptyMap());

        int subtaskId = 1;
        enumerator.handleSplitRequest(subtaskId, "testHost");
        verify(enumContext, never()).assignSplit(any(DeltaSourceSplit.class), anyInt());
    }

    @Test
    public void shouldAssignSplitToReader() {
        int subtaskId = 1;
        enumerator = setUpEnumeratorWithHeadSnapshot();

        when(enumContext.registeredReaders()).thenReturn(
            Collections.singletonMap(subtaskId, readerInfo));

        String host = "testHost";
        when(splitAssigner.getNext(host)).thenReturn(Optional.of(split))
            .thenReturn(Optional.empty());

        // handle request split when there is a split to assign
        enumerator.handleSplitRequest(subtaskId, host);
        verify(enumContext).assignSplit(split, subtaskId);
        verify(enumContext, never()).signalNoMoreSplits(anyInt());

        // check that we clear split from enumerator after assigning them.
        enumerator.handleSplitRequest(subtaskId, host);
        verify(enumContext).assignSplit(split, subtaskId); // the one from previous assignment.
        verify(enumerator).handleNoMoreSplits(subtaskId);
    }

    @Test
    public void shouldAddSplitBack() {
        int subtaskId = 1;
        enumerator = setUpEnumeratorWithHeadSnapshot();

        when(enumContext.registeredReaders()).thenReturn(
            Collections.singletonMap(subtaskId, readerInfo));

        String testHost = "testHost";
        enumerator.handleSplitRequest(subtaskId, testHost);
        verify(enumerator).handleNoMoreSplits(subtaskId);

        enumerator.addSplitsBack(Collections.singletonList(split), subtaskId);

        //capture the assigned split to mock assigner and use it in getNext mock
        verify(splitAssigner).addSplits(splitsCaptor.capture());

        when(splitAssigner.getNext(testHost)).thenReturn(
            Optional.ofNullable(splitsCaptor.getValue().get(0)));
        enumerator.handleSplitRequest(subtaskId, testHost);
        verify(enumContext).assignSplit(split, subtaskId);
    }

    @Test
    public void shouldReadInitialSnapshot() {

        enumerator = setUpEnumeratorWithHeadSnapshot();

        List<DeltaSourceSplit> mockSplits = mockSplits();
        when(fileEnumerator.enumerateSplits(any(AddFileEnumeratorContext.class),
            any(SplitFilter.class)))
            .thenReturn(mockSplits);

        enumerator.start();

        verify(splitAssigner).addSplits(splitsCaptor.capture());
        assertThat(splitsCaptor.getValue(), equalTo(mockSplits));
    }

    @Test
    public void shouldNotProcessAlreadyProcessedPaths() {
        enumerator = setUpEnumeratorWithHeadSnapshot();

        AddFile mockAddFile = mock(AddFile.class);
        when(mockAddFile.getPath()).thenReturn("add/file/path.parquet");
        when(headSnapshot.getAllFiles()).thenReturn(Collections.singletonList(mockAddFile));

        mockFileEnumerator(fileEnumerator);

        enumerator.start();

        verify(splitAssigner).addSplits(splitsCaptor.capture());
        assertThat(splitsCaptor.getValue().size(), equalTo(1));

        // Reprocess the same data again
        enumerator.start();

        verify(splitAssigner, times(2)).addSplits(splitsCaptor.capture());
        assertThat(splitsCaptor.getValue().isEmpty(), equalTo(true));
    }

    @SuppressWarnings("unchecked")
    protected <T> T setUpEnumeratorWithHeadSnapshot() {
        when(deltaLog.getSnapshotForVersionAsOf(
            headSnapshot.getVersion())).thenReturn(headSnapshot);
        sourceConfiguration.addOption(
            DeltaSourceOptions.LOADED_SCHEMA_SNAPSHOT_VERSION.key(), headSnapshot.getVersion());
        return (T) spy(createEnumerator());
    }

    @SuppressWarnings("unchecked")
    protected <T> T setUpEnumerator() {
        return (T) spy(createEnumerator());
    }

    @SuppressWarnings("unchecked")
    protected <T> T setUpEnumeratorFromCheckpoint(
        DeltaEnumeratorStateCheckpoint<DeltaSourceSplit> checkpoint) {
        return (T) spy(createEnumerator(checkpoint));
    }

    protected abstract SplitEnumeratorProvider getProvider();

    protected DeltaSourceSplitEnumerator createEnumerator() {
        return (DeltaSourceSplitEnumerator) getProvider().createInitialStateEnumerator(
            new Path(TABLE_PATH),
            DeltaSinkTestUtils.getHadoopConf(), enumContext, sourceConfiguration);
    }

    protected DeltaSourceSplitEnumerator createEnumerator(
        DeltaEnumeratorStateCheckpoint<DeltaSourceSplit> checkpoint) {
        return (DeltaSourceSplitEnumerator) getProvider().createEnumeratorForCheckpoint(checkpoint,
            DeltaSinkTestUtils.getHadoopConf(), enumContext, sourceConfiguration);
    }
}
