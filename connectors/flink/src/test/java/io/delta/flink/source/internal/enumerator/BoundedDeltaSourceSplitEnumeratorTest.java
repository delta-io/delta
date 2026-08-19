package io.delta.flink.source.internal.enumerator;

import java.net.URISyntaxException;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

import io.delta.flink.source.internal.DeltaSourceOptions;
import io.delta.flink.source.internal.file.AddFileEnumerator.SplitFilter;
import io.delta.flink.source.internal.file.AddFileEnumeratorContext;
import io.delta.flink.source.internal.state.DeltaEnumeratorStateCheckpoint;
import io.delta.flink.source.internal.state.DeltaEnumeratorStateCheckpointBuilder;
import io.delta.flink.source.internal.state.DeltaSourceSplit;
import org.apache.flink.api.connector.source.ReaderInfo;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.delta.standalone.Snapshot;

@RunWith(MockitoJUnitRunner.class)
public class BoundedDeltaSourceSplitEnumeratorTest extends DeltaSourceSplitEnumeratorTestBase {

    @Mock
    private Snapshot versionAsOfSnapshot;

    @Mock
    private Snapshot timestampAsOfSnapshot;

    private BoundedDeltaSourceSplitEnumerator enumerator;

    private BoundedSplitEnumeratorProvider provider;

    @Before
    public void setUp() throws URISyntaxException {
        super.setUp();

        when(splitAssignerProvider.create(any())).thenReturn(splitAssigner);
        when(fileEnumeratorProvider.create()).thenReturn(fileEnumerator);

        provider =
            new BoundedSplitEnumeratorProvider(splitAssignerProvider, fileEnumeratorProvider);
    }

    @After
    public void after() {
        super.after();
    }

    @Test
    public void shouldUseVersionAsOfSnapshot() {

        long versionAsOf = 10;
        when(deltaLog.getSnapshotForVersionAsOf(versionAsOf)).thenReturn(versionAsOfSnapshot);
        when(versionAsOfSnapshot.getVersion()).thenReturn(versionAsOf);

        sourceConfiguration.addOption(DeltaSourceOptions.VERSION_AS_OF, versionAsOf);
        sourceConfiguration.addOption(
            DeltaSourceOptions.LOADED_SCHEMA_SNAPSHOT_VERSION,
            versionAsOfSnapshot.getVersion()
        );

        enumerator = setUpEnumerator();
        enumerator.start();

        // verify that we use provided option to create snapshot and not use the deltaLog
        // .snapshot()
        verify(deltaLog).getSnapshotForVersionAsOf(versionAsOf);
        verify(deltaLog, never()).snapshot();
        verify(deltaLog, never()).getSnapshotForTimestampAsOf(anyLong());

        // verify that we read snapshot content
        verify(versionAsOfSnapshot).getAllFiles();
        verify(fileEnumerator).enumerateSplits(
            any(AddFileEnumeratorContext.class),
            any(SplitFilter.class)
        );

        // verify that Processor Callback was executed.
        verify(splitAssigner).addSplits(any(Collection.class));
    }

    @Test
    public void shouldUseTimestampAsOfSnapshot() {
        long timestampAsOfString = System.currentTimeMillis();
        long timestampAsOfVersion = 10;

        when(deltaLog.getSnapshotForVersionAsOf(timestampAsOfVersion)).thenReturn(
            timestampAsOfSnapshot);
        when(timestampAsOfSnapshot.getVersion()).thenReturn(timestampAsOfVersion);

        sourceConfiguration.addOption(DeltaSourceOptions.TIMESTAMP_AS_OF, timestampAsOfString);
        sourceConfiguration.addOption(
            DeltaSourceOptions.LOADED_SCHEMA_SNAPSHOT_VERSION,
            timestampAsOfSnapshot.getVersion()
        );

        enumerator = setUpEnumerator();
        enumerator.start();

        // verify that we read snapshot content
        verify(timestampAsOfSnapshot).getAllFiles();
        verify(fileEnumerator).enumerateSplits(
            any(AddFileEnumeratorContext.class),
            any(SplitFilter.class)
        );

        // verify that Processor Callback was executed.
        verify(splitAssigner).addSplits(any(Collection.class));
    }

    @Test
    public void shouldUseCheckpointSnapshot() {
        long snapshotVersion = 10;
        when(deltaLog.getSnapshotForVersionAsOf(snapshotVersion)).thenReturn(
            checkpointedSnapshot);
        when(checkpointedSnapshot.getVersion()).thenReturn(snapshotVersion);

        DeltaEnumeratorStateCheckpoint<DeltaSourceSplit> checkpoint =
            DeltaEnumeratorStateCheckpointBuilder
                .builder(deltaTablePath, snapshotVersion, Collections.emptyList())
                .build();

        enumerator = setUpEnumeratorFromCheckpoint(checkpoint);
        enumerator.start();

        // verify that we use provided option to create snapshot and not use the deltaLog
        // .snapshot()
        verify(deltaLog).getSnapshotForVersionAsOf(snapshotVersion);
        verify(deltaLog, never()).snapshot();
        verify(deltaLog, never()).getSnapshotForTimestampAsOf(anyLong());

        // verify that we read snapshot content
        verify(checkpointedSnapshot).getAllFiles();
        verify(fileEnumerator).enumerateSplits(any(AddFileEnumeratorContext.class), any(
            SplitFilter.class));

        // verify that Processor Callback was executed.
        verify(splitAssigner).addSplits(any(Collection.class));
    }

    @Test
    public void shouldSignalNoMoreSplitsIfNone() {
        int subtaskId = 1;
        enumerator = setUpEnumeratorWithHeadSnapshot();

        when(enumContext.registeredReaders()).thenReturn(
            Collections.singletonMap(subtaskId, readerInfo));

        enumerator.handleSplitRequest(subtaskId, "testHost");

        verify(enumContext).signalNoMoreSplits(subtaskId);
    }

    @Test
    public void shouldSignalNoMoreSplitsForAwaitingReaderInsteadOfRequestingReader() {
        int noMoreSubtask = 45;
        int activeSubtask = 44;
        String noMoreHost = "noMoreHost";
        String activeHost = "activeHost";
        DeltaSourceSplit split = mock(DeltaSourceSplit.class);
        Map<Integer, ReaderInfo> registeredReaders = new LinkedHashMap<>();
        registeredReaders.put(noMoreSubtask, readerInfo);
        registeredReaders.put(activeSubtask, readerInfo);

        enumerator = setUpEnumeratorWithHeadSnapshot();
        when(enumContext.registeredReaders()).thenReturn(registeredReaders);
        enumerator.readersAwaitingSplit.put(noMoreSubtask, noMoreHost);
        when(splitAssigner.getNext(noMoreHost)).thenReturn(Optional.empty());
        when(splitAssigner.getNext(activeHost)).thenReturn(Optional.of(split));

        enumerator.handleSplitRequest(activeSubtask, activeHost);

        verify(enumerator).handleNoMoreSplits(noMoreSubtask);
        verify(enumContext).signalNoMoreSplits(noMoreSubtask);
        verify(enumContext).assignSplit(split, activeSubtask);
        verify(enumerator, never()).handleNoMoreSplits(activeSubtask);
        verify(enumContext, never()).signalNoMoreSplits(activeSubtask);
    }

    @Override
    protected SplitEnumeratorProvider getProvider() {
        return this.provider;
    }
}
