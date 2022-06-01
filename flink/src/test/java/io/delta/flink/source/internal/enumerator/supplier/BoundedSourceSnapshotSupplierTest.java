package io.delta.flink.source.internal.enumerator.supplier;

import java.util.Collections;
import java.util.NoSuchElementException;

import io.delta.flink.source.internal.DeltaSourceConfiguration;
import io.delta.flink.source.internal.DeltaSourceOptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.delta.standalone.DeltaLog;
import io.delta.standalone.Snapshot;

@ExtendWith(MockitoExtension.class)
class BoundedSourceSnapshotSupplierTest {

    @Mock
    private DeltaLog deltaLog;

    @Mock
    private Snapshot deltaSnapshot;

    private BoundedSourceSnapshotSupplier supplier;

    @BeforeEach
    public void setUp() {
        supplier = new BoundedSourceSnapshotSupplier(deltaLog);
    }

    @Test
    public void shouldGetSnapshotFromTableHead() {

        DeltaSourceConfiguration sourceConfig = new DeltaSourceConfiguration();
        when(deltaLog.snapshot()).thenReturn(deltaSnapshot);

        Snapshot snapshot = supplier.getSnapshot(sourceConfig);

        assertThat(snapshot, equalTo(deltaSnapshot));
        verify(deltaLog, never()).getSnapshotForTimestampAsOf(anyLong());
        verify(deltaLog, never()).getSnapshotForVersionAsOf(anyLong());
    }

    @Test
    public void shouldGetSnapshotFromVersionAsOfOption() {

        long version = 10;

        DeltaSourceConfiguration sourceConfig = new DeltaSourceConfiguration(
            Collections.singletonMap(DeltaSourceOptions.VERSION_AS_OF.key(), version)
        );
        when(deltaLog.getSnapshotForVersionAsOf(version)).thenReturn(deltaSnapshot);

        Snapshot snapshot = supplier.getSnapshot(sourceConfig);

        assertThat(snapshot, equalTo(deltaSnapshot));
        verify(deltaLog, never()).getSnapshotForTimestampAsOf(anyLong());
        verify(deltaLog, never()).snapshot();
    }

    @Test
    public void shouldGetSnapshotFromTimestampAsOfOption() {

        String dateTime = "2022-02-24 04:55:00";
        long timestamp = 1645678500000L;

        DeltaSourceConfiguration sourceConfig = new DeltaSourceConfiguration(
            Collections.singletonMap(DeltaSourceOptions.TIMESTAMP_AS_OF.key(), dateTime)
        );
        when(deltaLog.getSnapshotForTimestampAsOf(timestamp)).thenReturn(deltaSnapshot);

        Snapshot snapshot = supplier.getSnapshot(sourceConfig);

        assertThat(snapshot, equalTo(deltaSnapshot));
        verify(deltaLog, never()).getSnapshotForVersionAsOf(anyLong());
        verify(deltaLog, never()).snapshot();
    }

    @Test
    public void shouldThrowIfNoSnapshotFound() {
        assertThrows(
            NoSuchElementException.class,
            () -> supplier.getSnapshot(new DeltaSourceConfiguration())
        );
    }
}
