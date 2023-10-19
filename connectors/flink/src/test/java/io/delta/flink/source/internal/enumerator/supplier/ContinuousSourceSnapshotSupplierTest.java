package io.delta.flink.source.internal.enumerator.supplier;

import java.util.Collections;
import java.util.NoSuchElementException;

import io.delta.flink.internal.options.DeltaConnectorConfiguration;
import io.delta.flink.source.internal.DeltaSourceOptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.delta.kernel.Table;
import io.delta.kernel.client.TableClient;
import io.delta.kernel.internal.SnapshotImpl;

import io.delta.standalone.DeltaLog;
import io.delta.standalone.Snapshot;

@ExtendWith(MockitoExtension.class)
class ContinuousSourceSnapshotSupplierTest {

    @Mock
    private DeltaLog deltaLog;

    @Mock
    private Snapshot deltaSnapshot;

    @Mock
    private TableClient tableClient;

    @Mock
    private Table table;

    @Mock
    private SnapshotImpl kernelSnapshot;

    private ContinuousSourceSnapshotSupplier supplier;

    @BeforeEach
    public void setUp() {
        supplier = new ContinuousSourceSnapshotSupplier(deltaLog, tableClient, table);
    }

    @Test
    public void shouldGetSnapshotFromTableHead() {
	DeltaConnectorConfiguration sourceConfig = new DeltaConnectorConfiguration();
        when(deltaLog.snapshot()).thenReturn(deltaSnapshot);

        Snapshot snapshot = supplier.getSnapshot(sourceConfig);

        assertThat(snapshot, equalTo(deltaSnapshot));
	verify(deltaLog, never()).getSnapshotForTimestampAsOf(anyLong());
        verify(deltaLog, never()).getSnapshotForVersionAsOf(anyLong());
    }

    @Test
    public void shouldGetSnapshotFromTableHeadViaKernel() throws Exception {
        DeltaConnectorConfiguration sourceConfig = new DeltaConnectorConfiguration();
	sourceConfig.addOption(DeltaSourceOptions.USE_KERNEL_FOR_SNAPSHOTS, true);
	when(table.getLatestSnapshot(tableClient)).thenReturn(kernelSnapshot);

        Snapshot snapshot = supplier.getSnapshot(sourceConfig);
	assertThat(snapshot, instanceOf(KernelSnapshotWrapper.class));
	KernelSnapshotWrapper wrapped = (KernelSnapshotWrapper)snapshot;
	assertThat(wrapped.getKernelSnapshot(), equalTo(kernelSnapshot));
    }

    @Test
    public void shouldGetSnapshotFromStartingVersionOption() {

        String version = "10";

        DeltaConnectorConfiguration sourceConfig = new DeltaConnectorConfiguration(
            Collections.singletonMap(DeltaSourceOptions.STARTING_VERSION.key(), version)
        );
        when(deltaLog.getSnapshotForVersionAsOf(Long.parseLong(version))).thenReturn(deltaSnapshot);

        Snapshot snapshot = supplier.getSnapshot(sourceConfig);

        assertThat(snapshot, equalTo(deltaSnapshot));
        verify(deltaLog, never()).getSnapshotForTimestampAsOf(anyLong());
        verify(deltaLog, never()).snapshot();
    }

    @Test
    public void shouldGetSnapshotFromLatestStartingVersionOption() {

        String version = "LaTeSt"; // option processing is case-insensitive.

        DeltaConnectorConfiguration sourceConfig = new DeltaConnectorConfiguration(
            Collections.singletonMap(DeltaSourceOptions.STARTING_VERSION.key(), version)
        );
        when(deltaLog.snapshot()).thenReturn(deltaSnapshot);

        Snapshot snapshot = supplier.getSnapshot(sourceConfig);

        assertThat(snapshot, equalTo(deltaSnapshot));
        verify(deltaLog, never()).getSnapshotForTimestampAsOf(anyLong());
        verify(deltaLog, never()).getSnapshotForVersionAsOf(anyLong());
    }

    @Test
    public void shouldGetSnapshotFromStartingTimestampOption() {

        long dateTime = TimestampFormatConverter.convertToTimestamp("2022-02-24 04:55:00");
        long timestamp = 1645678500000L;

        DeltaConnectorConfiguration sourceConfig = new DeltaConnectorConfiguration(
            Collections.singletonMap(DeltaSourceOptions.STARTING_TIMESTAMP.key(), dateTime)
        );
        long snapshotVersion = deltaSnapshot.getVersion();
        when(deltaLog.getVersionAtOrAfterTimestamp(timestamp)).thenReturn(snapshotVersion);
        when(deltaLog.getSnapshotForVersionAsOf(snapshotVersion)).thenReturn(deltaSnapshot);

        Snapshot snapshot = supplier.getSnapshot(sourceConfig);

        assertThat(snapshot, equalTo(deltaSnapshot));
        verify(deltaLog).getVersionAtOrAfterTimestamp(timestamp);
        verify(deltaLog).getSnapshotForVersionAsOf(snapshotVersion);
        verify(deltaLog, never()).snapshot();
    }

    @Test
    public void shouldThrowIfNoSnapshotFound() {
        assertThrows(
            NoSuchElementException.class,
            () -> supplier.getSnapshot(new DeltaConnectorConfiguration())
        );
    }
}
