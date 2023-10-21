package io.delta.flink.source.internal.enumerator.supplier;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.NoSuchElementException;
import java.util.Optional;

import io.delta.flink.internal.options.DeltaConnectorConfiguration;
import io.delta.flink.source.internal.DeltaSourceOptions;
import io.delta.flink.source.internal.KernelMetadataUtils;
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
import io.delta.kernel.TableNotFoundException;
import io.delta.kernel.client.TableClient;
import io.delta.kernel.internal.SnapshotImpl;

import io.delta.standalone.DeltaLog;
import io.delta.standalone.Snapshot;
import io.delta.standalone.actions.Metadata;
import io.delta.standalone.types.*;

@ExtendWith(MockitoExtension.class)
class ContinuousSourceSnapshotSupplierKernelTest {

    @Mock
    private DeltaLog deltaLog;

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
    public void kernelSnapshotShouldReturnCorrectVersion() {
        DeltaConnectorConfiguration sourceConfig = new DeltaConnectorConfiguration();
	sourceConfig.addOption(DeltaSourceOptions.USE_KERNEL_FOR_SNAPSHOTS, true);
	when(kernelSnapshot.getVersion(null)).thenReturn(10L);
	try {
	    when(table.getLatestSnapshot(tableClient)).thenReturn(kernelSnapshot);
	} catch (TableNotFoundException e) {
	    assertThat("Table not found", false);
	}

        Snapshot snapshot = supplier.getSnapshot(sourceConfig);
	long version = snapshot.getVersion();
	assertThat(version, equalTo(10L));
    }

    @Test
    public void kernelMetadataValidation() {
	DeltaConnectorConfiguration sourceConfig = new DeltaConnectorConfiguration();
	sourceConfig.addOption(DeltaSourceOptions.USE_KERNEL_FOR_SNAPSHOTS, true);
	when(kernelSnapshot.getMetadata()).thenReturn(KernelMetadataUtils.getKernelMetadata());
	try {
	    when(table.getLatestSnapshot(tableClient)).thenReturn(kernelSnapshot);
	} catch (TableNotFoundException e) {
	    assertThat("Table not found", false);
	}

        Snapshot snapshot = supplier.getSnapshot(sourceConfig);
	Metadata metadata = snapshot.getMetadata();

	assertThat(metadata.getId(), equalTo("id"));
	assertThat(metadata.getDescription(), equalTo("description"));
	assertThat(metadata.getFormat(), equalTo(new io.delta.standalone.actions.Format()));
	ArrayList<String> partitionCols = new ArrayList<String>();
	partitionCols.add("Row 0");
	partitionCols.add("Row 1");
	assertThat(metadata.getPartitionColumns(), equalTo(partitionCols));
	HashMap<Integer, Integer> config = new HashMap();
	config.put(new Integer(1), new Integer(2));
	assertThat(metadata.getConfiguration(), equalTo(config));
	assertThat(metadata.getCreatedTime(), equalTo(Optional.of(1234L)));

	// Types below are all standalone types
	FieldMetadata fmeta = FieldMetadata.builder().
	    putString("key1", "value1").
	    putString("key2", "value2").
	    build();
	ArrayList<DataType> expectedTypes = new ArrayList<DataType>();
	ArrayType arrayType = new ArrayType(new IntegerType(), false);
	expectedTypes.add(arrayType);
	expectedTypes.add(new BinaryType());
	expectedTypes.add(new BooleanType());
	expectedTypes.add(new ByteType());
	expectedTypes.add(new DateType());
	expectedTypes.add(DecimalType.USER_DEFAULT);
	expectedTypes.add(new DoubleType());
	expectedTypes.add(new FloatType());
	expectedTypes.add(new IntegerType());
	expectedTypes.add(new LongType());
	expectedTypes.add(new MapType(new ShortType(), arrayType, false));
	expectedTypes.add(new ShortType());
	expectedTypes.add(new StringType());
	expectedTypes.add(new TimestampType());
	StructType expectedSchema = new StructType();
	int fnum = 1;
	for (DataType dt : expectedTypes) {
	    expectedSchema = expectedSchema.add(new StructField(
						    "Field " + fnum,
						    dt,
						    false,
						    fmeta));
	    fnum++;
	}
	assertThat(metadata.getSchema(), equalTo(expectedSchema));
    }
}
