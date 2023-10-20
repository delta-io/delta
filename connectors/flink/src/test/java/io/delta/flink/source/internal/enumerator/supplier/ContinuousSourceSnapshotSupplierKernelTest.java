package io.delta.flink.source.internal.enumerator.supplier;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.NoSuchElementException;
import java.util.Optional;

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
import io.delta.kernel.TableNotFoundException;
import io.delta.kernel.client.TableClient;
import io.delta.kernel.data.ArrayValue;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.MapValue;
import io.delta.kernel.defaults.internal.data.vector.DefaultGenericVector;
import io.delta.kernel.internal.SnapshotImpl;
import io.delta.kernel.types.*;

import io.delta.standalone.DeltaLog;
import io.delta.standalone.Snapshot;
import io.delta.standalone.actions.Metadata;

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

    private io.delta.kernel.internal.actions.Metadata getKernelMetadata() {
	HashMap<String, String> metadata = new HashMap<>();
        metadata.put("key1", "value1");
        metadata.put("key2", "value2");
	ArrayList<DataType> typesToTest = new ArrayList<DataType>();
	ArrayType arrayType = new ArrayType(IntegerType.INTEGER, false);
	typesToTest.add(arrayType);
	typesToTest.add(BinaryType.BINARY);
	typesToTest.add(BooleanType.BOOLEAN);
	typesToTest.add(ByteType.BYTE);
	typesToTest.add(DateType.DATE);
	typesToTest.add(DecimalType.USER_DEFAULT);
	typesToTest.add(DoubleType.DOUBLE);
	typesToTest.add(FloatType.FLOAT);
	typesToTest.add(IntegerType.INTEGER);
	typesToTest.add(LongType.LONG);
	typesToTest.add(new MapType(ShortType.SHORT, arrayType, false));
	typesToTest.add(ShortType.SHORT);
	typesToTest.add(StringType.STRING);
	typesToTest.add(TimestampType.TIMESTAMP);
	ArrayList<StructField> fields = new ArrayList<StructField>();
	int fnum = 1;
	for (DataType dt : typesToTest) {
	    fields.add(
		new StructField(
		    "Field " + fnum,
		    dt,
		    false,
		    metadata
		    )
		);
	    fnum++;
	}
	StructType schema = new StructType(fields);
        return new io.delta.kernel.internal.actions.Metadata(
            "id",
            Optional.ofNullable("name"),
            Optional.ofNullable("description"),
            new io.delta.kernel.internal.actions.Format("parquet"),
            "schemaString",
            schema,
	    new ArrayValue() { // paritionColumns
		@Override
		public int getSize() {
		    return 2;
		}

		@Override
		public ColumnVector getElements() {
		    return new ColumnVector() {
			@Override
			public DataType getDataType() {
			    return null;
			}

			@Override
			public int getSize() {
			    return 2;
			}

			@Override
			public void close() {}
			
			@Override
			public boolean isNullAt(int rowId) {
			    return false;
			}

			@Override
			public String getString(int rowId) {
			    return "Row " + rowId;
			}
		    };
		}
	    },
            Optional.ofNullable(1234L),
	    new MapValue() { // conf
		@Override
		public int getSize() {
		    return 1;
		}
		@Override
		public ColumnVector getKeys() {
		    return new DefaultGenericVector(IntegerType.INTEGER, new Integer[]{new Integer(1)});
		}
		@Override
		public ColumnVector getValues() {
		    return new DefaultGenericVector(IntegerType.INTEGER, new Integer[]{new Integer(2)});
		}
	    }
        );
    }
    
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
	when(kernelSnapshot.getMetadata()).thenReturn(getKernelMetadata());
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

	io.delta.standalone.types.FieldMetadata fmeta = io.delta.standalone.types.FieldMetadata.builder().
	    putString("key1", "value1").
	    putString("key2", "value2").
	    build();
	ArrayList<io.delta.standalone.types.DataType> expectedTypes =
	    new ArrayList<io.delta.standalone.types.DataType>();
	io.delta.standalone.types.ArrayType arrayType = new io.delta.standalone.types.ArrayType(
	    new io.delta.standalone.types.IntegerType(), false
	);
	expectedTypes.add(arrayType);
	expectedTypes.add(new io.delta.standalone.types.BinaryType());
	expectedTypes.add(new io.delta.standalone.types.BooleanType());
	expectedTypes.add(new io.delta.standalone.types.ByteType());
	expectedTypes.add(new io.delta.standalone.types.DateType());
	expectedTypes.add(io.delta.standalone.types.DecimalType.USER_DEFAULT);
	expectedTypes.add(new io.delta.standalone.types.DoubleType());
	expectedTypes.add(new io.delta.standalone.types.FloatType());
	expectedTypes.add(new io.delta.standalone.types.IntegerType());
	expectedTypes.add(new io.delta.standalone.types.LongType());
	expectedTypes.add(
	    new io.delta.standalone.types.MapType(
		new io.delta.standalone.types.ShortType(),
		arrayType,
		false
	    )
	);
	expectedTypes.add(new io.delta.standalone.types.ShortType());
	expectedTypes.add(new io.delta.standalone.types.StringType());
	expectedTypes.add(new io.delta.standalone.types.TimestampType());
	io.delta.standalone.types.StructType expectedSchema = new io.delta.standalone.types.StructType();
	int fnum = 1;
	for (io.delta.standalone.types.DataType dt : expectedTypes) {
	    expectedSchema = expectedSchema.add(new io.delta.standalone.types.StructField(
						    "Field " + fnum,
						    dt,
						    false,
						    fmeta));
	    fnum++;
	}
	assertThat(metadata.getSchema(), equalTo(expectedSchema));
    }
}
