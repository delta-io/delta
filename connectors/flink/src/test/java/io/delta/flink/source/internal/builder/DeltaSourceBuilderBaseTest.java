package io.delta.flink.source.internal.builder;

import java.util.Collection;
import java.util.Collections;
import java.util.function.Supplier;

import io.delta.flink.source.DeltaSource;
import io.delta.flink.source.internal.enumerator.supplier.BoundedSnapshotSupplierFactory;
import io.delta.flink.source.internal.enumerator.supplier.SnapshotSupplierFactory;
import io.delta.flink.source.internal.exceptions.DeltaSourceException;
import io.delta.flink.source.internal.utils.SourceSchema;
import io.delta.flink.utils.DeltaTestUtils;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

import io.delta.standalone.DeltaLog;
import io.delta.standalone.Snapshot;
import io.delta.standalone.actions.Metadata;
import io.delta.standalone.types.IntegerType;
import io.delta.standalone.types.StringType;
import io.delta.standalone.types.StructField;
import io.delta.standalone.types.StructType;

@ExtendWith(MockitoExtension.class)
class DeltaSourceBuilderBaseTest {

    private static final String TABLE_PATH = "s3://some/path";

    private static final long SNAPSHOT_VERSION = 10;

    @Mock
    private DeltaLog deltaLog;

    @Mock
    private Snapshot snapshot;

    @Mock
    private Metadata metadata;

    private TestBuilder builder;

    private MockedStatic<DeltaLog> deltaLogStatic;

    @BeforeEach
    public void setUp() {
        deltaLogStatic = Mockito.mockStatic(DeltaLog.class);
        deltaLogStatic.when(() -> DeltaLog.forTable(any(Configuration.class), anyString()))
            .thenReturn(deltaLog);

        when(deltaLog.snapshot()).thenReturn(snapshot);
        when(snapshot.getVersion()).thenReturn(SNAPSHOT_VERSION);
        when(snapshot.getMetadata()).thenReturn(metadata);

        builder = new TestBuilder(
            new Path(TABLE_PATH),
            DeltaTestUtils.getHadoopConf(),
            new BoundedSnapshotSupplierFactory()
        );
    }

    @AfterEach
    public void after() {
        deltaLogStatic.close();
    }

    /**
     * Delta.io API for {@link Metadata#getSchema()} is annotated as {@code @Nullable}.
     * This test verifies that in case of missing Schema information, source connector will throw
     * appropriate exception when trying to extract table's schema from Delta log.
     */
    @Test
    public void shouldThrowIfNullDeltaSchema() throws Throwable {
        DeltaSourceException exception =
            assertThrows(DeltaSourceException.class, () -> builder.getSourceSchema());

        assertThat(
            exception.getSnapshotVersion().orElseThrow(
                (Supplier<Throwable>) () -> new AssertionError(
                    "Exception is missing snapshot version")),
            equalTo(SNAPSHOT_VERSION));
        assertThat(exception.getTablePath().orElse(null), equalTo(TABLE_PATH));
        assertThat(exception.getCause().getMessage(),
            equalTo(
                "Unable to find Schema information in Delta log for Snapshot version [10]")
        );
    }

    @Test
    public void shouldGetTableSchema() {
        StructField[] deltaFields = new StructField[]{
            new StructField("col1", new StringType()),
            new StructField("col2", new IntegerType())
        };
        StructType deltaSchema = new StructType(deltaFields);

        when(metadata.getSchema()).thenReturn(deltaSchema);

        SourceSchema sourceSchema = builder.getSourceSchema();

        assertThat(sourceSchema.getSnapshotVersion(), equalTo(SNAPSHOT_VERSION));
        assertArrayEquals(new String[]{"col1", "col2"}, sourceSchema.getColumnNames());
        assertArrayEquals(
            new LogicalType[]{new VarCharType(), new IntType()},
            sourceSchema.getColumnTypes()
        );
    }

    @Test
    public void shouldGetTableSchemaForUserColumns() {
        StructField[] deltaFields = new StructField[]{
            new StructField("col1", new StringType()),
            new StructField("col2", new IntegerType())
        };
        StructType deltaSchema = new StructType(deltaFields);

        when(metadata.getSchema()).thenReturn(deltaSchema);

        builder.columnNames(Collections.singletonList("col1"));
        SourceSchema sourceSchema = builder.getSourceSchema();

        assertThat(sourceSchema.getSnapshotVersion(), equalTo(SNAPSHOT_VERSION));
        assertArrayEquals(new String[]{"col1"}, sourceSchema.getColumnNames());
        assertArrayEquals(
            new LogicalType[]{new VarCharType()},
            sourceSchema.getColumnTypes()
        );
    }

    @Test
    public void shouldThrowIfDeltaSchemaMissingUserColumn() {
        StructField[] deltaFields = new StructField[]{
            new StructField("col1", new StringType()),
            new StructField("col2", new IntegerType())
        };
        StructType deltaSchema = new StructType(deltaFields);

        when(metadata.getSchema()).thenReturn(deltaSchema);

        builder.columnNames(Collections.singletonList("nope"));

        assertThrows(DeltaSourceException.class, () -> builder.getSourceSchema());

    }

    private static class TestBuilder extends DeltaSourceBuilderBase<RowData, TestBuilder> {

        TestBuilder(
            Path tablePath,
            Configuration hadoopConfiguration,
            SnapshotSupplierFactory snapshotSupplierFactory) {
            super(tablePath, hadoopConfiguration, snapshotSupplierFactory);
        }

        @Override
        public <V extends DeltaSource<RowData>> V build() {
            return null;
        }

        @Override
        protected Validator validateOptionExclusions() {
            return null;
        }

        @Override
        protected Collection<String> getApplicableOptions() {
            return Collections.emptyList();
        }
    }

}
