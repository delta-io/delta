package io.delta.flink.internal.table;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.types.AtomicDataType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.Mockito;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.delta.standalone.Operation;
import io.delta.standalone.Operation.Name;
import io.delta.standalone.actions.Metadata;
import io.delta.standalone.types.StringType;
import io.delta.standalone.types.StructField;
import io.delta.standalone.types.StructType;

class DeltaCatalogTableHelperTest {

    @Test
    public void shouldCreateTableOperation() {

        Metadata metadata = Metadata.builder()
            .schema(
                new StructType(new StructField[]{new StructField("col1", new StringType())})
            )
            .partitionColumns(Collections.singletonList("col1"))
            .configuration(Collections.singletonMap("customKey", "myVal"))
            .description("test description").build();

        Operation operation =
            DeltaCatalogTableHelper.prepareDeltaLogOperation(Name.CREATE_TABLE, metadata);

        Map<String, String> expectedOperationParameters = new HashMap<>();
        expectedOperationParameters.put("partitionBy", "\"[\\\"col1\\\"]\"");
        expectedOperationParameters.put("description", "\"test description\"");
        expectedOperationParameters.put("properties", "\"{\\\"customKey\\\":\\\"myVal\\\"}\"");
        expectedOperationParameters.put("isManaged", "false");

        assertThat(operation.getParameters())
            .containsExactlyInAnyOrderEntriesOf(expectedOperationParameters);
    }

    @ParameterizedTest
    @CsvSource(value = {
        "table-path, Filtered DDL options should not contain table-path option.",
        "connector, Filtered DDL options should not contain connector option."
    })
    public void shouldThrow_prepareDeltaTableProperties_filteredOptions(
        String option,
        String validationMessage) {

        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> DeltaCatalogTableHelper.prepareDeltaTableProperties(
                Collections.singletonMap(option, "aValue"),
                new ObjectPath("default", "testTable"),
                Mockito.mock(Metadata.class),
                true // allowOverride == true (value not relevant to the test)
            )
        );

        assertThat(exception.getMessage()).isEqualTo(validationMessage);
    }

    @Test
    public void shouldAlterProperties() {

        Metadata metadata = Metadata.builder()
            .schema(
                new StructType(new StructField[]{new StructField("col1", new StringType())})
            )
            .partitionColumns(Collections.singletonList("col1"))
            .configuration(Collections.singletonMap("customKey", "myVal"))
            .description("test description").build();

        Operation operation =
            DeltaCatalogTableHelper.prepareDeltaLogOperation(Name.SET_TABLE_PROPERTIES, metadata);

        Map<String, String> expectedOperationParameters =
            Collections.singletonMap("properties", "\"{\\\"customKey\\\":\\\"myVal\\\"}\"");

        assertThat(operation.getParameters())
            .containsExactlyInAnyOrderEntriesOf(expectedOperationParameters);
    }

    @Test
    public void shouldThrow_prepareDeltaLogOperation_unsupportedOperationName() {

        Metadata metadata = Metadata.builder()
            .schema(
                new StructType(new StructField[]{new StructField("col1", new StringType())})
            )
            .partitionColumns(Collections.singletonList("col1"))
            .configuration(Collections.singletonMap("customKey", "myVal"))
            .description("test description").build();

        CatalogException catalogException = assertThrows(CatalogException.class, () ->
            DeltaCatalogTableHelper.prepareDeltaLogOperation(Name.DELETE, metadata));

        assertThat(catalogException.getMessage())
            .isEqualTo("Trying to use unsupported Delta Operation [DELETE]");

    }

    @Test
    public void shouldThrow_resolveDeltaSchemaFromDdl_computedColumns() {

        ResolvedSchema schema = ResolvedSchema.of(
            Column.computed("col1", Mockito.mock(ResolvedExpression.class))
        );

        ResolvedCatalogTable table = new ResolvedCatalogTable(
            CatalogTable.of(
                Schema.newBuilder().fromResolvedSchema(schema).build(),
                "mock context",
                Collections.emptyList(),
                Collections.singletonMap("table-path", "file://some/path")),
            schema
        );

        CatalogException exception =
            assertThrows(CatalogException.class,
                () -> DeltaCatalogTableHelper.resolveDeltaSchemaFromDdl(table));

        assertThat(exception.getMessage())
            .isEqualTo(""
                + "Table definition contains unsupported column types. Currently, only physical "
                + "columns are supported by Delta Flink connector.\n"
                + "Invalid columns and types:\n"
                + "col1 -> ComputedColumn"
            );
    }

    @Test
    public void shouldThrow_resolveDeltaSchemaFromDdl_metadataColumns() {

        ResolvedSchema schema = ResolvedSchema.of(
            Column.metadata(
                "col1",
                // isVirtual == true;
                new AtomicDataType(new VarCharType()), "metadataKey", true)
        );

        ResolvedCatalogTable table = new ResolvedCatalogTable(
            CatalogTable.of(
                Schema.newBuilder().fromResolvedSchema(schema).build(),
                "mock context",
                Collections.emptyList(),
                Collections.singletonMap("table-path", "file://some/path")),
            schema
        );

        CatalogException exception =
            assertThrows(CatalogException.class,
                () -> DeltaCatalogTableHelper.resolveDeltaSchemaFromDdl(table));

        assertThat(exception.getMessage())
            .isEqualTo(""
                + "Table definition contains unsupported column types. Currently, only physical "
                + "columns are supported by Delta Flink connector.\n"
                + "Invalid columns and types:\n"
                + "col1 -> MetadataColumn"
            );
    }
}

