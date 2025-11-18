package io.delta.flink.internal.table.exceptions;

import java.util.List;

import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.CatalogException;

import io.delta.standalone.actions.Metadata;
import io.delta.standalone.types.StructType;

/**
 * Exception thrown when the schema or partition specification defined in DDL
 * does not match the schema stored in the Delta table's _delta_log.
 *
 * <p>This typically occurs when:
 * <ul>
 *   <li>CREATE TABLE DDL defines a different schema than what exists in Delta</li>
 *   <li>Partition columns in DDL don't match Delta table's partition columns</li>
 *   <li>Column types differ between DDL and Delta table</li>
 * </ul>
 */
public class DeltaSchemaMismatchException extends CatalogException {

    private static final long serialVersionUID = 1L;

    private final ObjectPath catalogTablePath;
    private final String deltaTablePath;
    private final Metadata deltaMetadata;
    private final StructType ddlDeltaSchema;
    private final List<String> ddlPartitions;

    public DeltaSchemaMismatchException(
            ObjectPath catalogTablePath,
            String deltaTablePath,
            Metadata deltaMetadata,
            StructType ddlDeltaSchema,
            List<String> ddlPartitions) {

        super(buildMessage(catalogTablePath, deltaTablePath, deltaMetadata,
            ddlDeltaSchema, ddlPartitions));

        this.catalogTablePath = catalogTablePath;
        this.deltaTablePath = deltaTablePath;
        this.deltaMetadata = deltaMetadata;
        this.ddlDeltaSchema = ddlDeltaSchema;
        this.ddlPartitions = ddlPartitions;
    }

    private static String buildMessage(
            ObjectPath catalogTablePath,
            String deltaTablePath,
            Metadata deltaMetadata,
            StructType ddlDeltaSchema,
            List<String> ddlPartitions) {

        String deltaSchemaString = (deltaMetadata.getSchema() == null)
            ? "null"
            : deltaMetadata.getSchema().getTreeString();

        return String.format(
            "Delta table [%s] from filesystem path [%s] has different schema or partition "
                + "spec than one defined in CREATE TABLE DDL.\n"
                + "DDL schema:\n[%s],\nDelta table schema:\n[%s]\n"
                + "DDL partition spec:\n[%s],\nDelta Log partition spec\n[%s]\n",
            catalogTablePath,
            deltaTablePath,
            ddlDeltaSchema.getTreeString(),
            deltaSchemaString,
            ddlPartitions,
            deltaMetadata.getPartitionColumns());
    }

    public ObjectPath getCatalogTablePath() {
        return catalogTablePath;
    }

    public String getDeltaTablePath() {
        return deltaTablePath;
    }

    public Metadata getDeltaMetadata() {
        return deltaMetadata;
    }

    public StructType getDdlDeltaSchema() {
        return ddlDeltaSchema;
    }

    public List<String> getDdlPartitions() {
        return ddlPartitions;
    }
}

