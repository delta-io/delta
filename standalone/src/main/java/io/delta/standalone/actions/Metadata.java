package io.delta.standalone.actions;

import java.util.*;

import io.delta.standalone.types.StructType;

/**
 * Updates the metadata of the table. The first version of a table must contain
 * a {@code Metadata} action. Subsequent {@code Metadata} actions completely
 * overwrite the current metadata of the table. It is the responsibility of the
 * writer to ensure that any data already present in the table is still valid
 * after any change. There can be at most one {@code Metadata} action in a
 * given version of the table.
 */
public final class Metadata {
    private final String id;
    private final String name;
    private final String description;
    private final Format format;
    private final String schemaString;
    private final List<String> partitionColumns;
    private final Map<String, String> configuration;
    private final Optional<Long> createdTime;
    private final StructType schema;

    public Metadata(String id, String name, String description, Format format, String schemaString,
                    List<String> partitionColumns, Map<String, String> configuration,
                    Optional<Long> createdTime, StructType schema) {
        this.id = id;
        this.name = name;
        this.description = description;
        this.format = format;
        this.schemaString = schemaString;
        this.partitionColumns = partitionColumns;
        this.configuration = configuration;
        this.createdTime = createdTime;
        this.schema = schema;
    }

    /**
     * @return the unique identifier for this table
     */
    public String getId() {
        return id;
    }

    /**
     * @return the user-provided identifier for this table
     */
    public String getName() {
        return name;
    }

    /**
     * @return the user-provided description for this table
     */
    public String getDescription() {
        return description;
    }

    /**
     * @return the {@code Format} for this table
     */
    public Format getFormat() {
        return format;
    }

    /**
     * @return the {@code StructType} schema of the table, as serialized JSON
     * @see StructType StructType
     */
    public String getSchemaString() {
        return schemaString;
    }

    /**
     * @return an unmodifiable {@code java.util.List} containing the names of
     *         columns by which the data should be partitioned
     */
    public List<String> getPartitionColumns() {
        return Collections.unmodifiableList(partitionColumns);
    }

    /**
     * @return an unmodifiable {@code java.util.Map} containing configuration
     *         options for this metadata
     */
    public Map<String, String> getConfiguration() {
        return Collections.unmodifiableMap(configuration);
    }

    /**
     * @return the time when this metadata action was created, in milliseconds
     *         since the Unix epoch
     */
    public Optional<Long> getCreatedTime() {
        return createdTime;
    }

    /**
     * @return the schema of the table as a {@code StructType}
     */
    public StructType getSchema() {
        return schema;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Metadata metadata = (Metadata) o;
        return Objects.equals(id, metadata.id) &&
                Objects.equals(name, metadata.name) &&
                Objects.equals(description, metadata.description) &&
                Objects.equals(format, metadata.format) &&
                Objects.equals(schemaString, metadata.schemaString) &&
                Objects.equals(partitionColumns, metadata.partitionColumns) &&
                Objects.equals(configuration, metadata.configuration) &&
                Objects.equals(createdTime, metadata.createdTime) &&
                Objects.equals(schema, metadata.schema);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, name, description, format, schemaString, partitionColumns,
                configuration, createdTime, schema);
    }
}
