/*
 * Copyright (2023) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.delta.kernel.internal.actions;

import static io.delta.kernel.internal.util.InternalUtils.requireNonNull;
import static io.delta.kernel.internal.util.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

import io.delta.kernel.data.*;
import io.delta.kernel.internal.data.GenericRow;
import io.delta.kernel.internal.lang.Lazy;
import io.delta.kernel.internal.types.DataTypeJsonSerDe;
import io.delta.kernel.internal.util.ColumnMapping;
import io.delta.kernel.internal.util.VectorUtils;
import io.delta.kernel.types.*;
import java.io.InvalidObjectException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

public class Metadata implements Serializable {
  private static final long serialVersionUID = 1L;

  public static Metadata fromRow(Row row) {
    requireNonNull(row);
    checkArgument(FULL_SCHEMA.equals(row.getSchema()));
    return fromColumnVector(
        VectorUtils.buildColumnVector(Collections.singletonList(row), FULL_SCHEMA), /* rowId */ 0);
  }

  public static Metadata fromColumnVector(ColumnVector vector, int rowId) {
    if (vector.isNullAt(rowId)) {
      return null;
    }

    final String schemaJson =
        requireNonNull(vector.getChild(4), rowId, "schemaString").getString(rowId);
    Lazy<StructType> lazySchema =
        new Lazy<>(() -> DataTypeJsonSerDe.deserializeStructType(schemaJson));

    return new Metadata(
        requireNonNull(vector.getChild(0), rowId, "id").getString(rowId),
        Optional.ofNullable(
            vector.getChild(1).isNullAt(rowId) ? null : vector.getChild(1).getString(rowId)),
        Optional.ofNullable(
            vector.getChild(2).isNullAt(rowId) ? null : vector.getChild(2).getString(rowId)),
        Format.fromColumnVector(requireNonNull(vector.getChild(3), rowId, "format"), rowId),
        schemaJson,
        lazySchema,
        vector.getChild(5).getArray(rowId),
        Optional.ofNullable(
            vector.getChild(6).isNullAt(rowId) ? null : vector.getChild(6).getLong(rowId)),
        vector.getChild(7).getMap(rowId));
  }

  public static final StructType FULL_SCHEMA =
      new StructType()
          .add("id", StringType.STRING, false /* nullable */)
          .add("name", StringType.STRING, true /* nullable */)
          .add("description", StringType.STRING, true /* nullable */)
          .add("format", Format.FULL_SCHEMA, false /* nullable */)
          .add("schemaString", StringType.STRING, false /* nullable */)
          .add(
              "partitionColumns",
              new ArrayType(StringType.STRING, false /* contains null */),
              false /* nullable */)
          .add("createdTime", LongType.LONG, true /* contains null */)
          .add(
              "configuration",
              new MapType(StringType.STRING, StringType.STRING, false),
              false /* nullable */);

  private final String id;
  private final Optional<String> name;
  private final Optional<String> description;
  private final Format format;
  private final String schemaString;
  private final Lazy<StructType> schema;
  private final ArrayValue partitionColumns;
  private final Optional<Long> createdTime;
  private final MapValue configurationMapValue;
  private final Lazy<Map<String, String>> configuration;
  // Partition column names in lower case.
  private final Lazy<Set<String>> partitionColNames;
  // Logical data schema excluding partition columns
  private final Lazy<StructType> dataSchema;

  public Metadata(
      String id,
      Optional<String> name,
      Optional<String> description,
      Format format,
      String schemaString,
      StructType schema,
      ArrayValue partitionColumns,
      Optional<Long> createdTime,
      MapValue configurationMapValue) {
    this(
        id,
        name,
        description,
        format,
        schemaString,
        new Lazy<>(() -> schema),
        partitionColumns,
        createdTime,
        configurationMapValue);
  }

  private Metadata(
      String id,
      Optional<String> name,
      Optional<String> description,
      Format format,
      String schemaString,
      Lazy<StructType> lazySchema,
      ArrayValue partitionColumns,
      Optional<Long> createdTime,
      MapValue configurationMapValue) {
    this.id = requireNonNull(id, "id is null");
    this.name = name;
    this.description = requireNonNull(description, "description is null");
    this.format = requireNonNull(format, "format is null");
    this.schemaString = requireNonNull(schemaString, "schemaString is null");
    this.schema =
        new Lazy<>(
            () -> {
              StructType s = lazySchema.get();
              ensureNoMetadataColumns(s);
              return s;
            });
    this.partitionColumns = requireNonNull(partitionColumns, "partitionColumns is null");
    this.createdTime = createdTime;
    this.configurationMapValue = requireNonNull(configurationMapValue, "configuration is null");
    this.configuration = new Lazy<>(() -> VectorUtils.toJavaMap(configurationMapValue));
    this.partitionColNames = new Lazy<>(this::loadPartitionColNames);
    this.dataSchema =
        new Lazy<>(
            () ->
                new StructType(
                    this.schema.get().fields().stream()
                        .filter(
                            field ->
                                !partitionColNames
                                    .get()
                                    .contains(field.getName().toLowerCase(Locale.ROOT)))
                        .collect(Collectors.toList())));
  }

  /**
   * Returns a new metadata object that has a new configuration which is the combination of its
   * current configuration and {@code configuration}.
   *
   * <p>For overlapping keys the values from {@code configuration} take precedence.
   */
  public Metadata withMergedConfiguration(Map<String, String> configuration) {
    Map<String, String> newConfiguration = new HashMap<>(getConfiguration());
    newConfiguration.putAll(configuration);
    return withReplacedConfiguration(newConfiguration);
  }

  /**
   * Returns a new metadata object that has a new configuration which does not contain any of the
   * keys provided in {@code keysToUnset}.
   */
  public Metadata withConfigurationKeysUnset(Set<String> keysToUnset) {
    Map<String, String> newConfiguration = new HashMap<>(getConfiguration());
    keysToUnset.forEach(newConfiguration::remove);
    return withReplacedConfiguration(newConfiguration);
  }

  /**
   * Returns a new Metadata object with the configuration provided with newConfiguration (any prior
   * configuration is replaced).
   */
  public Metadata withReplacedConfiguration(Map<String, String> newConfiguration) {
    return new Metadata(
        this.id,
        this.name,
        this.description,
        this.format,
        this.schemaString,
        this.schema, // pass Lazy directly to avoid forcing evaluation
        this.partitionColumns,
        this.createdTime,
        VectorUtils.stringStringMapValue(newConfiguration));
  }

  public Metadata withNewSchema(StructType schema) {
    return new Metadata(
        this.id,
        this.name,
        this.description,
        this.format,
        schema.toJson(),
        schema,
        this.partitionColumns,
        this.createdTime,
        this.configurationMapValue);
  }

  @Override
  public String toString() {
    List<String> partitionColumnsStr = VectorUtils.toJavaList(partitionColumns);
    StringBuilder sb = new StringBuilder();
    sb.append("List(");
    for (String partitionColumn : partitionColumnsStr) {
      sb.append(partitionColumn).append(", ");
    }
    if (sb.substring(sb.length() - 2).equals(", ")) {
      sb.setLength(sb.length() - 2); // Remove the last comma and space
    }
    sb.append(")");
    return "Metadata{"
        + "id='"
        + id
        + '\''
        + ", name="
        + name
        + ", description="
        + description
        + ", format="
        + format
        + ", schemaString='"
        + schemaString
        + '\''
        + ", partitionColumns="
        + sb
        + ", createdTime="
        + createdTime
        + ", configuration="
        + configuration.get()
        + '}';
  }

  public String getSchemaString() {
    return schemaString;
  }

  public StructType getSchema() {
    return schema.get();
  }

  public ArrayValue getPartitionColumns() {
    return partitionColumns;
  }

  /** Set of lowercase partition column names */
  public Set<String> getPartitionColNames() {
    return partitionColNames.get();
  }

  /** The logical data schema which excludes partition columns */
  public StructType getDataSchema() {
    return dataSchema.get();
  }

  public String getId() {
    return id;
  }

  public Optional<String> getName() {
    return name;
  }

  public Optional<String> getDescription() {
    return description;
  }

  public Format getFormat() {
    return format;
  }

  public Optional<Long> getCreatedTime() {
    return createdTime;
  }

  public MapValue getConfigurationMapValue() {
    return configurationMapValue;
  }

  public Map<String, String> getConfiguration() {
    return Collections.unmodifiableMap(configuration.get());
  }

  /**
   * The full schema (including partition columns) with the field names converted to their physical
   * names (column names used in the data files) based on the table's column mapping mode. When
   * column mapping mode is ID, fieldId metadata is preserved in the field metadata; all column
   * metadata is otherwise removed.
   */
  public StructType getPhysicalSchema() {
    ColumnMapping.ColumnMappingMode mappingMode =
        ColumnMapping.getColumnMappingMode(getConfiguration());
    return ColumnMapping.convertToPhysicalSchema(getSchema(), getSchema(), mappingMode);
  }

  /**
   * Filter out the key-value pair matches exactly with the old properties.
   *
   * @param newProperties the new properties to be filtered
   * @return the filtered properties
   */
  public Map<String, String> filterOutUnchangedProperties(Map<String, String> newProperties) {
    Map<String, String> oldProperties = getConfiguration();
    return newProperties.entrySet().stream()
        .filter(
            entry ->
                !oldProperties.containsKey(entry.getKey())
                    || !oldProperties.get(entry.getKey()).equals(entry.getValue()))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  /**
   * Encode as a {@link Row} object with the schema {@link Metadata#FULL_SCHEMA}.
   *
   * @return {@link Row} object with the schema {@link Metadata#FULL_SCHEMA}
   */
  public Row toRow() {
    Map<Integer, Object> metadataMap = new HashMap<>();
    metadataMap.put(0, id);
    metadataMap.put(1, name.orElse(null));
    metadataMap.put(2, description.orElse(null));
    metadataMap.put(3, format.toRow());
    metadataMap.put(4, schemaString);
    metadataMap.put(5, partitionColumns);
    metadataMap.put(6, createdTime.orElse(null));
    metadataMap.put(7, configurationMapValue);

    return new GenericRow(Metadata.FULL_SCHEMA, metadataMap);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        id,
        name,
        description,
        format,
        schema.get(),
        partitionColNames.get(),
        createdTime,
        configuration.get());
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof Metadata)) {
      return false;
    }
    Metadata other = (Metadata) o;
    return id.equals(other.id)
        && name.equals(other.name)
        && description.equals(other.description)
        && format.equals(other.format)
        && schema.get().equals(other.schema.get())
        && partitionColNames.get().equals(other.partitionColNames.get())
        && createdTime.equals(other.createdTime)
        && configuration.get().equals(other.configuration.get());
  }

  /** Helper method to load the partition column names. */
  private Set<String> loadPartitionColNames() {
    ColumnVector partitionColNameVector = partitionColumns.getElements();
    Set<String> partitionColumnNames = new HashSet<>();
    for (int i = 0; i < partitionColumns.getSize(); i++) {
      checkArgument(
          !partitionColNameVector.isNullAt(i), "Expected a non-null partition column name");
      String partitionColName = partitionColNameVector.getString(i);
      checkArgument(
          partitionColName != null && !partitionColName.isEmpty(),
          "Expected non-null and non-empty partition column name");
      partitionColumnNames.add(partitionColName.toLowerCase(Locale.ROOT));
    }
    return Collections.unmodifiableSet(partitionColumnNames);
  }

  /** Helper method to ensure that a table schema never contains metadata columns. */
  private void ensureNoMetadataColumns(StructType schema) {
    for (StructField field : schema.fields()) {
      if (field.isMetadataColumn()) {
        throw new IllegalArgumentException(
            "Table schema cannot contain metadata columns: " + field.getName());
      }
    }
  }

  /**
   * Serializable representation of Metadata. Converts complex Kernel types (ArrayValue, MapValue)
   * to simple Java types (List, Map) that are serializable.
   */
  private static class SerializableMetadata implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String id;
    @Nullable private final String name;
    @Nullable private final String description;
    private final String formatProvider;
    private final Map<String, String> formatOptions;
    private final String schemaString;
    private final List<String> partitionColumnsList;
    @Nullable private final Long createdTime;
    private final Map<String, String> configuration;

    SerializableMetadata(Metadata metadata) {
      this.id = metadata.id;
      this.name = metadata.name.orElse(null);
      this.description = metadata.description.orElse(null);
      this.formatProvider = metadata.format.getProvider();
      this.formatOptions = metadata.format.getOptions();
      this.schemaString = metadata.schemaString;
      this.partitionColumnsList = VectorUtils.toJavaList(metadata.partitionColumns);
      this.createdTime = metadata.createdTime.orElse(null);
      this.configuration = VectorUtils.toJavaMap(metadata.configurationMapValue);
    }

    // Reconstruct Metadata from serialized data
    private Object readResolve() {
      Format format = new Format(formatProvider, formatOptions);
      Lazy<StructType> lazySchema =
          new Lazy<>(() -> DataTypeJsonSerDe.deserializeStructType(schemaString));
      ArrayValue partitionColumns =
          VectorUtils.buildArrayValue(partitionColumnsList, StringType.STRING);
      MapValue configurationMapValue = VectorUtils.stringStringMapValue(configuration);

      return new Metadata(
          id,
          Optional.ofNullable(name),
          Optional.ofNullable(description),
          format,
          schemaString,
          lazySchema,
          partitionColumns,
          Optional.ofNullable(createdTime),
          configurationMapValue);
    }
  }

  /**
   * Replace this Metadata with SerializableMetadata during serialization. This is the standard Java
   * serialization proxy pattern for immutable objects with complex fields.
   */
  private Object writeReplace() {
    return new SerializableMetadata(this);
  }

  /** Prevent direct deserialization of Metadata (must use SerializableMetadata). */
  private void readObject(ObjectInputStream stream) throws InvalidObjectException {
    throw new InvalidObjectException("Use SerializableMetadata");
  }
}
