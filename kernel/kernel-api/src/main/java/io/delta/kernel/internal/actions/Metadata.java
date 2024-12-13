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
import static java.util.Objects.requireNonNull;

import io.delta.kernel.annotation.Nullable;
import io.delta.kernel.data.*;
import io.delta.kernel.engine.coordinatedcommits.actions.AbstractMetadata;
import io.delta.kernel.internal.data.GenericRow;
import io.delta.kernel.internal.lang.Lazy;
import io.delta.kernel.internal.types.DataTypeJsonSerDe;
import io.delta.kernel.internal.util.PartitionUtils;
import io.delta.kernel.internal.util.VectorUtils;
import io.delta.kernel.types.*;
import java.util.*;
import java.util.stream.Collectors;

public class Metadata implements AbstractMetadata {

  ///////////////////
  // Static Fields //
  ///////////////////

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

  ////////////////////
  // Static Methods //
  ////////////////////

  public static Metadata fromColumnVector(ColumnVector vector, int rowId) {
    if (vector.isNullAt(rowId)) {
      return null;
    }

    final String schemaJson =
        requireNonNull(vector.getChild(4), rowId, "schemaString").getString(rowId);

    final StructType schema = DataTypeJsonSerDe.deserializeStructType(schemaJson);

    return new Metadata(
        requireNonNull(vector.getChild(0), rowId, "id").getString(rowId),
        Optional.ofNullable(
            vector.getChild(1).isNullAt(rowId) ? null : vector.getChild(1).getString(rowId)),
        Optional.ofNullable(
            vector.getChild(2).isNullAt(rowId) ? null : vector.getChild(2).getString(rowId)),
        Format.fromColumnVector(requireNonNull(vector.getChild(3), rowId, "format"), rowId),
        schemaJson,
        schema,
        vector.getChild(5).getArray(rowId),
        Optional.ofNullable(
            vector.getChild(6).isNullAt(rowId) ? null : vector.getChild(6).getLong(rowId)),
        vector.getChild(7).getMap(rowId));
  }

  /////////////////////////////
  // Member Fields / Methods //
  /////////////////////////////

  // Constructor Params - anything Optional is not required as per the protocol spec. //

  private final String id;
  private final Optional<String> name;
  private final Optional<String> description;
  private final Format format;
  private final String schemaString;
  private final StructType schema;
  private final ArrayValue partitionColumns;
  private final Optional<Long> createdTime;
  private final MapValue configurationMapValue;

  // Private member fields //

  private final Lazy<Map<String, String>> configuration;
  private final Lazy<StructType> dataSchema; // Logical data schema excluding partition columns

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
    this.id = requireNonNull(id, "id is null");
    this.name = requireNonNull(name, "name is null");
    this.description = requireNonNull(description, "description is null");
    this.format = requireNonNull(format, "format is null");
    this.schemaString = requireNonNull(schemaString, "schemaString is null");
    ;
    this.schema = requireNonNull(schema, "schema is null");
    this.partitionColumns = requireNonNull(partitionColumns, "partitionColumns is null");
    this.createdTime = createdTime;
    this.configurationMapValue = requireNonNull(configurationMapValue, "configuration is null");
    this.configuration = new Lazy<>(() -> VectorUtils.toJavaMap(configurationMapValue));
    this.dataSchema = new Lazy<>(this::loadDataSchema);
  }

  ///////////////////////////
  // AbstractMetadata APIs //
  ///////////////////////////

  @Override
  public String getId() {
    return id;
  }

  @Override
  @Nullable
  public String getName() {
    return name.orElse(null);
  }

  @Override
  @Nullable
  public String getDescription() {
    return description.orElse(null);
  }

  @Override
  public String getProvider() {
    return format.getProvider();
  }

  @Override
  public Map<String, String> getFormatOptions() {
    return format.getOptions();
  }

  @Override
  public String getSchemaString() {
    return schemaString;
  }

  @Override
  public List<String> getPartitionColumns() {
    return VectorUtils.toJavaList(partitionColumns);
  }

  @Override
  public Map<String, String> getConfiguration() {
    return Collections.unmodifiableMap(configuration.get());
  }

  public Long getCreatedTime() {
    return createdTime.orElse(null);
  }

  ////////////////////////////////
  // Other Kernel Metadata APIs //
  ////////////////////////////////

  public StructType getSchema() {
    return schema;
  }

  public ArrayValue getPartitionColumnsArrayValue() {
    return partitionColumns;
  }

  /** The logical data schema which excludes partition columns */
  public StructType getDataSchema() {
    return dataSchema.get();
  }

  public Format getFormat() {
    return format;
  }

  public MapValue getConfigurationMapValue() {
    return configurationMapValue;
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

  public Metadata withNewConfiguration(Map<String, String> configuration) {
    Map<String, String> newConfiguration = new HashMap<>(getConfiguration());
    newConfiguration.putAll(configuration);
    return new Metadata(
        this.id,
        this.name,
        this.description,
        this.format,
        this.schemaString,
        this.schema,
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

  ////////////////////
  // Helper methods //
  ////////////////////

  private StructType loadDataSchema() {
    final Set<String> partColNamesLowerCase =
        PartitionUtils.arrayValueToLowerCaseSet(partitionColumns);

    return new StructType(
        schema.fields().stream()
            .filter(
                field -> !partColNamesLowerCase.contains(field.getName().toLowerCase(Locale.ROOT)))
            .collect(Collectors.toList()));
  }
}
