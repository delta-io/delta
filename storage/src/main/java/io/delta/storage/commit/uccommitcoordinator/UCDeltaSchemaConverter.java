/*
 * Copyright (2026) The Delta Lake Project Authors.
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

package io.delta.storage.commit.uccommitcoordinator;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.unitycatalog.client.JSON;
import io.unitycatalog.client.delta.model.ArrayType;
import io.unitycatalog.client.delta.model.DeltaType;
import io.unitycatalog.client.delta.model.MapType;
import io.unitycatalog.client.delta.model.PrimitiveType;
import io.unitycatalog.client.delta.model.StructField;
import io.unitycatalog.client.delta.model.StructType;
import io.unitycatalog.client.delta.serde.DeltaTypeModule;

import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Conversion from the UC SDK's Delta schema model ({@link StructType}, {@link DeltaType},
 * {@link PrimitiveType}) to Delta's JSON schema wire format. The reverse direction
 * ({@link #parseSchemaString}) is reserved for a follow-up and currently throws.
 *
 * <p>The SDK's default {@link ObjectMapper} (from {@link JSON#getDefault()}) does not emit
 * Delta's wire format on its own:
 * <ul>
 *   <li>{@link PrimitiveType} serializes as {@code {"type":"integer"}} by default, but Delta
 *       expects a bare string ({@code "integer"}). {@link DeltaTypeModule} provides custom
 *       serializers/deserializers that flatten primitives (and decimal) to bare strings.</li>
 *   <li>The SDK uses kebab-case JSON keys for nested types ({@code element-type},
 *       {@code contains-null}, {@code key-type}, etc.). Delta's wire format uses camelCase
 *       ({@code elementType}, {@code containsNull}, {@code keyType}, ...). The
 *       {@link CamelCaseArrayMixin} / {@link CamelCaseMapMixin} mixins rename those keys via
 *       Jackson's {@link com.fasterxml.jackson.databind.ObjectMapper#addMixIn} mechanism.</li>
 * </ul>
 * The resulting JSON is parseable by Delta's schema readers (e.g. {@code DataType.fromJson}).
 */
final class UCDeltaSchemaConverter {

  /** Primitive type names that the legacy create-table path ({@link #toUCStructType}) accepts. */
  private static final Set<String> PRIMITIVE_TYPE_NAMES = Set.of(
      "BOOLEAN", "BYTE", "SHORT", "INT", "LONG", "FLOAT", "DOUBLE",
      "DATE", "TIMESTAMP", "TIMESTAMP_NTZ", "STRING", "BINARY", "DECIMAL");

  /**
   * Singleton mapper preconfigured to emit Delta's wire format
   * (bare-string primitives, camelCase keys for nested types). See class-level docs.
   */
  private static final ObjectMapper DELTA_SCHEMA_MAPPER = createDeltaSchemaMapper();

  private UCDeltaSchemaConverter() {}

  /**
   * Serializes the SDK's {@link StructType} to Delta's JSON schema wire format. The resulting
   * string is parseable by Delta's schema readers (e.g. {@code DataType.fromJson}).
   *
   * @return JSON string, or {@code null} if {@code columns} is {@code null}.
   * @throws IllegalStateException if Jackson fails to serialize.
   */
  static String serializeSchema(StructType columns) {
    if (columns == null) {
      return null;
    }
    try {
      return DELTA_SCHEMA_MAPPER.writeValueAsString(columns);
    } catch (JsonProcessingException e) {
      throw new IllegalStateException("Failed to serialize UC schema to Delta JSON", e);
    }
  }

  /**
   * Parses a Delta JSON schema string into the SDK's {@link StructType}. Reserved for the
   * UpdateTable schema-diff path; not yet implemented.
   */
  static StructType parseSchemaString(String schemaString) {
    // TODO: implement full Delta schema string -> StructType conversion
    throw new UnsupportedOperationException(
        "Delta schema string to StructType conversion is not yet implemented.");
  }

  /**
   * Converts the legacy create-table path's {@link UCClient.ColumnDef} list into the UC SDK's
   * {@link StructType}. Only primitive types are supported today; complex types throw
   * {@link UnsupportedOperationException}.
   *
   * @throws NullPointerException if {@code columns} is {@code null}.
   */
  static StructType toUCStructType(List<UCClient.ColumnDef> columns) {
    Objects.requireNonNull(columns, "columns must not be null");
    StructType structType = new StructType();
    for (UCClient.ColumnDef col : columns) {
      structType.addFieldsItem(new StructField()
          .name(col.getName())
          .nullable(col.isNullable())
          .type(toUCDeltaType(col)));
    }
    return structType;
  }

  private static PrimitiveType toUCDeltaType(UCClient.ColumnDef col) {
    if (!PRIMITIVE_TYPE_NAMES.contains(col.getTypeName())) {
      throw new UnsupportedOperationException(
          "Complex column type '" + col.getTypeName() + "' for column '" + col.getName() +
              "' is not yet supported. Only primitive types are supported.");
    }
    return new PrimitiveType().type(col.getTypeText());
  }

  private static ObjectMapper createDeltaSchemaMapper() {
    // Copy the SDK's default mapper so we inherit its base config (visibility, naming, etc.)
    // without mutating the shared instance.
    ObjectMapper m = JSON.getDefault().getMapper().copy();
    // DeltaTypeModule flattens PrimitiveType/DecimalType into bare type-name strings.
    m.registerModule(new DeltaTypeModule());
    // The SDK ships ArrayType/MapType with kebab-case JSON keys; Delta's wire format uses
    // camelCase. Mixins rewrite the property names without modifying the generated SDK
    // classes themselves.
    m.addMixIn(ArrayType.class, CamelCaseArrayMixin.class);
    m.addMixIn(MapType.class, CamelCaseMapMixin.class);
    return m;
  }

  /**
   * Jackson mixin that renames {@link ArrayType}'s JSON keys from kebab-case to camelCase
   * (matching Delta's wire format).
   *
   * <p>The class is {@code abstract} and the methods abstract because Jackson never
   * instantiates the mixin: it only inspects annotated method signatures and projects the
   * annotations onto the target class. Making the class abstract makes that contract
   * explicit and avoids a no-op constructor.
   */
  private abstract static class CamelCaseArrayMixin {
    @JsonProperty("elementType")
    abstract DeltaType getElementType();
    @JsonSetter("elementType")
    abstract void setElementType(DeltaType v);
    @JsonProperty("containsNull")
    abstract Boolean getContainsNull();
    @JsonSetter("containsNull")
    abstract void setContainsNull(Boolean v);
  }

  /**
   * Jackson mixin that renames {@link MapType}'s JSON keys from kebab-case to camelCase
   * (matching Delta's wire format). See {@link CamelCaseArrayMixin} for the mixin pattern.
   */
  private abstract static class CamelCaseMapMixin {
    @JsonProperty("keyType")
    abstract DeltaType getKeyType();
    @JsonSetter("keyType")
    abstract void setKeyType(DeltaType v);
    @JsonProperty("valueType")
    abstract DeltaType getValueType();
    @JsonSetter("valueType")
    abstract void setValueType(DeltaType v);
    @JsonProperty("valueContainsNull")
    abstract Boolean getValueContainsNull();
    @JsonSetter("valueContainsNull")
    abstract void setValueContainsNull(Boolean v);
  }
}
