/*
 * Copyright (2025) The Delta Lake Project Authors.
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

package io.delta.kernel.types;

import static io.delta.kernel.types.MetadataColumnSpec.*;

import io.delta.kernel.annotation.Evolving;
import io.delta.kernel.exceptions.KernelException;
import io.delta.kernel.internal.types.DataTypeJsonSerDe;
import io.delta.kernel.internal.util.SchemaIterable;
import java.util.*;

/**
 * Represents a subfield of {@link StructType} with additional properties and metadata.
 *
 * @since 3.0.0
 */
@Evolving
public class StructField {
  ////////////////////////////////////////////////////////////////////////////////
  // Static Fields / Methods
  ////////////////////////////////////////////////////////////////////////////////

  /**
   * The existence of this key indicates that a column is a metadata column and its values indicates
   * what kind of {@link MetadataColumnSpec} it is.
   */
  public static final String METADATA_SPEC_KEY = "delta.metadataSpec";

  /**
   * Indicates that a column was requested for internal computations and should not be returned to
   * the user.
   */
  public static final String IS_INTERNAL_COLUMN_KEY = "delta.isInternalColumn";

  /** The name of the default row index metadata column. */
  private static final String DEFAULT_ROW_INDEX_COLUMN_NAME = "_metadata.row_index";

  /**
   * The default row index metadata column used by Kernel. When present, this column must be
   * populated with row index of each row when reading from Parquet.
   */
  public static StructField DEFAULT_ROW_INDEX_COLUMN =
      createMetadataColumn(DEFAULT_ROW_INDEX_COLUMN_NAME, MetadataColumnSpec.ROW_INDEX);

  public static final String COLLATIONS_METADATA_KEY = "__COLLATIONS";
  public static final String FROM_TYPE_KEY = "fromType";
  public static final String TO_TYPE_KEY = "toType";
  public static final String FIELD_PATH_KEY = "fieldPath";
  public static final String DELTA_TYPE_CHANGES_KEY = "delta.typeChanges";

  /**
   * Creates a metadata column of the given {@code colSpec} with the given {@code name}.
   *
   * @param name Name of the metadata column
   * @param colSpec Type of the metadata column
   * @return A StructField representing the metadata column
   */
  public static StructField createMetadataColumn(String name, MetadataColumnSpec colSpec) {
    switch (colSpec) {
      case ROW_INDEX:
        return new StructField(
            name,
            colSpec.dataType,
            colSpec.nullable,
            new FieldMetadata.Builder()
                .putMetadataColumnSpec(METADATA_SPEC_KEY, ROW_INDEX)
                .build());
      case ROW_ID:
        return new StructField(
            name,
            colSpec.dataType,
            colSpec.nullable,
            new FieldMetadata.Builder().putMetadataColumnSpec(METADATA_SPEC_KEY, ROW_ID).build());
      case ROW_COMMIT_VERSION:
        return new StructField(
            name,
            colSpec.dataType,
            colSpec.nullable,
            new FieldMetadata.Builder()
                .putMetadataColumnSpec(METADATA_SPEC_KEY, ROW_COMMIT_VERSION)
                .build());
      default:
        throw new IllegalArgumentException("Unknown MetadataColumnType: " + colSpec);
    }
  }

  ////////////////////////////////////////////////////////////////////////////////
  // Instance Fields / Methods
  ////////////////////////////////////////////////////////////////////////////////

  private final String name;
  private final DataType dataType;
  private final boolean nullable;
  private final FieldMetadata metadata;
  private final List<TypeChange> typeChanges;

  public StructField(String name, DataType dataType, boolean nullable) {
    this(name, dataType, nullable, FieldMetadata.empty());
  }

  public StructField(String name, DataType dataType, boolean nullable, FieldMetadata metadata) {
    this(name, dataType, nullable, metadata, Collections.emptyList());
  }

  /*
   * N.B. Type changes should be entirely managed by the Delta Kernel, users are not expected to
   * maintain this field, and therefore should not be using this constructor.
   */
  StructField(
      String name,
      DataType dataType,
      boolean nullable,
      FieldMetadata metadata,
      List<TypeChange> typeChanges) {
    this.name = name;
    this.dataType = dataType;
    this.nullable = nullable;
    this.typeChanges = typeChanges == null ? Collections.emptyList() : typeChanges;

    FieldMetadata nestedMetadata = collectNestedMapArrayTypeMetadata();
    this.metadata =
        new FieldMetadata.Builder().fromMetadata(metadata).fromMetadata(nestedMetadata).build();
    if (!this.typeChanges.isEmpty()
        && (dataType instanceof MapType
            || dataType instanceof StructType
            || dataType instanceof ArrayType)) {
      throw new KernelException("Type changes are not supported on nested types.");
    }
  }

  /** @return the name of this field */
  public String getName() {
    return name;
  }

  /** @return the data type of this field */
  public DataType getDataType() {
    return dataType;
  }

  /** @return the metadata for this field */
  public FieldMetadata getMetadata() {
    return metadata;
  }

  /** @return whether this field allows to have a {@code null} value. */
  public boolean isNullable() {
    return nullable;
  }

  /**
   * Returns the list of type changes for this field. A field can go through multiple type changes
   * (e.g. {@code int->long->decimal}). Changes are ordered from least recent to most recent in the
   * list (index 0 is the oldest change).
   *
   * <p>N.B. Type changes should be entirely managed by the Delta Kernel, users are not expected to
   * maintain this field.
   */
  public List<TypeChange> getTypeChanges() {
    return Collections.unmodifiableList(typeChanges);
  }

  public boolean isMetadataColumn() {
    return metadata != null && metadata.contains(METADATA_SPEC_KEY);
  }

  public boolean isDataColumn() {
    return !isMetadataColumn();
  }

  /** Returns the type of metadata column if this is a metadata column, otherwise returns null. */
  public MetadataColumnSpec getMetadataColumnSpec() {
    return metadata.getMetadataColumnSpec(METADATA_SPEC_KEY);
  }

  public boolean isInternalColumn() {
    return Optional.ofNullable(metadata.getBoolean(IS_INTERNAL_COLUMN_KEY)).orElse(false);
  }

  @Override
  public String toString() {
    return String.format(
        "StructField(name=%s,type=%s,nullable=%s,metadata=%s,typeChanges=%s)",
        name, dataType, nullable, metadata, typeChanges);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    StructField that = (StructField) o;
    return nullable == that.nullable
        && name.equals(that.name)
        && dataType.equals(that.dataType)
        && metadata.equals(that.metadata)
        && Objects.equals(typeChanges, that.typeChanges);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, dataType, nullable, metadata, typeChanges);
  }

  public StructField withNewMetadata(FieldMetadata metadata) {
    return new StructField(name, dataType, nullable, metadata, typeChanges);
  }

  /**
   * Creates a copy of this StructField with the specified type changes.
   *
   * <p>N.B. Type changes should be entirely managed by the Delta Kernel, users are not expected to
   * maintain this field.
   *
   * @param typeChanges The list of type changes to set
   * @return A new StructField with the same properties but with the specified type changes
   */
  public StructField withTypeChanges(List<TypeChange> typeChanges) {
    return new StructField(name, dataType, nullable, metadata, typeChanges);
  }

  /**
   * Creates a copy of this StructField with the specified data type.
   *
   * <p>TypeChanges are NOT updated as part of this call.
   *
   * @param newType The new type to use in the StructField.
   * @return A new StructField with the same properties but with the specified data type.
   */
  public StructField withDataType(DataType newType) {
    return new StructField(name, newType, nullable, metadata, typeChanges);
  }

  /** Fetches collation and type changes metadata from nested fields. */
  private FieldMetadata collectNestedMapArrayTypeMetadata() {
    FieldMetadata.Builder collationBuilder = FieldMetadata.builder();
    List<FieldMetadata> typeChangesBuilder = new ArrayList<>();
    // This is a little risky since this isn't fully initialized but should be fine since all fields
    // we needed are initialized.
    // StructTypes children would already have their own collation metadata, so skip them here.
    SchemaIterable iterable =
        SchemaIterable.newSchemaIterableWithIgnoredRecursion(
            new StructType().add(this), new Class[] {StructType.class});
    for (SchemaIterable.SchemaElement element : iterable) {
      DataType type = element.getField().getDataType();
      if (type instanceof StringType) {
        StringType stringType = (StringType) type;
        if (!stringType
            .getCollationIdentifier()
            .equals(CollationIdentifier.fromString("SPARK.UTF8_BINARY"))) {
          // TODO: Should this account for column mapping?
          String path =
              element.getPathFromNearestStructFieldAncestor(
                  element.getNearestStructFieldAncestor().name);
          collationBuilder.putString(
              path, stringType.getCollationIdentifier().toStringWithoutVersion());
        }
      }
      StructField field = element.getField();
      if (!field.getTypeChanges().isEmpty()) {
        for (TypeChange typeChange : field.getTypeChanges()) {
          FieldMetadata.Builder typeChangeBuilder = FieldMetadata.builder();
          typeChangeBuilder.putString(FROM_TYPE_KEY, typeAsString(typeChange.getFrom()));
          typeChangeBuilder.putString(TO_TYPE_KEY, typeAsString(typeChange.getTo()));
          if (!element.isStructField()) {
            // For type changes the field name the field name is not a prefix.
            typeChangeBuilder.putString(
                FIELD_PATH_KEY, element.getPathFromNearestStructFieldAncestor(""));
          }
          typeChangesBuilder.add(typeChangeBuilder.build());
        }
      }
    }

    FieldMetadata.Builder finalBuilder = FieldMetadata.builder();

    FieldMetadata collationMetadata = collationBuilder.build();
    if (!collationMetadata.getEntries().isEmpty()) {
      finalBuilder.putFieldMetadata(COLLATIONS_METADATA_KEY, collationMetadata);
    }
    if (!typeChangesBuilder.isEmpty()) {
      finalBuilder.putFieldMetadataArray(
          DELTA_TYPE_CHANGES_KEY, typeChangesBuilder.toArray(new FieldMetadata[0]));
    }
    return finalBuilder.build();
  }

  private static String typeAsString(DataType dt) {
    String jsonString = DataTypeJsonSerDe.serializeDataType(dt);
    // Remove leading/trailing quotes.
    return jsonString.substring(1, jsonString.length() - 1);
  }
}
