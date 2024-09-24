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

package io.delta.kernel.types;

import io.delta.kernel.annotation.Evolving;
import io.delta.kernel.expressions.CollationIdentifier;
import io.delta.kernel.internal.util.Tuple2;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

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

  /** Indicates a metadata column when present in the field metadata and the value is true */
  private static String IS_METADATA_COLUMN_KEY = "isMetadataColumn";

  /**
   * The name of a row index metadata column. When present this column must be populated with row
   * index of each row when reading from parquet.
   */
  public static String METADATA_ROW_INDEX_COLUMN_NAME = "_metadata.row_index";

  public static StructField METADATA_ROW_INDEX_COLUMN =
      new StructField(
          METADATA_ROW_INDEX_COLUMN_NAME,
          LongType.LONG,
          false,
          FieldMetadata.builder().putBoolean(IS_METADATA_COLUMN_KEY, true).build());

  ////////////////////////////////////////////////////////////////////////////////
  // Instance Fields / Methods
  ////////////////////////////////////////////////////////////////////////////////

  private final String name;
  private final DataType dataType;
  private final boolean nullable;
  private final FieldMetadata metadata;

  public StructField(String name, DataType dataType, boolean nullable) {
    this(name, dataType, nullable, FieldMetadata.empty());
  }

  public StructField(String name, DataType dataType, boolean nullable, FieldMetadata metadata) {
    this.name = name;
    this.dataType = dataType;
    this.nullable = nullable;
    this.metadata = metadata;
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

  public boolean isMetadataColumn() {
    return metadata.contains(IS_METADATA_COLUMN_KEY)
        && (boolean) metadata.get(IS_METADATA_COLUMN_KEY);
  }

  public boolean isDataColumn() {
    return !isMetadataColumn();
  }

  @Override
  public String toString() {
    return String.format(
        "StructField(name=%s,type=%s,nullable=%s,metadata=%s)", name, dataType, nullable, metadata);
  }

  public FieldMetadata getSerializationMetadata() {
    List<Tuple2<String, String>> nestedCollatedFields = getNestedCollatedFields(dataType, name);
    if (nestedCollatedFields.isEmpty()) {
      return metadata;
    }

    FieldMetadata.Builder metadataBuilder = new FieldMetadata.Builder();
    for (Tuple2<String, String> nestedField : nestedCollatedFields) {
      metadataBuilder.putString(nestedField._1, nestedField._2);
    }
    return new FieldMetadata.Builder()
        .fromMetadata(metadata)
        .putFieldMetadata(DataType.COLLATIONS_METADATA_KEY, metadataBuilder.build())
        .build();
  }

  private List<Tuple2<String, String>> getNestedCollatedFields(DataType parent, String path) {
    List<Tuple2<String, String>> nestedCollatedFields = new ArrayList<>();
    if (parent instanceof StringType) {
      StringType stringType = (StringType) parent;
      if (!stringType
          .getCollationIdentifier()
          .equals(CollationIdentifier.DEFAULT_COLLATION_IDENTIFIER)) {
        nestedCollatedFields.add(
            new Tuple2<>(
                path, ((StringType) parent).getCollationIdentifier().toStringWithoutVersion()));
      }
    } else if (parent instanceof MapType) {
      nestedCollatedFields.addAll(
          getNestedCollatedFields(((MapType) parent).getKeyType(), path + ".key"));
      nestedCollatedFields.addAll(
          getNestedCollatedFields(((MapType) parent).getValueType(), path + ".value"));
    } else if (parent instanceof ArrayType) {
      nestedCollatedFields.addAll(
          getNestedCollatedFields(((ArrayType) parent).getElementType(), path + ".element"));
    }
    return nestedCollatedFields;
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
        && metadata.equals(that.metadata);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, dataType, nullable, metadata);
  }

  public StructField withNewMetadata(FieldMetadata metadata) {
    return new StructField(name, dataType, nullable, metadata);
  }
}
