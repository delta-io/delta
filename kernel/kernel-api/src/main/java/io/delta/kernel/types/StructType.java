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
import io.delta.kernel.expressions.Column;
import io.delta.kernel.internal.types.DataTypeJsonSerDe;
import io.delta.kernel.internal.util.Tuple2;
import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Struct type which contains one or more columns.
 *
 * @since 3.0.0
 */
@Evolving
public final class StructType extends DataType {

  private final Map<String, Tuple2<StructField, Integer>> nameToFieldAndOrdinal;
  private final List<StructField> fields;
  private final List<String> fieldNames;

  public StructType() {
    this(new ArrayList<>());
  }

  public StructType(List<StructField> fields) {
    // Extract all nested fields and ensure that they do not contain metadata columns
    validateNoMetadataColumns(
        fields.stream()
            .filter(f -> !(f.getDataType() instanceof BasePrimitiveType))
            .collect(Collectors.toList()));

    // Ensure that there are no duplicate metadata columns at the top level
    Set<MetadataColumnSpec> seenMetadataCols = new HashSet<>();
    for (StructField field : fields) {
      if (field.isMetadataColumn()) {
        MetadataColumnSpec colType = field.getMetadataColumnSpec();
        if (seenMetadataCols.contains(colType)) {
          throw new IllegalArgumentException(
              String.format("Duplicate metadata column %s found in struct type", colType));
        }
        seenMetadataCols.add(colType);
      }
    }

    this.fields = fields;
    this.fieldNames = fields.stream().map(f -> f.getName()).collect(Collectors.toList());

    this.nameToFieldAndOrdinal = new HashMap<>();
    for (int i = 0; i < fields.size(); i++) {
      nameToFieldAndOrdinal.put(fields.get(i).getName(), new Tuple2<>(fields.get(i), i));
    }
  }

  public StructType add(StructField field) {
    final List<StructField> fieldsCopy = new ArrayList<>(fields);
    fieldsCopy.add(field);

    return new StructType(fieldsCopy);
  }

  public StructType add(String name, DataType dataType) {
    return add(new StructField(name, dataType, true /* nullable */));
  }

  public StructType add(String name, DataType dataType, boolean nullable) {
    return add(new StructField(name, dataType, nullable));
  }

  public StructType add(String name, DataType dataType, FieldMetadata metadata) {
    return add(new StructField(name, dataType, true /* nullable */, metadata));
  }

  public StructType add(String name, DataType dataType, boolean nullable, FieldMetadata metadata) {
    return add(new StructField(name, dataType, nullable, metadata));
  }

  /** Add a predefined metadata column of {@link MetadataColumnSpec} to the struct type. */
  public StructType addMetadataColumn(String name, MetadataColumnSpec colType) {
    return add(StructField.createMetadataColumn(name, colType));
  }

  /** @return array of fields */
  public List<StructField> fields() {
    return Collections.unmodifiableList(fields);
  }

  /** @return array of field names */
  public List<String> fieldNames() {
    return fieldNames;
  }

  /** @return the number of fields */
  public int length() {
    return fields.size();
  }

  /** @return the index of the field with the given name, or -1 if not found */
  public int indexOf(String fieldName) {
    Tuple2<StructField, Integer> fieldAndOrdinal = nameToFieldAndOrdinal.get(fieldName);
    return fieldAndOrdinal != null ? fieldAndOrdinal._2 : -1;
  }

  /** @return the index of the metadata column of the given spec, or -1 if not found */
  public int indexOf(MetadataColumnSpec spec) {
    // We only allow each metadata column type to appear at most once in the schema and only at top
    // level (i.e., not nested).
    for (int i = 0; i < fields.size(); i++) {
      if (spec.equals(fields.get(i).getMetadataColumnSpec())) {
        return i;
      }
    }
    return -1; // Not found
  }

  /** @return true if the struct type contains a metadata column of the given spec */
  public boolean contains(MetadataColumnSpec spec) {
    return indexOf(spec) >= 0;
  }

  public StructField get(String fieldName) {
    return nameToFieldAndOrdinal.get(fieldName)._1;
  }

  public StructField at(int index) {
    return fields.get(index);
  }

  /**
   * Creates a {@link Column} expression for the field at the given {@code ordinal}
   *
   * @param ordinal the ordinal of the {@link StructField} to create a column for
   * @return a {@link Column} expression for the {@link StructField} with ordinal {@code ordinal}
   */
  public Column column(int ordinal) {
    final StructField field = at(ordinal);
    return new Column(field.getName());
  }

  /**
   * Convert the struct type to Delta protocol specified serialization format.
   *
   * @return serialized in JSON format.
   */
  public String toJson() {
    return DataTypeJsonSerDe.serializeStructType(this);
  }

  @Override
  public boolean equivalent(DataType dataType) {
    if (!(dataType instanceof StructType)) {
      return false;
    }

    StructType otherType = ((StructType) dataType);
    return otherType.length() == length()
        && IntStream.range(0, length())
            .mapToObj(i -> otherType.at(i).getDataType().equivalent(at(i).getDataType()))
            .allMatch(result -> result);
  }

  /**
   * Checks whether the given {@code dataType} is compatible with this type when writing data.
   * Collation differences are ignored.
   *
   * <p>This method is intended to be used during the write path to validate that an input type
   * matches the expected schema before data is written.
   *
   * <p>It should not be used in other cases, such as the read path.
   *
   * @param dataType the input data type being written
   * @return {@code true} if the input type is compatible with this type.
   */
  @Override
  public boolean isWriteCompatible(DataType dataType) {
    if (this == dataType) {
      return true;
    }
    if (dataType == null || getClass() != dataType.getClass()) {
      return false;
    }
    StructType structType = (StructType) dataType;
    return this.length() == structType.length()
        && fieldNames.equals(structType.fieldNames)
        && IntStream.range(0, this.length())
            .mapToObj(
                i -> {
                  StructField thisField = this.at(i);
                  StructField otherField = structType.at(i);
                  return (thisField == null && otherField == null)
                      || (thisField != null && thisField.isWriteCompatible(otherField));
                })
            .allMatch(result -> result);
  }

  @Override
  public boolean isNested() {
    return true;
  }

  @Override
  public boolean existsRecursively(Predicate<DataType> predicate) {
    if (super.existsRecursively(predicate)) {
      return true;
    }
    for (StructField field : fields) {
      if (field.getDataType().existsRecursively(predicate)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public String toString() {
    return String.format(
        "struct(%s)", fields.stream().map(StructField::toString).collect(Collectors.joining(", ")));
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    StructType that = (StructType) o;
    return nameToFieldAndOrdinal.equals(that.nameToFieldAndOrdinal)
        && fields.equals(that.fields)
        && fieldNames.equals(that.fieldNames);
  }

  @Override
  public int hashCode() {
    return Objects.hash(nameToFieldAndOrdinal, fields, fieldNames);
  }

  /**
   * Validates that there are no metadata columns in a list of StructFields.
   *
   * @param fields The list of fields to validate
   * @throws IllegalArgumentException if any nested metadata columns are found
   */
  private void validateNoMetadataColumns(List<StructField> fields) {
    for (StructField field : fields) {
      DataType dataType = field.getDataType();

      if (dataType instanceof StructType) {
        StructType structType = (StructType) dataType;
        // We filter out nested StructTypes since they have already been validated at their creation
        validateNoMetadataColumns(
            structType.fields().stream()
                .filter(f -> !(f.getDataType() instanceof StructType))
                .collect(Collectors.toList()));
      } else if (dataType instanceof MapType) {
        MapType mapType = (MapType) dataType;
        validateNoMetadataColumns(Arrays.asList(mapType.getKeyField(), mapType.getValueField()));
      } else if (dataType instanceof ArrayType) {
        ArrayType arrayType = (ArrayType) dataType;
        validateNoMetadataColumns(Collections.singletonList(arrayType.getElementField()));
      } else if (field.isMetadataColumn()) {
        throw new IllegalArgumentException(
            "Metadata columns are only allowed at the top level of a schema.");
      }
    }
  }
}
