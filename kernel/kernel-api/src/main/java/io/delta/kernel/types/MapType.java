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

import static io.delta.kernel.internal.util.Preconditions.checkArgument;

import io.delta.kernel.annotation.Evolving;
import java.util.Objects;
import java.util.function.Predicate;

/**
 * Data type representing a {@code map} type.
 *
 * @since 3.0.0
 */
@Evolving
public class MapType extends DataType {

  private final StructField keyField;
  private final StructField valueField;

  public static final String MAP_KEY_NAME = "key";
  public static final String MAP_VALUE_NAME = "value";

  public MapType(DataType keyType, DataType valueType, boolean valueContainsNull) {
    validateKeyType(keyType);
    this.keyField = new StructField(MAP_KEY_NAME, keyType, false);
    this.valueField = new StructField(MAP_VALUE_NAME, valueType, valueContainsNull);
  }

  public MapType(StructField keyField, StructField valueField) {
    validateKeyType(keyField.getDataType());
    this.keyField = keyField;
    this.valueField = valueField;
  }

  public StructField getKeyField() {
    return keyField;
  }

  public StructField getValueField() {
    return valueField;
  }

  public DataType getKeyType() {
    return getKeyField().getDataType();
  }

  public DataType getValueType() {
    return getValueField().getDataType();
  }

  public boolean isValueContainsNull() {
    return valueField.isNullable();
  }

  @Override
  public boolean equivalent(DataType dataType) {
    return dataType instanceof MapType
        && ((MapType) dataType).getKeyType().equivalent(getKeyType())
        && ((MapType) dataType).getValueType().equivalent(getValueType())
        && ((MapType) dataType).isValueContainsNull() == isValueContainsNull();
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
    MapType mapType = (MapType) dataType;
    return ((keyField == null && mapType.keyField == null)
            || (keyField != null && keyField.isWriteCompatible(mapType.keyField)))
        && ((valueField == null && mapType.valueField == null)
            || (valueField != null && valueField.isWriteCompatible(mapType.valueField)));
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    MapType mapType = (MapType) o;
    return Objects.equals(keyField, mapType.keyField)
        && Objects.equals(valueField, mapType.valueField);
  }

  @Override
  public boolean isNested() {
    return true;
  }

  @Override
  public boolean existsRecursively(Predicate<DataType> predicate) {
    return super.existsRecursively(predicate)
        || getKeyType().existsRecursively(predicate)
        || getValueType().existsRecursively(predicate);
  }

  @Override
  public int hashCode() {
    return Objects.hash(keyField, valueField);
  }

  @Override
  public String toString() {
    return String.format("map[%s, %s]", getKeyType(), getValueType());
  }

  /**
   * Asserts whether the given {@code keyType} is valid for a map's key type. Disallows {@code
   * StringType} with non-SPARK.UTF8_BINARY collation anywhere within the key type, including when
   * nested inside complex types.
   */
  private void validateKeyType(DataType keyType) {
    checkArgument(
        !keyType.existsRecursively(
            dataType -> {
              if (dataType instanceof StringType) {
                StringType stringType = (StringType) dataType;
                return !stringType.getCollationIdentifier().isSparkUTF8BinaryCollation();
              }
              return false;
            }),
        "Map key type cannot contain StringType with non-SPARK.UTF8_BINARY collation");
  }
}
