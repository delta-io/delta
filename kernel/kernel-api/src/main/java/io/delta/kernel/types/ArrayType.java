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
import java.util.Objects;

/**
 * Represent {@code array} data type
 *
 * @since 3.0.0
 */
@Evolving
public class ArrayType extends DataType {
  private final StructField elementField;

  public ArrayType(DataType elementType, boolean containsNull) {
    this.elementField = new StructField("element", elementType, containsNull);
  }

  public ArrayType(StructField elementField) {
    this.elementField = elementField;
  }

  public StructField getElementField() {
    return elementField;
  }

  public DataType getElementType() {
    return elementField.getDataType();
  }

  public boolean containsNull() {
    return elementField.isNullable();
  }

  @Override
  public boolean equivalent(DataType dataType) {
    return dataType instanceof ArrayType
        && ((ArrayType) dataType).getElementType().equivalent(getElementType());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ArrayType arrayType = (ArrayType) o;
    return Objects.equals(elementField, arrayType.elementField);
  }

  @Override
  public boolean equalsIgnoringNames(DataType dataType) {
    if (!(dataType instanceof ArrayType)) {
      return false;
    }
    ArrayType arrayType = (ArrayType) dataType;
    return arrayType.containsNull() == containsNull()
        && arrayType.getElementType().equalsIgnoringNames(getElementType());
  }

  @Override
  public int hashCode() {
    return Objects.hash(elementField);
  }

  @Override
  public String toString() {
    return "array[" + getElementType() + "]";
  }
}
