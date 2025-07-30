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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

public class MetadataType extends DataType {

  public static final MetadataType ROW_INDEX = new MetadataType(LongType.LONG, "row_index");

  public static final MetadataType ROW_ID = new MetadataType(LongType.LONG, "row_id");

  public static final MetadataType ROW_COMMIT_VERSION =
      new MetadataType(LongType.LONG, "row_commit_version");

  public static final MetadataType FILE_PATH = new MetadataType(StringType.STRING, "file_path");

  public static MetadataType createMetadataType(String metadataTypeName) {
    return Optional.ofNullable(nameToMetadataTypeMap.get().get(metadataTypeName))
        .orElseThrow(
            () -> new IllegalArgumentException("Unknown metadata type: " + metadataTypeName));
  }

  public static boolean isMetadataType(String typeName) {
    return nameToMetadataTypeMap.get().containsKey(typeName);
  }

  private static final Supplier<Map<String, MetadataType>> nameToMetadataTypeMap =
      () ->
          Collections.unmodifiableMap(
              new HashMap<String, MetadataType>() {
                {
                  put("row_index(long)", ROW_INDEX);
                  put("row_id(long)", ROW_ID);
                  put("row_commit_version(long)", ROW_COMMIT_VERSION);
                  put("file_path(string)", FILE_PATH);
                }
              });

  private final DataType valueType;
  private final String valueName;

  protected MetadataType(DataType valueType, String valueName) {
    this.valueType = valueType;
    this.valueName = valueName;
  }

  public DataType getDataType() {
    return valueType;
  }

  @Override
  public boolean equivalent(DataType dataType) {
    return valueType.equals(dataType);
  }

  @Override
  public boolean isNested() {
    return valueType.isNested();
  }

  @Override
  public int hashCode() {
    return valueType.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    MetadataType that = (MetadataType) obj;
    return valueName.equals(that.valueName) && valueType.equals(that.valueType);
  }

  @Override
  public String toString() {
    return String.format("%s(%s)", valueName, valueType.toString());
  }
}
