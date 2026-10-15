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
package io.delta.spark.internal.v2.utils;

import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.internal.json.JsonUtils;
import io.delta.kernel.internal.types.DataTypeJsonSerDe;
import io.delta.kernel.types.StructType;
import java.io.Serializable;

public class SerializableKernelRowWrapper implements Serializable {

  private final String rowJson;
  private final String schemaJson;
  private transient Row row;

  public SerializableKernelRowWrapper(Row row) {
    this.rowJson = JsonUtils.rowToJson(row);
    this.schemaJson = DataTypeJsonSerDe.serializeDataType(row.getSchema());
    this.row = row;
  }

  public Row getRow() {
    if (row == null) {
      StructType schema = DataTypeJsonSerDe.deserializeStructType(schemaJson);
      row = JsonUtils.rowFromJson(rowJson, schema);
    }
    return row;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    SerializableKernelRowWrapper that = (SerializableKernelRowWrapper) o;
    return rowJson.equals(that.rowJson) && schemaJson.equals(that.schemaJson);
  }

  @Override
  public int hashCode() {
    int result = rowJson.hashCode();
    result = 31 * result + schemaJson.hashCode();
    return result;
  }
}
