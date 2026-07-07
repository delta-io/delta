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

package io.delta.kernel.defaults.internal.json;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.internal.data.DefaultJsonRow;
import io.delta.kernel.types.StructType;

/**
 * Utilities method to serialize and deserialize {@link Row} objects with a limited set of data type
 * values.
 *
 * <p>Following are the supported data types: {@code boolean}, {@code byte}, {@code short}, {@code
 * int}, {@code long}, {@code float}, {@code double}, {@code string}, {@code StructType} (containing
 * any of the supported subtypes), {@code ArrayType}, {@code MapType} (only a map with string keys
 * is supported).
 */
public class JsonUtils {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private JsonUtils() {}

  /**
   * Converts a {@link Row} to a single line JSON string.
   *
   * @param row the row to convert
   * @return JSON string
   */
  public static String rowToJson(Row row) {
    return io.delta.kernel.internal.util.JsonUtils.rowToJson(row);
  }

  /**
   * Converts a JSON string to a {@link Row}.
   *
   * @param json JSON string
   * @param schema to read the JSON according the schema
   * @return {@link Row} instance with given schema.
   */
  public static Row rowFromJson(String json, StructType schema) {
    try {
      final JsonNode jsonNode = OBJECT_MAPPER.readTree(json);
      return new DefaultJsonRow((ObjectNode) jsonNode, schema);
    } catch (JsonProcessingException ex) {
      throw new RuntimeException(String.format("Could not parse JSON: %s", json), ex);
    }
  }
}
