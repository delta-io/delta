/*
 * Copyright (2024) The Delta Lake Project Authors.
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

package io.delta.kernel.internal.util;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.delta.kernel.exceptions.KernelException;
import java.io.IOException;
import java.io.StringWriter;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.Map;

public class JsonUtils {

  private JsonUtils() {}

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final JsonFactory FACTORY = new JsonFactory();

  public static JsonFactory factory() {
    return FACTORY;
  }

  public static ObjectMapper mapper() {
    return MAPPER;
  }

  @FunctionalInterface
  public interface ToJson {
    void generate(JsonGenerator generator) throws IOException;
  }

  @FunctionalInterface
  public interface JsonValueWriter<T> {
    void write(JsonGenerator generator, T value) throws IOException;
  }

  /**
   * Utility class for writing JSON with a Jackson {@link JsonGenerator}.
   *
   * @param toJson function that produces JSON using a {@link JsonGenerator}
   * @return a JSON string produced from the generator
   */
  public static String generate(ToJson toJson) {
    try (StringWriter writer = new StringWriter();
        JsonGenerator generator = factory().createGenerator(writer)) {
      toJson.generate(generator);
      generator.flush();
      return writer.toString();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * Parses the given JSON string into a map of key-value pairs.
   *
   * <p>The JSON string should be in the format:
   *
   * <pre>{@code {"key1": "value1", "key2": "value2", ...}}</pre>
   *
   * where both keys and values are strings.
   *
   * @param jsonString The JSON string to parse
   * @return A map containing the key-value pairs extracted from the JSON string
   */
  public static Map<String, String> parseJSONKeyValueMap(String jsonString) {
    if (jsonString == null || jsonString.trim().isEmpty()) {
      return Collections.emptyMap();
    }

    try {
      return MAPPER.readValue(jsonString, new TypeReference<Map<String, String>>() {});
    } catch (Exception e) {
      throw new KernelException(String.format("Failed to parse JSON string: %s", jsonString), e);
    }
  }
}
