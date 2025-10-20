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
package io.delta.kernel.internal.metrics;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import io.delta.kernel.expressions.Column;
import io.delta.kernel.expressions.Predicate;
import io.delta.kernel.metrics.MetricsReport;
import io.delta.kernel.types.StructType;
import java.io.IOException;

/** Defines JSON serializer for {@link MetricsReport} types */
public final class MetricsReportSerializer {

  /**
   * Serializes a {@link MetricsReport} to a JSON string.
   *
   * <p>This method handles all types of metrics reports, using Jackson's type information to
   * properly serialize the specific report implementation.
   *
   * @param report the metrics report to serialize
   * @return a JSON string representation of the report
   * @throws JsonProcessingException if serialization fails
   */
  public static String serialize(MetricsReport report) throws JsonProcessingException {
    return OBJECT_MAPPER.writeValueAsString(report);
  }

  /////////////////////////////////
  // Private fields and methods //
  ////////////////////////////////

  private static final ObjectMapper OBJECT_MAPPER =
      new ObjectMapper()
          .registerModule(new Jdk8Module()) // To support Optional
          .registerModule( // Serialize Exception using toString()
              new SimpleModule().addSerializer(Exception.class, new ToStringSerializer()))
          .registerModule( // Serialize StructType using toString
              new SimpleModule().addSerializer(StructType.class, new ToStringSerializer()))
          .registerModule( // Serialize Predicate using toString
              new SimpleModule().addSerializer(Predicate.class, new ToStringSerializer()))
          .registerModule( // Serialize Column to exclude un-necessary fields
              new SimpleModule().addSerializer(Column.class, new ColumnSerializer()));

  private static class ColumnSerializer extends JsonSerializer<Column> {
    @Override
    public void serialize(Column value, JsonGenerator gen, SerializerProvider serializers)
        throws IOException {
      gen.writeStartArray();
      for (String name : value.getNames()) {
        gen.writeString(name);
      }
      gen.writeEndArray();
    }
  }

  private MetricsReportSerializer() {}
}
