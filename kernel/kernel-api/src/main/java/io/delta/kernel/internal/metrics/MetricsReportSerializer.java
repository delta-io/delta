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

/** Provides Jackson ObjectMapper configuration for serializing {@link MetricsReport} types */
public final class MetricsReportSerializer {

  /**
   * ObjectMapper configured for serializing metrics reports.
   *
   * <p>This ObjectMapper is pre-configured with custom serializers for:
   *
   * <ul>
   *   <li>Java 8 Optional types (serialized as null when empty)
   *   <li>Exceptions (serialized using their toString() representation)
   *   <li>Complex types like StructType and Predicate (using string representation)
   *   <li>Column objects (serialized as arrays of field names)
   * </ul>
   */
  public static final ObjectMapper OBJECT_MAPPER =
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
