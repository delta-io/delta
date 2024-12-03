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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import io.delta.kernel.metrics.SnapshotMetricsResult;
import io.delta.kernel.metrics.SnapshotReport;
import java.io.IOException;
import java.util.Optional;

/** Defines JSON serializers for {@link MetricsReport} types */
public final class MetricsReportSerializers {

  /////////////////
  // Public APIs //
  /////////////////

  /**
   * Serializes a {@link SnapshotReport} to a JSON string
   *
   * @throws JsonProcessingException
   */
  public static String serializeSnapshotReport(SnapshotReport snapshotReport)
      throws JsonProcessingException {
    return OBJECT_MAPPER.writeValueAsString(snapshotReport);
  }

  /////////////////////////////////
  // Private fields and methods //
  ////////////////////////////////

  private static final ObjectMapper OBJECT_MAPPER =
      new ObjectMapper()
          .registerModule(
              new SimpleModule()
                  .addSerializer(SnapshotReport.class, new SnapshotReportSerializer()));

  private MetricsReportSerializers() {}

  /////////////////
  // Serializers //
  ////////////////

  static class SnapshotReportSerializer extends StdSerializer<SnapshotReport> {

    SnapshotReportSerializer() {
      super(SnapshotReport.class);
    }

    @Override
    public void serialize(
        SnapshotReport snapshotReport, JsonGenerator gen, SerializerProvider provider)
        throws IOException {
      gen.writeStartObject();
      gen.writeStringField("tablePath", snapshotReport.tablePath());
      gen.writeStringField("operationType", snapshotReport.operationType());
      gen.writeStringField("reportUUID", snapshotReport.reportUUID().toString());
      writeOptionalField(
          "version", snapshotReport.version(), item -> gen.writeNumberField("version", item), gen);
      writeOptionalField(
          "providedTimestamp",
          snapshotReport.providedTimestamp(),
          item -> gen.writeNumberField("providedTimestamp", item),
          gen);
      gen.writeFieldName("snapshotMetrics");
      writeSnapshotMetrics(snapshotReport.snapshotMetrics(), gen);
      writeOptionalField(
          "exception",
          snapshotReport.exception(),
          item -> gen.writeStringField("exception", item.toString()),
          gen);
      gen.writeEndObject();
    }

    private void writeSnapshotMetrics(SnapshotMetricsResult snapshotMetrics, JsonGenerator gen)
        throws IOException {
      gen.writeStartObject();
      writeOptionalField(
          "timestampToVersionResolutionDuration",
          snapshotMetrics.timestampToVersionResolutionDuration(),
          item -> gen.writeNumberField("timestampToVersionResolutionDuration", item),
          gen);
      gen.writeNumberField(
          "loadProtocolAndMetadataDuration", snapshotMetrics.loadInitialDeltaActionsDuration());
      gen.writeEndObject();
    }
  }

  //////////////////////////////////
  // Helper fx for serialization //
  /////////////////////////////////

  /**
   * For an optional item - If it is empty, writes out a null value - If it is non-empty, writes the
   * items value using the provided nonNullConsumer
   *
   * @param fieldName name of the field to write
   * @param item optional item
   * @param nonNullConsumer consumes an items non-null value
   * @throws IOException
   */
  private static <T> void writeOptionalField(
      String fieldName,
      Optional<T> item,
      ConsumerThrowsIOException<T> nonNullConsumer,
      JsonGenerator gen)
      throws IOException {
    if (item.isPresent()) {
      nonNullConsumer.accept(item.get());
    } else {
      gen.writeNullField(fieldName);
    }
  }

  // Need to create custom consumer so we can propagate the IOException without wrapping it
  private interface ConsumerThrowsIOException<T> {
    void accept(T t) throws IOException;
  }
}
