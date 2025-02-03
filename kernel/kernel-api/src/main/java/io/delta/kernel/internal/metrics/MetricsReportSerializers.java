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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import io.delta.kernel.expressions.Predicate;
import io.delta.kernel.metrics.MetricsReport;
import io.delta.kernel.metrics.ScanReport;
import io.delta.kernel.metrics.SnapshotReport;
import io.delta.kernel.metrics.TransactionReport;
import io.delta.kernel.types.StructType;

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

  /**
   * Serializes a {@link ScanReport} to a JSON string
   *
   * @throws JsonProcessingException
   */
  public static String serializeScanReport(ScanReport scanReport) throws JsonProcessingException {
    return OBJECT_MAPPER.writeValueAsString(scanReport);
  }

  /**
   * Serializes a {@link TransactionReport} to a JSON string
   *
   * @throws JsonProcessingException
   */
  public static String serializeTransactionReport(TransactionReport transactionReport)
      throws JsonProcessingException {
    return OBJECT_MAPPER.writeValueAsString(transactionReport);
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
              new SimpleModule().addSerializer(Predicate.class, new ToStringSerializer()));

  private MetricsReportSerializers() {}
}
