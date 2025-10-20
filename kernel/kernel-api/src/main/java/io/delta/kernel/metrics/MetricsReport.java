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

package io.delta.kernel.metrics;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.delta.kernel.internal.metrics.MetricsReportSerializer;

/**
 * Interface containing the metrics for a given operation.
 *
 * <p>Implementations of this interface capture performance metrics and diagnostic information for
 * various Delta table operations. These metrics can be used for monitoring, debugging, and
 * performance analysis.
 */
public interface MetricsReport {

  /**
   * Converts this metrics report to a JSON string representation.
   *
   * <p>The JSON format includes all metrics data from this report in a structured format suitable
   * for logging, monitoring systems, or further analysis. The exact structure depends on the
   * specific report type (e.g., {@link SnapshotReport}, {@link ScanReport}, {@link
   * TransactionReport}).
   *
   * <p>The default serialization is performed using Jackson. See {@link MetricsReportSerializer}
   * for more details.
   *
   * @return a JSON string representation of this metrics report
   * @throws JsonProcessingException if the report cannot be serialized to JSON (e.g., due to
   *     circular references or unsupported types)
   */
  default String toJson() throws JsonProcessingException {
    return MetricsReportSerializer.serialize(this);
  }
}
