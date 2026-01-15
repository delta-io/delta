/*
 *  Copyright (2021) The Delta Lake Project Authors.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package io.delta.flink.table;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * A listener interface for receiving performance-related metric events emitted by Delta components.
 *
 * <p>Each event represents a completed operation identified by {@code eventName}, along with its
 * execution time in nanoseconds.
 *
 * <p>Implementations of this interface are expected to be lightweight and non-blocking, as
 * callbacks may be invoked on performance-critical execution paths.
 */
public interface MetricListener extends Serializable {

  /**
   * Called when a performance-related event occurs.
   *
   * @param eventName a logical name identifying the metric or operation (e.g., {@code
   *     "snapshot.load"}, {@code "commit.retry"})
   * @param elapseNano the elapsed time of the operation in nanoseconds
   */
  void onEvent(String eventName, long elapseNano);

  /**
   * A {@link MetricListener} implementation that aggregates basic statistics (minimum, maximum, and
   * average elapsed time) for each metric.
   *
   * <p>Statistics are maintained per {@code eventName} and updated incrementally as events are
   * received.
   *
   * <p>This implementation is intended for lightweight in-memory aggregation and diagnostics.
   * Thread-safety guarantees depend on the concrete implementation and should be documented
   * accordingly.
   */
  class StatsListener implements MetricListener {

    // (metricName, [count, max, min, sum])
    Map<String, long[]> summary = new HashMap<>();

    @Override
    public void onEvent(String eventName, long elapseNano) {
      summary.merge(
          eventName,
          new long[] {1, elapseNano, elapseNano, elapseNano},
          (existing, newvalue) -> {
            existing[0] += newvalue[0];
            existing[1] = Math.max(existing[1], newvalue[1]);
            existing[2] = Math.min(existing[2], newvalue[2]);
            existing[3] += newvalue[3];
            return existing;
          });
    }

    /**
     * Get the statistical results of metrics
     *
     * @return a map of (metricName, [count, max, min, average])
     */
    public Map<String, long[]> report() {
      return summary.entrySet().stream()
          .collect(
              Collectors.toMap(
                  Map.Entry::getKey,
                  entry ->
                      new long[] {
                        entry.getValue()[0],
                        entry.getValue()[1],
                        entry.getValue()[2],
                        entry.getValue()[3] / entry.getValue()[0]
                      }));
    }
  }

  /** Record the point-wise data points. */
  class PointListener implements MetricListener {

    private final Map<String, List<Long>> data = new HashMap<>();

    @Override
    public void onEvent(String eventName, long elapseNano) {
      data.computeIfAbsent(eventName, (key) -> new ArrayList<>()).add(elapseNano);
    }

    public Map<String, List<Long>> report() {
      return data;
    }
  }
}
