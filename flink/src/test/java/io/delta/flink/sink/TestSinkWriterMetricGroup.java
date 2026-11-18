/*
 *  Copyright (2026) The Delta Lake Project Authors.
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

package io.delta.flink.sink;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.metrics.groups.OperatorIOMetricGroup;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;

/**
 * A {@link SinkWriterMetricGroup} that caches counters and gauges so they can be read back in
 * tests. Flink's {@code UnregisteredSinkWriterMetricGroup} returns new instances on every call,
 * making assertions impossible.
 */
class TestSinkWriterMetricGroup extends UnregisteredMetricsGroup implements SinkWriterMetricGroup {

  private final Counter numRecordsSendCounter = new SimpleCounter();
  private final Counter numBytesSendCounter = new SimpleCounter();
  private final Counter numRecordsSendErrorsCounter = new SimpleCounter();
  private final Counter numRecordsOutErrorsCounter = new SimpleCounter();
  private final Map<String, Counter> customCounters = new ConcurrentHashMap<>();
  private final Map<String, Gauge<?>> customGauges = new ConcurrentHashMap<>();

  @Override
  public OperatorIOMetricGroup getIOMetricGroup() {
    return UnregisteredMetricsGroup.createOperatorIOMetricGroup();
  }

  @Override
  public Counter getNumRecordsOutErrorsCounter() {
    return numRecordsOutErrorsCounter;
  }

  @Override
  public Counter getNumRecordsSendErrorsCounter() {
    return numRecordsSendErrorsCounter;
  }

  @Override
  public Counter getNumRecordsSendCounter() {
    return numRecordsSendCounter;
  }

  @Override
  public Counter getNumBytesSendCounter() {
    return numBytesSendCounter;
  }

  @Override
  public void setCurrentSendTimeGauge(Gauge<Long> currentSendTimeGauge) {
    customGauges.put("currentSendTime", currentSendTimeGauge);
  }

  @Override
  public Counter counter(String name) {
    return customCounters.computeIfAbsent(name, k -> new SimpleCounter());
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T, G extends Gauge<T>> G gauge(String name, G gauge) {
    customGauges.put(name, gauge);
    return gauge;
  }

  @SuppressWarnings("unchecked")
  public <T> Gauge<T> getGauge(String name) {
    return (Gauge<T>) customGauges.get(name);
  }
}
