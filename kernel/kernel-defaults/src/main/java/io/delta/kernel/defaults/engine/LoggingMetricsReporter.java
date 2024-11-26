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
package io.delta.kernel.defaults.engine;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.delta.kernel.engine.MetricsReporter;
import io.delta.kernel.internal.metrics.MetricsReportSerializers;
import io.delta.kernel.internal.snapshot.SnapshotManager;
import io.delta.kernel.metrics.MetricsReport;
import io.delta.kernel.metrics.SnapshotReport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An implementation of {@link MetricsReporter} that logs the reports (as JSON) to Log4J at the info
 * level.
 */
public class LoggingMetricsReporter implements MetricsReporter {

  private static final Logger logger = LoggerFactory.getLogger(SnapshotManager.class);

  @Override
  public void report(MetricsReport report) {
    try {
      if (report instanceof SnapshotReport) {
        logger.info(
            "SnapshotReport = %s",
            MetricsReportSerializers.serializeSnapshotReport((SnapshotReport) report));
      } else {
        logger.info(
            "%s = [%s does not support serializing this type of MetricReport]",
            report.getClass(), this.getClass());
      }
    } catch (JsonProcessingException e) {
      logger.info("Encountered exception while serializing report %s: %s", report, e);
    }
  }
}
