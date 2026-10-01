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

import io.delta.kernel.engine.MetricsReporter;
import io.delta.kernel.metrics.MetricsReport;
import io.delta.kernel.shaded.com.fasterxml.jackson.core.JsonProcessingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An implementation of {@link MetricsReporter} that logs the reports (as JSON) to Log4J at the info
 * level.
 */
public class LoggingMetricsReporter implements MetricsReporter {

  private static final Logger logger = LoggerFactory.getLogger(LoggingMetricsReporter.class);

  @Override
  public void report(MetricsReport report) {
    try {
      logger.info("{} = {}", report.getClass().getName(), report.toJson());
    } catch (JsonProcessingException e) {
      logger.warn("Serialization issue while logging metrics report {}: {}", report, e.toString());
    } catch (Exception e) {
      logger.warn("Unexpected error while logging metrics report {}:", report, e);
    }
  }
}
