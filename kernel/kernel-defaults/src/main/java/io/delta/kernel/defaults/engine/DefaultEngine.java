/*
 * Copyright (2023) The Delta Lake Project Authors.
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

import io.delta.kernel.engine.*;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;

/** Default implementation of {@link Engine} based on Hadoop APIs. */
public class DefaultEngine implements Engine {
  private final Configuration hadoopConf;

  protected DefaultEngine(Configuration hadoopConf) {
    this.hadoopConf = hadoopConf;
  }

  @Override
  public ExpressionHandler getExpressionHandler() {
    return new DefaultExpressionHandler();
  }

  @Override
  public JsonHandler getJsonHandler() {
    return new DefaultJsonHandler(hadoopConf);
  }

  @Override
  public FileSystemClient getFileSystemClient() {
    return new DefaultFileSystemClient(hadoopConf);
  }

  @Override
  public ParquetHandler getParquetHandler() {
    return new DefaultParquetHandler(hadoopConf);
  }

  @Override
  public CommitCoordinatorClientHandler getCommitCoordinatorClientHandler(
      String name, Map<String, String> conf) {
    return new DefaultCommitCoordinatorClientHandler(hadoopConf, name, conf);
  }

  @Override
  public List<MetricsReporter> getMetricsReporters() {
    return Collections.singletonList(new LoggingMetricsReporter());
  };

  /**
   * Create an instance of {@link DefaultEngine}.
   *
   * @param hadoopConf Hadoop configuration to use.
   * @return an instance of {@link DefaultEngine}.
   */
  public static DefaultEngine create(Configuration hadoopConf) {
    return new DefaultEngine(hadoopConf);
  }
}
