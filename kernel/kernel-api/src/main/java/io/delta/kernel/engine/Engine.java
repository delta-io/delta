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

package io.delta.kernel.engine;

import io.delta.kernel.annotation.Evolving;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Interface encapsulating all clients needed by the Delta Kernel in order to read the Delta table.
 * Connectors are expected to pass an implementation of this interface when reading a Delta table.
 *
 * @since 3.0.0
 */
@Evolving
public interface Engine {

  /**
   * Get the connector provided {@link ExpressionHandler}.
   *
   * @return An implementation of {@link ExpressionHandler}.
   */
  ExpressionHandler getExpressionHandler();

  /**
   * Get the connector provided {@link JsonHandler}.
   *
   * @return An implementation of {@link JsonHandler}.
   */
  JsonHandler getJsonHandler();

  /**
   * Get the connector provided {@link FileSystemClient}.
   *
   * @return An implementation of {@link FileSystemClient}.
   */
  FileSystemClient getFileSystemClient();

  /**
   * Get the connector provided {@link ParquetHandler}.
   *
   * @return An implementation of {@link ParquetHandler}.
   */
  ParquetHandler getParquetHandler();

  /**
   * Retrieves a {@link CommitCoordinatorClientHandler} for the specified commit coordinator client.
   *
   * <p>{@link CommitCoordinatorClientHandler} helps Kernel perform commits to a table which is
   * owned by a commit coordinator.
   *
   * @see <a
   *     href="https://github.com/delta-io/delta/blob/master/protocol_rfcs/managed-commits.md#sample-commit-owner-api">Coordinated
   *     commit protocol table feature</a>.
   *     <p>This method creates and returns an implementation of {@link
   *     CommitCoordinatorClientHandler} based on the provided name and configuration of the
   *     underlying commit coordinator client.
   * @param name The identifier or name of the underlying commit coordinator client
   * @param conf The configuration settings for the underlying commit coordinator client
   * @return An implementation of {@link CommitCoordinatorClientHandler} configured for the
   *     specified client
   * @since 3.3.0
   */
  CommitCoordinatorClientHandler getCommitCoordinatorClientHandler(
      String name, Map<String, String> conf);

  /** Get the engine's {@link MetricsReporter} instances to push reports to. */
  default List<MetricsReporter> getMetricsReporters() {
    return Collections.emptyList();
  };
}
