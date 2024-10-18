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

package io.delta.kernel.coordinatedcommits;

import static java.util.Objects.requireNonNull;

import io.delta.kernel.TableIdentifier;
import io.delta.kernel.annotation.Evolving;
import java.util.Map;
import java.util.Optional;

/**
 * The complete descriptor of a Coordinated Commits (CC) Delta table, including its logPath, table
 * identifier, and table CC table configuration.
 *
 * @since 3.3.0
 */
@Evolving
public class TableDescriptor {

  private final String logPath;
  private final Optional<TableIdentifier> tableIdOpt;
  private final Map<String, String> tableConf;

  public TableDescriptor(
      String logPath, Optional<TableIdentifier> tableIdOpt, Map<String, String> tableConf) {
    requireNonNull(logPath, "Expected non-null value for 'logPath'");
    requireNonNull(tableIdOpt, "Expected non-null value for 'tableIdOpt'");
    requireNonNull(tableConf, "Expected non-null value for 'tableConf'");

    this.logPath = logPath;
    this.tableIdOpt = tableIdOpt;
    this.tableConf = tableConf;
  }

  /** Returns the Delta log path of the table. */
  public String getLogPath() {
    return logPath;
  }

  /** Returns the optional table identifier of the table, e.g. <catalog> / <schema> / <tableName> */
  public Optional<TableIdentifier> getTableIdentifierOpt() {
    return tableIdOpt;
  }

  /**
   * Returns the Coordinated Commits table configuration.
   *
   * <p>This is the parsed value of the Delta table property {@link
   * io.delta.kernel.internal.TableConfig#COORDINATED_COMMITS_TABLE_CONF} and represents the
   * configuration properties for describing the Delta table to commit-coordinator.
   */
  public Map<String, String> getTableConf() {
    return tableConf;
  }
}
