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
import java.util.Objects;
import java.util.Optional;

/**
 * The complete descriptor of a Coordinated Commits (CC) Delta table, including its logPath, table
 * identifier, and table CC configuration.
 *
 * @since 3.4.0
 */
@Evolving
public class TableDescriptor {

  private final String logPath;
  private final Optional<TableIdentifier> tableId;
  private final Map<String, String> tableConf;

  public TableDescriptor(
      String logPath, Optional<TableIdentifier> tableId, Map<String, String> tableConf) {
    this.logPath = requireNonNull(logPath, "logPath is null");
    this.tableId = requireNonNull(tableId, "tableId is null");
    this.tableConf = requireNonNull(tableConf, "tableConf is null");
  }

  /** Returns the Delta log path of the table. */
  public String getLogPath() {
    return logPath;
  }

  /** Returns the optional table identifier of the table, e.g. $catalog / $schema / $tableName */
  public Optional<TableIdentifier> getTableId() {
    return tableId;
  }

  /**
   * Returns the Coordinated Commits table configuration.
   *
   * <p>This is the parsed value of the Delta table property {@link
   * io.delta.kernel.internal.TableConfig#COORDINATED_COMMITS_TABLE_CONF} and represents the
   * properties for describing the Delta table to the commit-coordinator.
   */
  public Map<String, String> getTableConf() {
    return tableConf;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final TableDescriptor that = (TableDescriptor) o;
    return getLogPath().equals(that.getLogPath())
        && tableId.equals(that.tableId)
        && getTableConf().equals(that.getTableConf());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getLogPath(), tableId, getTableConf());
  }

  @Override
  public String toString() {
    return String.format(
        "TableDescriptor{logPath='%s', tableId=%s, tableConf=%s}", logPath, tableId, tableConf);
  }
}
