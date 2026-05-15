/*
 * Copyright (2026) The Delta Lake Project Authors.
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

package io.delta.storage.commit.uccommitcoordinator;

import io.delta.storage.commit.actions.AbstractMetadata;
import io.delta.storage.commit.uccommitcoordinator.UCDeltaModels.StagingTableInfo;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Extends {@link UCClient} with Delta table lifecycle operations backed by the UC Delta REST
 * Catalog API (load, stage, and create tables).
 */
public interface UCDeltaClient extends UCClient {

  /**
   * Loads a table's metadata from Unity Catalog.
   *
   * @param catalog the catalog name
   * @param schema  the schema name
   * @param table   the table name
   * @return the table's {@link AbstractMetadata}
   * @throws IOException on network or API errors
   */
  AbstractMetadata loadTable(String catalog, String schema, String table) throws IOException;

  /**
   * Reserves a staging slot for a new Delta table. The returned response contains the table ID,
   * storage location, and protocol/property requirements that the caller must honor when
   * finalizing the table with {@link #createTable}.
   *
   * @param catalog the catalog name
   * @param schema  the schema name
   * @param table   the table name
   * @return a {@link StagingTableInfo} with the reserved table details
   * @throws IOException on network or API errors
   */
  StagingTableInfo createStagingTable(String catalog, String schema, String table)
      throws IOException;

  /**
   * Finalizes a previously staged Delta table, making it visible in the catalog.
   *
   * @param catalog          the catalog name
   * @param schema           the schema name
   * @param name             the table name
   * @param location         the storage location
   * @param tableType        the table type (MANAGED or EXTERNAL), or {@code null}
   * @param comment          the table comment, or {@code null}
   * @param partitionColumns the partition column names, or {@code null}
   * @param protocol         the required Delta protocol, or {@code null}
   * @param properties       the table properties, or {@code null}
   * @return the newly created table's {@link AbstractMetadata}
   * @throws IOException on network or API errors
   */
  AbstractMetadata createTable(
      String catalog,
      String schema,
      String name,
      String location,
      UCDeltaModels.TableType tableType,
      String comment,
      List<String> partitionColumns,
      UCDeltaModels.DeltaProtocol protocol,
      Map<String, String> properties) throws IOException;
}
