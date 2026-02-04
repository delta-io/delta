/*
 * Copyright (2021) The Delta Lake Project Authors.
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

package io.delta.storage.commit;

import java.util.Map;

/**
 * Response object containing information about a staging table created by a catalog.
 *
 * A staging table is a temporary table location allocated by the catalog before the
 * actual Delta table is physically created. This is part of the three-phase table
 * creation process for catalog-managed tables:
 * 1. Create staging table to obtain a unique storage location
 * 2. Initialize the Delta table at the staging location
 * 3. Register the table with its final name in the catalog
 */
public class StagingTableInfo {
  private final String tableId;
  private final String storageLocation;
  private final Map<String, String> tableConf;

  /**
   * Constructs a new StagingTableInfo.
   *
   * @param tableId The unique identifier for this table assigned by the catalog
   * @param storageLocation The storage location (URI) where the table should be created
   * @param tableConf Additional table configuration provided by the catalog
   */
  public StagingTableInfo(
      String tableId,
      String storageLocation,
      Map<String, String> tableConf) {
    this.tableId = tableId;
    this.storageLocation = storageLocation;
    this.tableConf = tableConf;
  }

  /**
   * Returns the unique table identifier assigned by the catalog.
   * This ID should be used in subsequent operations like commit and getCommits.
   *
   * @return The table ID
   */
  public String getTableId() {
    return tableId;
  }

  /**
   * Returns the storage location (URI) where the Delta table should be created.
   * For example: "s3://bucket/path/to/table" or "abfss://container@account.dfs.core.windows.net/path"
   *
   * @return The storage location URI as a string
   */
  public String getStorageLocation() {
    return storageLocation;
  }

  /**
   * Returns additional table configuration provided by the catalog.
   * This may include metadata, table properties, or other catalog-specific settings.
   *
   * @return Map of table configuration key-value pairs
   */
  public Map<String, String> getTableConf() {
    return tableConf;
  }
}
