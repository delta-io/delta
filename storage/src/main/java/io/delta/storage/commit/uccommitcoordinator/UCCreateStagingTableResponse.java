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

package io.delta.storage.commit.uccommitcoordinator;

/**
 * Response from Unity Catalog's createStagingTable REST API.
 *
 * Contains the table ID and storage location allocated by Unity Catalog for a new staging table.
 */
public class UCCreateStagingTableResponse {
  private String tableId;
  private String storageLocation;
  private String metastoreId;

  public UCCreateStagingTableResponse() {}

  public UCCreateStagingTableResponse(
      String tableId,
      String storageLocation,
      String metastoreId) {
    this.tableId = tableId;
    this.storageLocation = storageLocation;
    this.metastoreId = metastoreId;
  }

  public String getTableId() {
    return tableId;
  }

  public void setTableId(String tableId) {
    this.tableId = tableId;
  }

  public String getStorageLocation() {
    return storageLocation;
  }

  public void setStorageLocation(String storageLocation) {
    this.storageLocation = storageLocation;
  }

  public String getMetastoreId() {
    return metastoreId;
  }

  public void setMetastoreId(String metastoreId) {
    this.metastoreId = metastoreId;
  }
}
