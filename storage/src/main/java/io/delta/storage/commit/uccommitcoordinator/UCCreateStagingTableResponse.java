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

import io.delta.storage.commit.actions.AbstractProtocol;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Response from phase-1 of the managed-table create flow. Carries the server-allocated
 * table UUID, the vended storage location, storage credentials for writing the initial
 * commit, the required protocol (features the server requires the client to enable), and
 * the required properties the server will set on the created table.
 *
 * <p>All fields are non-null by construction; empty lists/maps are valid values.
 */
public final class UCCreateStagingTableResponse {

  private final String tableId;
  private final String location;
  private final List<UCStorageCredential> storageCredentials;
  private final AbstractProtocol requiredProtocol;
  private final Map<String, String> requiredProperties;

  public UCCreateStagingTableResponse(
      String tableId,
      String location,
      List<UCStorageCredential> storageCredentials,
      AbstractProtocol requiredProtocol,
      Map<String, String> requiredProperties) {
    this.tableId = Objects.requireNonNull(tableId, "tableId");
    this.location = Objects.requireNonNull(location, "location");
    this.storageCredentials = Collections.unmodifiableList(
        Objects.requireNonNull(storageCredentials, "storageCredentials"));
    this.requiredProtocol = Objects.requireNonNull(requiredProtocol, "requiredProtocol");
    this.requiredProperties = Collections.unmodifiableMap(
        Objects.requireNonNull(requiredProperties, "requiredProperties"));
  }

  public String getTableId() { return tableId; }
  public String getLocation() { return location; }
  public List<UCStorageCredential> getStorageCredentials() { return storageCredentials; }
  public AbstractProtocol getRequiredProtocol() { return requiredProtocol; }
  public Map<String, String> getRequiredProperties() { return requiredProperties; }

  @Override
  public String toString() {
    return "UCCreateStagingTableResponse{tableId='" + tableId + "', location='" + location
        + "', credentialCount=" + storageCredentials.size()
        + ", requiredPropertyKeys=" + requiredProperties.keySet() + '}';
  }
}
