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

package io.delta.storage.uc;

import io.delta.storage.commit.uccommitcoordinator.UCClient;
import io.unitycatalog.client.delta.model.CredentialOperation;
import io.unitycatalog.client.delta.model.CredentialsResponse;
import io.unitycatalog.client.delta.model.LoadTableResponse;

import java.io.IOException;

/**
 * UC client interface for Delta REST Catalog table methods.
 *
 * <p>Delta already uses {@link UCClient} for the legacy UC commit APIs. This interface adds the
 * named-table methods used by the Delta REST Catalog APIs. This is the transition interface while
 * callers migrate off {@link UCClient}; once that migration is complete, {@code UCClient} should
 * be deprecated and the Delta-side UC client surface should converge here.
 */
public interface UCDeltaClient extends UCClient {

  /**
   * Loads a table from Unity Catalog.
   *
   * <p>This is the client call for {@code GET
   * /delta/v1/catalogs/{catalog}/schemas/{schema}/tables/{table}}. The response contains table
   * metadata such as schema, location, properties, and any unbackfilled commits returned by the
   * server. This method does not read {@code _delta_log} or table data.
   */
  LoadTableResponse loadTable(
      String catalog,
      String schema,
      String table) throws IOException;

  /**
   * Gets temporary storage credentials for a table and an access mode.
   *
   * <p>This is the client call for {@code GET
   * /delta/v1/catalogs/{catalog}/schemas/{schema}/tables/{table}/credentials}. Callers use the
   * returned credentials when they need storage access to the table location. The requested
   * {@code operation} controls the access level of the credential.
   */
  CredentialsResponse getTableCredentials(
      CredentialOperation operation,
      String catalog,
      String schema,
      String table) throws IOException;
}
