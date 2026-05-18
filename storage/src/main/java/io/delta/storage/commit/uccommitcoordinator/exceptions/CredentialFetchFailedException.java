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

package io.delta.storage.commit.uccommitcoordinator.exceptions;

import io.delta.storage.commit.uccommitcoordinator.UCDeltaClient;
import io.delta.storage.commit.uccommitcoordinator.UCDeltaModels.TableInfo;
import java.io.IOException;

/**
 * Thrown by {@link UCDeltaClient} when credential vending exhausts retries. Carries a
 * cred-less {@link TableInfo} so callers with a fallback (e.g. SSP) can recover.
 */
public class CredentialFetchFailedException extends IOException {

  private final TableInfo tableInfoWithoutCredentials;

  public CredentialFetchFailedException(
      String message, Throwable cause, TableInfo tableInfoWithoutCredentials) {
    super(message, cause);
    this.tableInfoWithoutCredentials = tableInfoWithoutCredentials;
  }

  public TableInfo getTableInfoWithoutCredentials() {
    return tableInfoWithoutCredentials;
  }
}
