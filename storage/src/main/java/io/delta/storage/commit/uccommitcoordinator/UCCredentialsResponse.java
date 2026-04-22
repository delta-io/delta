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

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Response envelope for all three credential-vending entry points on
 * {@link UCDeltaClient}: {@code getTableCredentials}, {@code getStagingTableCredentials},
 * and {@code getPathCredentials}.
 */
public final class UCCredentialsResponse {

  private final List<UCStorageCredential> credentials;

  public UCCredentialsResponse(List<UCStorageCredential> credentials) {
    this.credentials = Collections.unmodifiableList(
        Objects.requireNonNull(credentials, "credentials"));
  }

  public List<UCStorageCredential> getCredentials() { return credentials; }

  @Override
  public String toString() {
    return "UCCredentialsResponse{credentialCount=" + credentials.size() + '}';
  }
}
