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
import java.util.Map;
import java.util.Objects;

/**
 * One vended cloud-storage credential entry scoped to a path prefix. Carried inside
 * {@link UCCreateStagingTableResponse} and {@link UCCredentialsResponse}.
 *
 * <p>{@code pathPrefix} is the cloud URI prefix the credential is valid for (e.g.
 * {@code s3://bucket/path}). Callers match the longest prefix that contains the file
 * they intend to access.
 *
 * <p>{@code credentials} carries the cloud-vendor-specific secret bag (access key,
 * session token, refresh token, bearer token, etc.). The key space is provider-dependent
 * and opaque to storage.
 */
public final class UCStorageCredential {

  private final String pathPrefix;
  private final Map<String, String> credentials;
  private final long expiryMillis;

  public UCStorageCredential(
      String pathPrefix, Map<String, String> credentials, long expiryMillis) {
    this.pathPrefix = Objects.requireNonNull(pathPrefix, "pathPrefix");
    this.credentials = Collections.unmodifiableMap(
        Objects.requireNonNull(credentials, "credentials"));
    this.expiryMillis = expiryMillis;
  }

  public String getPathPrefix() { return pathPrefix; }
  public Map<String, String> getCredentials() { return credentials; }
  /** Expiration as epoch-millis. A value of {@code 0} means server did not declare a TTL. */
  public long getExpiryMillis() { return expiryMillis; }

  @Override
  public String toString() {
    return "UCStorageCredential{prefix='" + pathPrefix + "', keys=" + credentials.keySet()
        + ", expiryMillis=" + expiryMillis + '}';
  }
}
