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

/** Delta-owned models for UC Delta table credentials. */
public final class UCDeltaModels {
  private UCDeltaModels() {}

  public enum CredentialOperation {
    READ,
    READ_WRITE
  }

  public static final class CredentialsResponse {
    private final List<StorageCredential> storageCredentials;

    public CredentialsResponse(List<StorageCredential> storageCredentials) {
      this.storageCredentials = storageCredentials;
    }

    public List<StorageCredential> getStorageCredentials() {
      return storageCredentials == null ? Collections.emptyList() : storageCredentials;
    }
  }

  public static final class StorageCredential {
    private final String prefix;
    private final CredentialOperation operation;
    private final StorageCredentialConfig config;
    private final Long expirationTimeMs;

    public StorageCredential(
        String prefix,
        CredentialOperation operation,
        StorageCredentialConfig config,
        Long expirationTimeMs) {
      this.prefix = prefix;
      this.operation = operation;
      this.config = config;
      this.expirationTimeMs = expirationTimeMs;
    }

    public String getPrefix() {
      return prefix;
    }

    public CredentialOperation getOperation() {
      return operation;
    }

    public StorageCredentialConfig getConfig() {
      return config;
    }

    public Long getExpirationTimeMs() {
      return expirationTimeMs;
    }
  }

  public static final class StorageCredentialConfig {
    private final String s3AccessKeyId;
    private final String s3SecretAccessKey;
    private final String s3SessionToken;
    private final String azureSasToken;
    private final String gcsOauthToken;

    public StorageCredentialConfig(
        String s3AccessKeyId,
        String s3SecretAccessKey,
        String s3SessionToken,
        String azureSasToken,
        String gcsOauthToken) {
      this.s3AccessKeyId = s3AccessKeyId;
      this.s3SecretAccessKey = s3SecretAccessKey;
      this.s3SessionToken = s3SessionToken;
      this.azureSasToken = azureSasToken;
      this.gcsOauthToken = gcsOauthToken;
    }

    public String getS3AccessKeyId() {
      return s3AccessKeyId;
    }

    public String getS3SecretAccessKey() {
      return s3SecretAccessKey;
    }

    public String getS3SessionToken() {
      return s3SessionToken;
    }

    public String getAzureSasToken() {
      return azureSasToken;
    }

    public String getGcsOauthToken() {
      return gcsOauthToken;
    }
  }
}
