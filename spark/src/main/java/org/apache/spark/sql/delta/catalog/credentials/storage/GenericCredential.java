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

package org.apache.spark.sql.delta.catalog.credentials.storage;

import io.unitycatalog.client.internal.Clock;
import io.unitycatalog.client.model.AwsCredentials;
import io.unitycatalog.client.model.AzureUserDelegationSAS;
import io.unitycatalog.client.model.GcpOauthToken;
import io.unitycatalog.client.model.TemporaryCredentials;

import java.util.Objects;

public class GenericCredential {
  private final TemporaryCredentials tempCred;

  public GenericCredential(TemporaryCredentials tempCred) {
    this.tempCred = tempCred;
  }

  public static GenericCredential forAws(
      String accessKey, String secretKey, String sessionToken, long expiredTimeMillis) {
    AwsCredentials awsCredentials = new AwsCredentials();
    awsCredentials.setAccessKeyId(accessKey);
    awsCredentials.setSecretAccessKey(secretKey);
    awsCredentials.setSessionToken(sessionToken);

    TemporaryCredentials tempCred = new TemporaryCredentials();
    tempCred.setAwsTempCredentials(awsCredentials);
    tempCred.setExpirationTime(expiredTimeMillis);

    return new GenericCredential(tempCred);
  }

  public static GenericCredential forAzure(String sasToken, long expiredTimeMillis) {
    AzureUserDelegationSAS azureSAS = new AzureUserDelegationSAS();
    azureSAS.setSasToken(sasToken);

    TemporaryCredentials tempCred = new TemporaryCredentials();
    tempCred.setAzureUserDelegationSas(azureSAS);
    tempCred.setExpirationTime(expiredTimeMillis);

    return new GenericCredential(tempCred);
  }

  public static GenericCredential forGcs(String oauthToken, long expiredTimeMillis) {
    GcpOauthToken gcpOauthToken = new GcpOauthToken();
    gcpOauthToken.setOauthToken(oauthToken);

    TemporaryCredentials tempCred = new TemporaryCredentials();
    tempCred.setGcpOauthToken(gcpOauthToken);
    tempCred.setExpirationTime(expiredTimeMillis);

    return new GenericCredential(tempCred);
  }

  public TemporaryCredentials temporaryCredentials() {
    return tempCred;
  }

  public boolean readyToRenew(Clock clock, long renewalLeadTimeMillis) {
    return tempCred.getExpirationTime() != null
        && tempCred.getExpirationTime() <= clock.now().toEpochMilli() + renewalLeadTimeMillis;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(tempCred);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    GenericCredential that = (GenericCredential) o;
    return Objects.equals(tempCred, that.tempCred);
  }
}
