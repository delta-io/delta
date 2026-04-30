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

package io.delta.storage.unitycatalog.hadoop.credentials;

import java.util.Objects;

import org.apache.hadoop.conf.Configuration;
import io.delta.storage.unitycatalog.hadoop.UCDeltaRestCatalogApiCredentialConf;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;

public class AwsVendedTokenProvider extends GenericCredentialProvider
    implements AwsCredentialsProvider {

  public AwsVendedTokenProvider(Configuration conf) {
    initialize(conf);
  }

  @Override
  public GenericCredential initGenericCredential(Configuration conf) {
    if (conf.get(UCDeltaRestCatalogApiCredentialConf.S3A_INIT_ACCESS_KEY) != null
        && conf.get(UCDeltaRestCatalogApiCredentialConf.S3A_INIT_SECRET_KEY) != null
        && conf.get(UCDeltaRestCatalogApiCredentialConf.S3A_INIT_SESSION_TOKEN) != null) {
      String accessKey = conf.get(UCDeltaRestCatalogApiCredentialConf.S3A_INIT_ACCESS_KEY);
      String secretKey = conf.get(UCDeltaRestCatalogApiCredentialConf.S3A_INIT_SECRET_KEY);
      String sessionToken = conf.get(UCDeltaRestCatalogApiCredentialConf.S3A_INIT_SESSION_TOKEN);

      long expiredTimeMillis =
          conf.getLong(UCDeltaRestCatalogApiCredentialConf.S3A_INIT_CRED_EXPIRED_TIME, Long.MAX_VALUE);
      if (expiredTimeMillis <= 0) {
        throw new IllegalStateException(
            String.format(
                "Expired time %s must be greater than 0, please check configure key '%s'",
                expiredTimeMillis,
                UCDeltaRestCatalogApiCredentialConf.S3A_INIT_CRED_EXPIRED_TIME));
      }

      return GenericCredential.forAws(accessKey, secretKey, sessionToken, expiredTimeMillis);
    } else {
      return null;
    }
  }

  @Override
  public AwsCredentials resolveCredentials() {
    GenericCredential generic = accessCredentials();

    io.unitycatalog.client.model.AwsCredentials awsTempCred =
        generic.temporaryCredentials().getAwsTempCredentials();
    Objects.requireNonNull(awsTempCred, "AWS temp credential of generic credentials cannot be null");

    return AwsSessionCredentials.builder()
        .accessKeyId(awsTempCred.getAccessKeyId())
        .secretAccessKey(awsTempCred.getSecretAccessKey())
        .sessionToken(awsTempCred.getSessionToken())
        .build();
  }
}
