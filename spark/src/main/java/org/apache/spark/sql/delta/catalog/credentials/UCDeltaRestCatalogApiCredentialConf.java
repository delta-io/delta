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

package org.apache.spark.sql.delta.catalog.credentials;

import io.unitycatalog.client.retry.JitterDelayRetryPolicy;
import io.unitycatalog.client.retry.RetryPolicy;

import org.apache.hadoop.conf.Configuration;

public class UCDeltaRestCatalogApiCredentialConf {
  private UCDeltaRestCatalogApiCredentialConf() {}

  public static final String S3A_CREDENTIALS_PROVIDER = "fs.s3a.aws.credentials.provider";

  public static final String S3A_INIT_ACCESS_KEY = "fs.s3a.init.access.key";
  public static final String S3A_INIT_SECRET_KEY = "fs.s3a.init.secret.key";
  public static final String S3A_INIT_SESSION_TOKEN = "fs.s3a.init.session.token";
  public static final String S3A_INIT_CRED_EXPIRED_TIME =
      "fs.s3a.init.credential.expired.time";

  public static final String AZURE_INIT_SAS_TOKEN = "fs.azure.init.sas.token";
  public static final String AZURE_INIT_SAS_TOKEN_EXPIRED_TIME =
      "fs.azure.init.sas.token.expired.time";

  public static final String FS_AZURE_ACCOUNT_AUTH_TYPE_PROPERTY_NAME =
      "fs.azure.account.auth.type";
  public static final String FS_AZURE_ACCOUNT_IS_HNS_ENABLED = "fs.azure.account.hns.enabled";
  public static final String FS_AZURE_SAS_TOKEN_PROVIDER_TYPE =
      "fs.azure.sas.token.provider.type";

  public static final String GCS_INIT_OAUTH_TOKEN = "fs.gs.init.oauth.token";
  public static final String GCS_INIT_OAUTH_TOKEN_EXPIRATION_TIME =
      "fs.gs.init.oauth.token.expiration.time";

  // Delta filters many table options before building Hadoop configurations, so all custom keys
  // that must reach FileSystem implementations intentionally use the fs.* namespace.
  public static final String UC_URI_KEY = "fs.unitycatalog.uri";
  public static final String UC_AUTH_PREFIX = "fs.unitycatalog.auth.";
  public static final String UC_AUTH_TYPE = "fs.unitycatalog.auth.type";
  public static final String UC_AUTH_TOKEN_KEY = "fs.unitycatalog.auth.token";

  public static final String UC_RENEWAL_LEAD_TIME_KEY =
      "fs.unitycatalog.renewal.leadTimeMillis";
  public static final long UC_RENEWAL_LEAD_TIME_DEFAULT_VALUE = 30_000L;

  public static final String UC_TEST_CLOCK_NAME = "fs.unitycatalog.test.clock.name";

  public static final String UC_CREDENTIALS_UID_KEY = "fs.unitycatalog.credentials.uid";

  public static final String UC_TABLE_ID_KEY = "fs.unitycatalog.table.id";
  public static final String UC_TABLE_OPERATION_KEY = "fs.unitycatalog.table.operation";

  public static final String UC_PATH_KEY = "fs.unitycatalog.path";
  public static final String UC_PATH_OPERATION_KEY = "fs.unitycatalog.path.operation";

  public static final String UC_CREDENTIALS_TYPE_KEY = "fs.unitycatalog.credentials.type";
  public static final String UC_CREDENTIALS_TYPE_TABLE_VALUE = "table";
  public static final String UC_CREDENTIALS_TYPE_PATH_VALUE = "path";

  public static final String UC_CREDENTIAL_CACHE_ENABLED_KEY =
      "fs.unitycatalog.credential.cache.enabled";
  public static final boolean UC_CREDENTIAL_CACHE_ENABLED_DEFAULT_VALUE = true;

  public static final String REQUEST_RETRY_MAX_ATTEMPTS_KEY =
      "fs.unitycatalog.request.retry.maxAttempts";
  public static final String REQUEST_RETRY_INITIAL_DELAY_KEY =
      "fs.unitycatalog.request.retry.initialDelayMs";
  public static final String REQUEST_RETRY_DELAY_MULTIPLIER_KEY =
      "fs.unitycatalog.request.retry.delayMultiplier";
  public static final String REQUEST_RETRY_DELAY_JITTER_FACTOR_KEY =
      "fs.unitycatalog.request.retry.delayJitterFactor";

  public static RetryPolicy createRequestRetryPolicy(Configuration conf) {
    JitterDelayRetryPolicy.Builder builder = JitterDelayRetryPolicy.builder();

    if (conf == null) {
      return builder.build();
    }

    builder.maxAttempts(
        conf.getInt(REQUEST_RETRY_MAX_ATTEMPTS_KEY, JitterDelayRetryPolicy.DEFAULT_MAX_ATTEMPTS));
    builder.initDelayMs(
        conf.getLong(
            REQUEST_RETRY_INITIAL_DELAY_KEY, JitterDelayRetryPolicy.DEFAULT_INITIAL_DELAY_MS));
    builder.delayMultiplier(
        conf.getDouble(
            REQUEST_RETRY_DELAY_MULTIPLIER_KEY, JitterDelayRetryPolicy.DEFAULT_DELAY_MULTIPLIER));
    builder.delayJitterFactor(
        conf.getDouble(
            REQUEST_RETRY_DELAY_JITTER_FACTOR_KEY,
            JitterDelayRetryPolicy.DEFAULT_DELAY_JITTER_FACTOR));

    return builder.build();
  }
}
