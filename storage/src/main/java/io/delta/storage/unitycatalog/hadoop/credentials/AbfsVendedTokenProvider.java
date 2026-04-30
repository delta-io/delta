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

import io.unitycatalog.client.model.AzureUserDelegationSAS;

import java.util.Objects;

import io.delta.storage.unitycatalog.hadoop.UCDeltaRestCatalogApiCredentialConf;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.azurebfs.extensions.SASTokenProvider;

public class AbfsVendedTokenProvider extends GenericCredentialProvider implements SASTokenProvider {
  public static final String ACCESS_TOKEN_KEY = "fs.azure.sas.fixed.token";

  public AbfsVendedTokenProvider() {}

  @Override
  public void initialize(Configuration conf, String accountName) {
    initialize(conf);
  }

  @Override
  public GenericCredential initGenericCredential(Configuration conf) {
    if (conf.get(UCDeltaRestCatalogApiCredentialConf.AZURE_INIT_SAS_TOKEN) != null
        && conf.get(UCDeltaRestCatalogApiCredentialConf.AZURE_INIT_SAS_TOKEN_EXPIRED_TIME) != null) {
      String sasToken = conf.get(UCDeltaRestCatalogApiCredentialConf.AZURE_INIT_SAS_TOKEN);
      Objects.requireNonNull(
          sasToken,
          String.format(
              "Azure SAS token not set, please check '%s' in hadoop configuration",
              UCDeltaRestCatalogApiCredentialConf.AZURE_INIT_SAS_TOKEN));

      long expiredTimeMillis =
          conf.getLong(UCDeltaRestCatalogApiCredentialConf.AZURE_INIT_SAS_TOKEN_EXPIRED_TIME, 0L);
      if (expiredTimeMillis <= 0) {
        throw new IllegalStateException(
            String.format(
                "Azure SAS token expired time must be greater than 0, please check '%s' in "
                    + "hadoop configuration",
                UCDeltaRestCatalogApiCredentialConf.AZURE_INIT_SAS_TOKEN_EXPIRED_TIME));
      }

      return GenericCredential.forAzure(sasToken, expiredTimeMillis);
    } else {
      return null;
    }
  }

  @Override
  public String getSASToken(String account, String fileSystem, String path, String operation) {
    GenericCredential generic = accessCredentials();

    AzureUserDelegationSAS azureSAS = generic.temporaryCredentials().getAzureUserDelegationSas();
    Objects.requireNonNull(azureSAS, "Azure SAS of generic credential cannot be null");

    return azureSAS.getSasToken();
  }
}
