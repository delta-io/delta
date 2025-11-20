/*
 * Copyright (2021) The Delta Lake Project Authors.
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

package org.apache.spark.sql.delta.serverSidePlanning

import org.apache.hadoop.fs.Path

/**
 * Test filesystem for validating Azure credentials.
 * Verifies that Azure credentials from server-side planning
 * were correctly injected into Hadoop configuration.
 *
 * Note: Azure credential injection is not yet implemented in ServerSidePlannedTable.
 * This class provides the infrastructure for future Azure support.
 */
class AzureCredentialTestFileSystem extends CredentialTestFileSystem {

  override def scheme(): String = "abfs"

  override def checkCredentials(path: Path): Unit = {
    val conf = getConf
    val uri = path.toUri
    val account = uri.getHost

    // Check for SAS token (used for temporary credentials)
    // Format: fs.azure.sas.<container>.<account>.dfs.core.windows.net
    val sasKey = s"fs.azure.sas.${uri.getPath.split("/").headOption.getOrElse("")}.$account"
    val sasToken = conf.get(sasKey)

    // Alternative: check for account key (used for static credentials)
    val accountKey = conf.get(s"fs.azure.account.key.$account")

    assert((sasToken != null && sasToken.nonEmpty) || (accountKey != null && accountKey.nonEmpty),
      s"Azure credentials must be set (either $sasKey or fs.azure.account.key.$account)")
  }
}
