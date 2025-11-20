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
 * Test filesystem for validating GCS credentials.
 * Verifies that GCS credentials from server-side planning
 * were correctly injected into Hadoop configuration.
 *
 * Note: GCS credential injection is not yet implemented in ServerSidePlannedTable.
 * This class provides the infrastructure for future GCS support.
 */
class GCSCredentialTestFileSystem extends CredentialTestFileSystem {

  override def scheme(): String = "gs"

  override def checkCredentials(path: Path): Unit = {
    val conf = getConf

    // Check for OAuth access token (used for temporary credentials)
    val accessToken = conf.get("fs.gs.auth.access.token")
    val tokenExpiration = conf.get("fs.gs.auth.access.token.expiration")

    assert(accessToken != null && accessToken.nonEmpty,
      "fs.gs.auth.access.token must be set")
    assert(tokenExpiration != null && tokenExpiration.nonEmpty,
      "fs.gs.auth.access.token.expiration must be set")
  }
}
