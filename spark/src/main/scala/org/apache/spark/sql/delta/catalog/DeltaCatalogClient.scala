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

package org.apache.spark.sql.delta.catalog

import org.apache.hadoop.fs.Path

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog.{Identifier, Table}

/**
 * Spark-facing Delta catalog API hook.
 *
 * <p>The interface is intentionally free of UC SDK and Hadoop credential dependencies so the shared
 * catalog path does not depend on a specific UC client implementation.
 */
private[catalog] trait DeltaCatalogClient {
  def loadTable(ident: Identifier): Option[Table]
}

private[delta] object DeltaCatalogClient {
  private[catalog] val UCDeltaRestCatalogApiEnabledKey =
    UCDeltaCatalogClient.UCDeltaRestCatalogApiEnabledKey
  private[catalog] val RenewCredentialEnabledKey =
    UCDeltaCatalogClient.RenewCredentialEnabledKey
  private[catalog] val CredScopedFsEnabledKey =
    UCDeltaCatalogClient.CredScopedFsEnabledKey

  private[catalog] def deltaRestApiEnabledConf(catalogName: String): String = {
    UCDeltaCatalogClient.deltaRestApiEnabledConf(catalogName)
  }

  private[catalog] def renewCredentialEnabledConf(catalogName: String): String = {
    UCDeltaCatalogClient.renewCredentialEnabledConf(catalogName)
  }

  private[catalog] def credScopedFsEnabledConf(catalogName: String): String = {
    UCDeltaCatalogClient.credScopedFsEnabledConf(catalogName)
  }

  private[delta] def pathCredentialOptions(
      spark: SparkSession,
      path: Path): Map[String, String] = {
    UCDeltaCatalogClient.pathCredentialOptions(spark, path)
  }
}
