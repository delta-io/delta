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

import org.apache.spark.sql.SparkSession

/**
 * Factory that creates IcebergRESTCatalogPlanningClient instances.
 * Lives in the iceberg module alongside the implementation.
 */
class IcebergRESTCatalogPlanningClientFactory extends ServerSidePlanningClientFactory {
  override def buildForCatalog(
      spark: SparkSession,
      catalogName: String): ServerSidePlanningClient = {
    val catalogUri = spark.conf.get(s"spark.sql.catalog.$catalogName.uri", "")
    val token = spark.conf.get(s"spark.sql.catalog.$catalogName.token", "")

    if (catalogUri.isEmpty) {
      throw new IllegalStateException(
        s"Catalog URI not configured for catalog '$catalogName'. " +
        s"Please set spark.sql.catalog.$catalogName.uri")
    }

    new IcebergRESTCatalogPlanningClient(catalogUri, token)
  }
}
