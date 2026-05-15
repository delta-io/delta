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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.catalog.{Identifier, Table}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * Backend hook through which [[AbstractDeltaCatalog]] injects custom catalog interactions
 * that bypass the catalog operations normally provided by Spark's
 * [[org.apache.spark.sql.connector.catalog.TableCatalog]] interface (the
 * [[org.apache.spark.sql.connector.catalog.DelegatingCatalogExtension]] delegate that
 * `AbstractDeltaCatalog` extends). Concrete implementations route table operations to a
 * catalog-specific path, e.g. talking directly to a REST endpoint instead of the
 * configured delegate, applying catalog-specific table-property handling, or vending
 * storage credentials on the returned [[Table]]. Keeping these behind a client interface
 * isolates that plumbing from `AbstractDeltaCatalog`.
 */
private[catalog] trait AbstractDeltaCatalogClient {

  /**
   * @throws org.apache.spark.sql.catalyst.analysis.NoSuchTableException if the catalog has
   *   no record of this identifier
   */
  def loadTable(ident: Identifier): Table
}

/** Builds a [[AbstractDeltaCatalogClient]] from catalog options. */
private[catalog] trait AbstractDeltaCatalogClientFactory {
  def fromCatalogOptions(
      catalogName: String,
      options: CaseInsensitiveStringMap,
      fallbackLoadTable: Identifier => Table): AbstractDeltaCatalogClient
}

private[catalog] object AbstractDeltaCatalogClient extends Logging {

  private val UC_DELTA_REST_API_ENABLED_KEY: String = "deltaRestApi.enabled"
  private val UC_DELTA_CATALOG_CLIENT_IMPL_CLASS_NAME: String =
    "org.apache.spark.sql.delta.catalog.UCDeltaCatalogClientImpl"

  /**
   * Returns a [[AbstractDeltaCatalogClient]] when the catalog opted in via `deltaRestApi.enabled`,
   * else `null`. The concrete impl is loaded reflectively so [[AbstractDeltaCatalog]] doesn't
   * compile-depend on it; environments that don't ship [[UCDeltaCatalogClientImpl]] degrade
   * to `null`.
   */
  def fromCatalogOptionsIfEnabled(
      catalogName: String,
      options: CaseInsensitiveStringMap,
      fallbackLoadTable: Identifier => Table): AbstractDeltaCatalogClient = {
    if (options.getBoolean(UC_DELTA_REST_API_ENABLED_KEY, false)) {
      val factory = try {
        // scalastyle:off classforname
        val cls = Class.forName(UC_DELTA_CATALOG_CLIENT_IMPL_CLASS_NAME + "$")
        // scalastyle:on classforname
        cls.getField("MODULE$").get(null).asInstanceOf[AbstractDeltaCatalogClientFactory]
      } catch {
        case _: ClassNotFoundException =>
          logWarning(s"'$UC_DELTA_REST_API_ENABLED_KEY' is true but " +
            s"$UC_DELTA_CATALOG_CLIENT_IMPL_CLASS_NAME is not on the classpath; skipping it.")
          return null
      }
      factory.fromCatalogOptions(catalogName, options, fallbackLoadTable)
    } else {
      null
    }
  }
}
