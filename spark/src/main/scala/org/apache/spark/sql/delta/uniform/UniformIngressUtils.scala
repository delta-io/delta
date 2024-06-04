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

package org.apache.spark.sql.delta.uniform
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.delta.metering.DeltaLogging

object UniformIngressUtils extends DeltaLogging {
  private val UFI_IDENT = "_isUniformIngressTable"

  /**
   * Helper function to check if the given table is Uniform Ingress Table.
   *
   * @param catalogTable the `CatalogTable` corresponds to the table to be checked.
   * @return whether the table is a UFI table or not.
   */
  def isUniformIngressTable(catalogTable: CatalogTable): Boolean = {
    catalogTable.properties.contains(UFI_IDENT)
  }
}