/*
 * Copyright (2025) The Delta Lake Project Authors.
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

package org.apache.spark.sql.execution.datasources.v2

import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.connector.catalog.{CatalogPlugin, Identifier, Table}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * Shim for DataSourceV2Relation to handle API changes between Spark versions.
 * In Spark 4.0, DataSourceV2Relation has 5 constructor parameters.
 */
object DataSourceV2RelationShim {

  /**
   * Main extractor for DataSourceV2Relation that works across Spark versions.
   * Returns the common fields that exist in all versions.
   */
  def unapply(plan: LogicalPlan): Option[
    (Table, Seq[AttributeReference],
      Option[CatalogPlugin],
      Option[Identifier],
      CaseInsensitiveStringMap)] = {
    plan match {
      case r: DataSourceV2Relation =>
        Some((r.table, r.output, r.catalog, r.identifier, r.options))
      case _ => None
    }
  }
}

/**
 * Simplified extractor when only table and options are needed.
 */
object DataSourceV2RelationSimple {
  def unapply(plan: LogicalPlan): Option[(Table, CaseInsensitiveStringMap)] = {
    plan match {
      case r: DataSourceV2Relation =>
        Some((r.table, r.options))
      case _ => None
    }
  }
}

/**
 * Extractor for cases that only need the table.
 */
object DataSourceV2RelationTable {
  def unapply(plan: LogicalPlan): Option[Table] = {
    plan match {
      case r: DataSourceV2Relation => Some(r.table)
      case _ => None
    }
  }
}
