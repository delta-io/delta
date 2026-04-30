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
package org.apache.spark.sql.delta.test.shims

import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.streaming.StreamingRelationV2
import org.apache.spark.sql.connector.catalog.{CatalogPlugin, Identifier, Table, TableProvider}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * Pattern extractor for `StreamingRelationV2` that stays source-compatible
 * across Spark versions. In Spark 4.2 the case class gained a 9th parameter
 * `sourceIdentifyingName`, which breaks 8-arg extractor patterns used in
 * Delta tests. This extractor exposes the 8 fields that exist in all
 * supported versions.
 */
object StreamingRelationV2Shim {
  def unapply(r: StreamingRelationV2): Option[(
      Option[TableProvider],
      String,
      Table,
      CaseInsensitiveStringMap,
      Seq[AttributeReference],
      Option[CatalogPlugin],
      Option[Identifier],
      Option[LogicalPlan])] =
    Some((r.source, r.sourceName, r.table, r.extraOptions, r.output,
          r.catalog, r.identifier, r.v1Relation))
}
