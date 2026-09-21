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

package org.apache.spark.sql.delta.v2.interop

import java.util.{Objects, Optional}

import scala.jdk.OptionConverters._

import org.apache.spark.annotation.Experimental
import org.apache.spark.sql.catalyst.catalog.CatalogTable

/** Request-scoped inputs for Delta DSv2 snapshot operations. */
@Experimental
final case class DeltaV2QueryContext(catalogTableOpt: Option[CatalogTable]) {
  Objects.requireNonNull(catalogTableOpt, "catalogTableOpt is null")
}

object DeltaV2QueryContext {
  val empty: DeltaV2QueryContext = DeltaV2QueryContext(None)

  /** Creates a query context from a Java optional CatalogTable. */
  def fromJava(catalogTableOpt: Optional[CatalogTable]): DeltaV2QueryContext = {
    DeltaV2QueryContext(Objects.requireNonNull(catalogTableOpt, "catalogTableOpt is null").toScala)
  }
}
