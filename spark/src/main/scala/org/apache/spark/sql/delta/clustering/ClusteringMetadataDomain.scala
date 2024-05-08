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

package org.apache.spark.sql.delta.clustering

import org.apache.spark.sql.delta.skipping.clustering.ClusteringColumn
import org.apache.spark.sql.delta.{JsonMetadataDomain, JsonMetadataDomainUtils}
import org.apache.spark.sql.delta.actions.{Action, DomainMetadata}

/**
 * Metadata domain for Clustered table which tracks clustering columns.
 */
case class ClusteringMetadataDomain(clusteringColumns: Seq[Seq[String]])
  extends JsonMetadataDomain[ClusteringMetadataDomain] {
  override val domainName: String = ClusteringMetadataDomain.domainName
}

object ClusteringMetadataDomain extends JsonMetadataDomainUtils[ClusteringMetadataDomain] {
  override val domainName = "delta.clustering"
  // Extracts the ClusteringMetadataDomain and the removed field.
  def unapply(action: Action): Option[(ClusteringMetadataDomain, Boolean)] = action match {
    case d: DomainMetadata if d.domain == domainName => Some((fromJsonConfiguration(d), d.removed))
    case _ => None
  }

  def fromClusteringColumns(clusteringColumns: Seq[ClusteringColumn]): ClusteringMetadataDomain = {
    ClusteringMetadataDomain(clusteringColumns.map(_.physicalName))
  }
}
