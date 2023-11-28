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

package org.apache.spark.sql.delta.skipping.clustering

import org.apache.spark.sql.delta.{ClusteringTableFeature, DeltaConfigs}
import org.apache.spark.sql.delta.actions.{Protocol, TableFeatureProtocolUtils}
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.sources.DeltaSQLConf

import org.apache.spark.sql.internal.SQLConf

/**
 * Clustered table utility functions.
 */
trait ClusteredTableUtilsBase extends DeltaLogging {
 /**
  * Returns whether the protocol version supports the Liquid table feature.
  */
  def isSupported(protocol: Protocol): Boolean = protocol.isFeatureSupported(ClusteringTableFeature)

  /** Returns true to enable clustering table and currently it is only enabled for testing.
   *
   * Note this function is going to be removed when clustering table is fully developed.
   */
  def clusteringTableFeatureEnabled: Boolean =
    SQLConf.get.getConf(DeltaSQLConf.DELTA_ENABLE_CLUSTERING_TABLE_FEATURE)

  /** The clustering implementation name for [[AddFile.clusteringProvider]] */
  def clusteringProvider: String = "liquid"
}

object ClusteredTableUtils extends ClusteredTableUtilsBase
