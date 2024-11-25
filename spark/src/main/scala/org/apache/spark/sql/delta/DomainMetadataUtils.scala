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

package org.apache.spark.sql.delta

import org.apache.spark.sql.delta.skipping.clustering.ClusteredTableUtils
import org.apache.spark.sql.delta.actions.{Action, DomainMetadata, Protocol}
import org.apache.spark.sql.delta.clustering.ClusteringMetadataDomain
import org.apache.spark.sql.delta.metering.DeltaLogging

/**
 * Domain metadata utility functions.
 */
trait DomainMetadataUtilsBase extends DeltaLogging {
  // List of metadata domains that will be removed for the REPLACE TABLE operation.
  protected val METADATA_DOMAINS_TO_REMOVE_FOR_REPLACE_TABLE: Set[String] = Set(
    ClusteringMetadataDomain.domainName)

  // List of metadata domains that will be copied from the table we are restoring to.
  // Note that ClusteringMetadataDomain are recreated in handleDomainMetadataForRestoreTable
  // instead of being blindly copied over.
  protected val METADATA_DOMAIN_TO_COPY_FOR_RESTORE_TABLE: Set[String] = Set.empty

  // List of metadata domains that will be copied from the table on a CLONE operation.
  protected val METADATA_DOMAIN_TO_COPY_FOR_CLONE_TABLE: Set[String] = Set(
    ClusteringMetadataDomain.domainName)

  /**
   * Returns whether the protocol version supports the [[DomainMetadata]] action.
   */
  def domainMetadataSupported(protocol: Protocol): Boolean =
    protocol.isFeatureSupported(DomainMetadataTableFeature)

  /**
   * Given a list of [[Action]]s, build a domain name to [[DomainMetadata]] map.
   * Note duplicated domain name is not expected otherwise an internal error is thrown.
   */
  def extractDomainMetadatasMap(actions: Seq[Action]): Map[String, DomainMetadata] = {
    actions
      .collect { case action: DomainMetadata => action }
      .groupBy(_.domain)
      .map { case (name, domains) =>
        if (domains.length != 1) {
          throw DeltaErrors.domainMetadataDuplicate(domains.head.domain)
        }
        name -> domains.head
      }
  }

  /**
   * Validate there are no two [[DomainMetadata]] actions with the same domain name. An internal
   * exception is thrown if any duplicated domains are detected.
   *
   * @param actions: Actions the current transaction wants to commit.
   */
  def validateDomainMetadataSupportedAndNoDuplicate(
      actions: Seq[Action], protocol: Protocol): Seq[DomainMetadata] = {
    val domainMetadatas = extractDomainMetadatasMap(actions)
    if (domainMetadatas.nonEmpty && !domainMetadataSupported(protocol)) {
      throw DeltaErrors.domainMetadataTableFeatureNotSupported(
        domainMetadatas.map(_._2.domain).mkString("[", ",", "]"))
    }
    domainMetadatas.values.toSeq
  }

  /**
   * Generates a new sequence of DomainMetadata to commits for REPLACE TABLE.
   *  - By default, existing metadata domains survive as long as they don't appear in the
   *    new metadata domains, in which case new metadata domains overwrite the existing ones.
   *  - Existing domains will be removed only if they appear in the pre-defined
   *    "removal" list (e.g., table features require some specific domains to be removed).
   */
  def handleDomainMetadataForReplaceTable(
      existingDomainMetadatas: Seq[DomainMetadata],
      newDomainMetadatas: Seq[DomainMetadata]): Seq[DomainMetadata] = {
    val newDomainNames = newDomainMetadatas.map(_.domain).toSet
    existingDomainMetadatas
      // Filter out metadata domains unless they are in the list to be removed
      // and they don't appear in the new metadata domains.
      .filter(m => !newDomainNames.contains(m.domain) &&
        METADATA_DOMAINS_TO_REMOVE_FOR_REPLACE_TABLE.contains(m.domain))
      .map(_.copy(removed = true)) ++ newDomainMetadatas
  }

  /**
   * Generates a new sequence of DomainMetadata to commits for RESTORE TABLE.
   *  - Domains in the toSnapshot will be copied if they appear in the pre-defined
   *    "copy" list (e.g., table features require some specific domains to be copied).
   *  - All other domains not in the list are dropped from the "toSnapshot".
   *
   * For clustering metadata domain, it overwrites the existing domain metadata in the
   * fromSnapshot with the following clustering columns.
   * 1. If toSnapshot is not a clustered table or missing domain metadata, use empty clustering
   *    columns.
   * 2. If toSnapshot is a clustered table, use the clustering columns from toSnapshot.
   *
   * @param toSnapshot    The snapshot being restored to, which is referred as "source" table.
   * @param fromSnapshot  The snapshot being restored from, which is the current state.
   */
  def handleDomainMetadataForRestoreTable(
      toSnapshot: Snapshot,
      fromSnapshot: Snapshot): Seq[DomainMetadata] = {
    val filteredDomainMetadata = toSnapshot.domainMetadata.filter { m =>
      METADATA_DOMAIN_TO_COPY_FOR_RESTORE_TABLE.contains(m.domain)
    }
    val clusteringColumnsToRestore = ClusteredTableUtils.getClusteringColumnsOptional(toSnapshot)

    val isRestoringToClusteredTable =
      ClusteredTableUtils.isSupported(toSnapshot.protocol) && clusteringColumnsToRestore.nonEmpty
    val clusteringColumns = if (isRestoringToClusteredTable) {
      // We overwrite the clustering columns in the fromSnapshot with the clustering columns
      // in the toSnapshot.
      clusteringColumnsToRestore.get
    } else {
      // toSnapshot is not a clustered table or missing domain metadata, so we write domain
      // metadata with empty clustering columns.
      Seq.empty
    }

    val matchingMetadataDomain =
      ClusteredTableUtils.getMatchingMetadataDomain(
        clusteringColumns,
        fromSnapshot.domainMetadata)
    filteredDomainMetadata ++ matchingMetadataDomain.clusteringDomainOpt
  }

  /**
   *  Generates sequence of DomainMetadata to commit for CLONE TABLE command.
   */
  def handleDomainMetadataForCloneTable(
      sourceDomainMetadatas: Seq[DomainMetadata]): Seq[DomainMetadata] = {
    sourceDomainMetadatas.filter { m =>
      METADATA_DOMAIN_TO_COPY_FOR_CLONE_TABLE.contains(m.domain)
    }
  }
}

object DomainMetadataUtils extends DomainMetadataUtilsBase
