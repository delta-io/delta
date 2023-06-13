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

import org.apache.spark.sql.delta.actions.{Action, DomainMetadata, Protocol}
import org.apache.spark.sql.delta.metering.DeltaLogging

object DomainMetadataUtils extends DeltaLogging {
  // List of metadata domains that will be removed for the REPLACE TABLE operation.
  private val METADATA_DOMAINS_TO_REMOVE_FOR_REPLACE_TABLE: Set[String] = Set(
  )
  // List of metadata domains that will be copied from the table we are restoring to.
  private val METADATA_DOMAIN_TO_COPY_FOR_RESTORE_TABLE =
    METADATA_DOMAINS_TO_REMOVE_FOR_REPLACE_TABLE

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
   *  - Source (table to restore to) domains will be copied if they appear in the pre-defined
   *    "copy" list (e.g., table features require some specific domains to be copied).
   *  - All other domains not in the list are "retained".
   */
  def handleDomainMetadataForRestoreTable(
      sourceDomainMetadatas: Seq[DomainMetadata]): Seq[DomainMetadata] = {
    sourceDomainMetadatas.filter { m =>
      METADATA_DOMAIN_TO_COPY_FOR_RESTORE_TABLE.contains(m.domain)
    }
  }
}
