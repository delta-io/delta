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

package org.apache.spark.sql.delta.cic

import scala.collection.JavaConverters._

import io.delta.storage.commit.TableIdentifier
import io.delta.storage.commit.uccommitcoordinator.UCDeltaClient

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.delta.coordinatedcommits.UCCommitCoordinatorBuilder

/**
 * Unity Catalog-backed [[IdentitySequenceService]]: the production backend that hands out
 * identity-value ranges for Concurrent Identity Columns (CIC) by delegating to a UC
 * [[UCDeltaClient]]'s identity-sequence operations, the same way the kernel's
 * `UCCatalogManagedClient` wraps a UC client for catalog-managed table operations.
 *
 * The identity-sequence operations live on [[UCDeltaClient]] (the Delta-Tables API client), not on
 * `UCClient`. The client comes from the same factory the commit coordinator uses
 * ([[UCCommitCoordinatorBuilder.ucClientFactory]]), so CIC requires the Delta-Tables API client
 * (the `deltaRestApi.enabled` catalog config must not be `false`).
 *
 * Wiring: instantiated reflectively (no-arg constructor) by [[IdentitySequenceServices.resolve]]
 * when a session sets `spark.databricks.delta.identityColumn.concurrent.serviceClassName` to this
 * class name. The UC endpoint and credentials come from the session's UC catalog config
 * (`spark.sql.catalog.<name>.*`).
 *
 * Table addressing: [[UCDeltaClient]] is name-based -- it takes a [[TableIdentifier]]
 * (catalog.schema.table), matching the UC Delta REST API. [[IdentitySequenceService]] requests
 * identify a table only by its UC table id, so this adapter has no name to pass:
 * [[tableIdentifierFor]] throws and the backend cannot be used end-to-end until the requests carry
 * the three-level name.
 *
 * Driver-only: never capture an instance in a closure shipped to executors.
 */
class UCDeltaIdentitySequenceService extends IdentitySequenceService {

  // Resolved once per instance (resolve creates a fresh instance per reservation) from the active
  // session's single UC catalog config. Lazy so construction is cheap and any config error
  // surfaces on the first sequence call rather than at resolve time.
  private lazy val ucClient: UCDeltaClient = {
    val spark = SparkSession.active
    val ucConfig = UCCommitCoordinatorBuilder.getCatalogConfigs(spark) match {
      case Nil =>
        throw new IllegalStateException(
          "No Unity Catalog catalog is configured, so the CIC identity-sequence service cannot be " +
          "reached. Configure one via `spark.sql.catalog.<name> = " +
          "io.unitycatalog.spark.UCSingleCatalog` with a `.uri` (and auth) sub-key.")
      case (_, cfg) :: Nil => cfg
      case multiple =>
        // The tableId-keyed request carries no catalog, so with more than one UC catalog we cannot
        // tell which endpoint owns the sequence.
        throw new IllegalStateException(
          "Multiple Unity Catalog catalogs are configured " +
          s"(${multiple.map(_._1).mkString(", ")}); the tableId-keyed CIC backend cannot pick one. " +
          "Configure a single UC catalog for now.")
    }
    // The identity-sequence ops live on UCDeltaClient (the Delta-Tables API client). The factory
    // returns the legacy commit-coordinator client when `deltaRestApi.enabled=false`, which does
    // not speak the Delta API, so require the Delta-Tables client here.
    UCCommitCoordinatorBuilder.ucClientFactory.createUCClient(ucConfig.asJava) match {
      case delta: UCDeltaClient => delta
      case other =>
        throw new IllegalStateException(
          "CIC identity sequences require the Unity Catalog Delta-Tables API client; got " +
          s"${other.getClass.getName}. Ensure `spark.sql.catalog.<name>.deltaRestApi.enabled` is " +
          "not set to false.")
    }
  }

  override def createSequence(req: CreateSequenceRequest): Unit = {
    require(req.step != 0L, "step must be non-zero")
    require(req.sequenceId.nonEmpty, "sequenceId must be non-empty")
    require(req.tableId.nonEmpty, "tableId must be non-empty")
    ucClient.createIdentitySequence(
      tableIdentifierFor(req.tableId), req.sequenceId, req.start, req.step)
  }

  override def reserveIds(req: ReserveIdsRequest): ReserveIdsResponse = {
    require(req.count > 0L, s"reserveIds requires count > 0, got ${req.count}")
    require(req.step != 0L, "reserveIds requires a non-zero step")
    // The client validates the granted range (step echo + that it spans `count` values) and maps a
    // not-found to NoSuchElementException, which the reservation path turns into SEQUENCE_NOT_FOUND.
    val range = ucClient.reserveIdentityIds(
      tableIdentifierFor(req.tableId), req.sequenceId, req.count, req.step)
    ReserveIdsResponse(
      sequenceId = range.getSequenceId,
      rangeStart = range.getRangeStart,
      rangeEnd = range.getRangeEnd,
      step = range.getStep)
  }

  override def dropSequence(req: DropSequenceRequest): Unit = {
    require(req.sequenceId.nonEmpty, "sequenceId must be non-empty")
    require(req.tableId.nonEmpty, "tableId must be non-empty")
    ucClient.dropIdentitySequence(tableIdentifierFor(req.tableId), req.sequenceId)
  }

  /**
   * The [[TableIdentifier]] (catalog.schema.table) that [[UCDeltaClient]] requires.
   * [[IdentitySequenceService]] requests carry only a UC table id, so there is no three-level name
   * to supply and this always throws.
   */
  private def tableIdentifierFor(tableId: String): TableIdentifier =
    throw new UnsupportedOperationException(
      s"CIC identity-sequence calls need the table's three-level UC name, which " +
      s"IdentitySequenceService requests do not carry (got table id '$tableId').")
}
