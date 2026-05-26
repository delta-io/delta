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

import org.apache.spark.sql.delta.actions.DomainMetadata
import org.apache.spark.sql.delta.coordinatedcommits.{
  CatalogOwnedTableUtils,
  CatalogOwnedTestBaseSuite,
  TrackingCommitCoordinatorClient
}
import org.apache.spark.sql.delta.skipping.clustering.{ClusteredTableUtils, ClusteringColumn}
import org.apache.spark.sql.delta.test.{DeltaSQLCommandTest, DeltaSQLTestUtils}

import org.apache.spark.sql.catalyst.TableIdentifier

/**
 * Unit tests for clustering domain metadata changes on CatalogOwned tables through
 * [[OptimisticTransaction.commitLarge]], which is used by RESTORE TABLE and bypasses
 * prepareCommit.
 */
class UCManagedTableClusteringSuite
  extends CatalogOwnedTestBaseSuite
  with DeltaSQLTestUtils
  with DeltaSQLCommandTest {

  // Enable CatalogOwned by default so every CREATE TABLE produces a CatalogOwned table.
  override def catalogOwnedCoordinatorBackfillBatchSize: Option[Int] = Some(1)

  private val clusteringOnId: Seq[DomainMetadata] =
    Seq(ClusteredTableUtils.createDomainMetadata(Seq(ClusteringColumn(Seq("id")))))

  private val clusteringOnName: Seq[DomainMetadata] =
    Seq(ClusteredTableUtils.createDomainMetadata(Seq(ClusteringColumn(Seq("name")))))

  /** Creates a CatalogOwned clustered-by-id table and returns its (log, snapshot). */
  private def createClusteredTable(tableName: String): (DeltaLog, Snapshot) = {
    sql(s"CREATE TABLE $tableName (id INT, name STRING) USING delta " +
      s"CLUSTER BY (id) TBLPROPERTIES ('delta.feature.catalogManaged' = 'supported')")
    sql(s"INSERT INTO $tableName VALUES (1, 'a')")
    val (log, snap) =
      DeltaLog.forTableWithSnapshot(spark, TableIdentifier(tableName))
    assert(snap.isCatalogOwned, "table should be CatalogOwned")
    (log, snap)
  }

  test("commitLarge allows clustering change on CatalogOwned table") {
    withTable("tbl") {
      val (log, snap) = createClusteredTable("tbl")
      val (version, updatedSnapshot) = new OptimisticTransaction(log, None, snap).commitLarge(
        spark,
        clusteringOnName.iterator,
        newProtocolOpt = None,
        op = DeltaOperations.Restore(Some(0L), None),
        context = Map.empty,
        metrics = Map.empty)

      assert(version === snap.version + 1)
      assert(updatedSnapshot.domainMetadata.exists(_.configuration.contains("name")))

      val coordinator = getCatalogOwnedCommitCoordinatorClient(
        CatalogOwnedTableUtils.DEFAULT_CATALOG_NAME_FOR_TESTING)
        .asInstanceOf[TrackingCommitCoordinatorClient]
      val domainMetadata = coordinator.lastCommitUpdatedActions.getDomainMetadata
      assert(domainMetadata.size() === 1)
      assert(domainMetadata.get(0).getDomain === "delta.clustering")
      assert(domainMetadata.get(0).getConfiguration.contains("name"))
    }
  }

  test("commit with unchanged clustering is allowed on CatalogOwned table") {
    withTable("tbl") {
      val (log, snap) = createClusteredTable("tbl")
      // Commit the same clustering DomainMetadata that the snapshot already has.
      new OptimisticTransaction(log, None, snap)
        .commit(clusteringOnId, DeltaOperations.ManualUpdate)
    }
  }
}
