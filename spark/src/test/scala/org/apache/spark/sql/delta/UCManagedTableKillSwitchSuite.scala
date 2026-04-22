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
import org.apache.spark.sql.delta.coordinatedcommits.CatalogOwnedTestBaseSuite
import org.apache.spark.sql.delta.skipping.clustering.{ClusteredTableUtils, ClusteringColumn}
import org.apache.spark.sql.delta.test.{DeltaSQLCommandTest, DeltaSQLTestUtils}

import org.apache.spark.sql.catalyst.TableIdentifier

/**
 * Unit tests for the kill switch that blocks clustering column changes on UC-managed
 * CatalogOwned tables. These tests bypass UCSingleCatalog (only available in sparkUnityCatalog)
 * by overriding [[OptimisticTransaction.isUCManagedTable]] in a test-local subclass.
 *
 * The kill switch fires in [[OptimisticTransaction.commitLarge]], which is used by RESTORE TABLE
 * and bypasses prepareCommit.
 */
class UCManagedTableKillSwitchSuite
  extends CatalogOwnedTestBaseSuite
  with DeltaSQLTestUtils
  with DeltaSQLCommandTest {

  // Enable CatalogOwned by default so every CREATE TABLE produces a CatalogOwned table.
  override def catalogOwnedCoordinatorBackfillBatchSize: Option[Int] = Some(1)

  /**
   * Test subclass that pretends it targets a UC-managed table, bypassing the real
   * [[CatalogOwnedTableUtils.getCatalogName]] check that requires UCSingleCatalog.
   */
  private class UCManagedTxn(log: DeltaLog, snap: Snapshot)
      extends OptimisticTransaction(log, None, snap) {
    override protected[delta] lazy val isUCManagedTable: Boolean = true
  }

  /**
   * Test subclass that pretends it targets a UC-managed table on the DRC path. Used to
   * verify the kill-switch softening rules in [[OptimisticTransaction]].
   */
  private class UCManagedDRCTxn(log: DeltaLog, snap: Snapshot)
      extends OptimisticTransaction(log, None, snap) {
    override protected[delta] lazy val isUCManagedTable: Boolean = true
    override protected[delta] lazy val isDRCEnabledTable: Boolean = true
  }

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

  test("commitLarge blocks clustering change on UC-managed CatalogOwned table") {
    withTable("tbl") {
      val (log, snap) = createClusteredTable("tbl")
      val ex = intercept[DeltaAnalysisException] {
        new UCManagedTxn(log, snap).commitLarge(
          spark,
          clusteringOnName.iterator,
          newProtocolOpt = None,
          op = DeltaOperations.Restore(Some(0L), None),
          context = Map.empty,
          metrics = Map.empty)
      }
      assert(ex.getMessage.contains("Clustering column changes on Unity Catalog managed tables"))
    }
  }

  test("commit with unchanged clustering is allowed on UC-managed CatalogOwned table") {
    withTable("tbl") {
      val (log, snap) = createClusteredTable("tbl")
      // Commit the same clustering DomainMetadata that the snapshot already has - must not throw.
      new UCManagedTxn(log, snap).commit(clusteringOnId, DeltaOperations.ManualUpdate)
    }
  }

  test("commitLarge allows clustering change when DRC is enabled on the table") {
    withTable("tbl") {
      val (log, snap) = createClusteredTable("tbl")
      // Same clustering change that throws in the UC-managed case above; on the DRC path the
      // kill switch is softened so commitLarge proceeds (the change is propagated via the
      // DRC commit's set-domain-metadata update instead).
      new UCManagedDRCTxn(log, snap).commitLarge(
        spark,
        clusteringOnName.iterator,
        newProtocolOpt = None,
        op = DeltaOperations.Restore(Some(0L), None),
        context = Map.empty,
        metrics = Map.empty)
    }
  }
}
