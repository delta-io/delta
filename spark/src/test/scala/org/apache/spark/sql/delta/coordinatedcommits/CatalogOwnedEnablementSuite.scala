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

package org.apache.spark.sql.delta.coordinatedcommits

import org.apache.spark.sql.delta._
import org.apache.spark.sql.delta.test.{DeltaSQLCommandTest, DeltaSQLTestUtils}
import org.apache.commons.lang3.NotImplementedException

import org.apache.spark.sql.catalyst.TableIdentifier

class CatalogOwnedEnablementSuite
  extends DeltaSQLTestUtils
  with DeltaSQLCommandTest {

  override def beforeEach(): Unit = {
    super.beforeEach()
    CatalogOwnedCommitCoordinatorProvider.clearBuilders()
    CatalogOwnedCommitCoordinatorProvider.registerBuilder(
      "spark_catalog", TrackingInMemoryCommitCoordinatorBuilder(batchSize = 3))
  }

  private def validateCompleteEnablement(snapshot: Snapshot, expectEnabled: Boolean): Unit = {
    assert(snapshot.isCatalogOwned == expectEnabled)
    Seq(
      CatalogOwnedTableFeature,
      VacuumProtocolCheckTableFeature,
      InCommitTimestampTableFeature
    ).foreach { feature =>
      assert(snapshot.protocol.writerFeatures.exists(_.contains(feature.name)) == expectEnabled)
    }
    Seq(
      CatalogOwnedTableFeature,
      VacuumProtocolCheckTableFeature
    ).foreach { feature =>
      assert(snapshot.protocol.readerFeatures.exists(_.contains(feature.name)) == expectEnabled)
    }
  }

  test("ALTER TABLE - upgrade not supported") {
    withTable("t1") {
      spark.sql(s"CREATE TABLE t1 (id INT) USING delta")
      spark.sql(s"INSERT INTO t1 VALUES 1") // commit 1
      spark.sql(s"INSERT INTO t1 VALUES 2") // commit 2
      val log = DeltaLog.forTable(spark, TableIdentifier("t1"))
      validateCompleteEnablement(log.unsafeVolatileSnapshot, expectEnabled = false)
      // UPGRADE is not yet supported.
      val ex = intercept[NotImplementedException] {
        sql(s"ALTER TABLE t1 SET TBLPROPERTIES " +
          s"('delta.feature.${CatalogOwnedTableFeature.name}' = 'supported')")
      }
      assert(ex.getMessage.contains("Upgrading to CatalogOwned table is not yet supported."))
    }
  }

  test("CREATE TABLE - catalog-owned table downgrade is blocked") {
    withTable("t1") {
      spark.sql(s"CREATE TABLE t1 (id INT) USING delta TBLPROPERTIES " +
        s"('delta.feature.${CatalogOwnedTableFeature.name}' = 'supported')")
      spark.sql(s"INSERT INTO t1 VALUES 1") // commit 1
      spark.sql(s"INSERT INTO t1 VALUES 2") // commit 2
      val log = DeltaLog.forTable(spark, TableIdentifier("t1"))
      validateCompleteEnablement(log.unsafeVolatileSnapshot, expectEnabled = true)
      // Drop feature should fail as it is not supported.
      checkError(
        intercept[DeltaTableFeatureException] {
          sql(s"ALTER TABLE t1 DROP FEATURE '${CatalogOwnedTableFeature.name}'")
        },
        "DELTA_FEATURE_DROP_UNSUPPORTED_CLIENT_FEATURE",
        parameters = Map("feature" -> CatalogOwnedTableFeature.name)
      )
    }
  }
}
