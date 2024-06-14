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

class CoordinatedCommitsEnablementSuite
  extends CoordinatedCommitsBaseSuite
    with DeltaSQLTestUtils
    with DeltaSQLCommandTest
    with CoordinatedCommitsTestUtils {

  override def coordinatedCommitsBackfillBatchSize: Option[Int] = Some(3)

  import testImplicits._

  private def validateCoordinatedCommitsCompleteEnablement(
      snapshot: Snapshot, expectEnabled: Boolean): Unit = {
    assert(
      DeltaConfigs.COORDINATED_COMMITS_COORDINATOR_NAME.fromMetaData(snapshot.metadata).isDefined
      == expectEnabled)
    Seq(
      CoordinatedCommitsTableFeature,
      VacuumProtocolCheckTableFeature,
      InCommitTimestampTableFeature)
      .foreach { feature =>
        assert(snapshot.protocol.writerFeatures.exists(_.contains(feature.name)) == expectEnabled)
      }
    assert(
      snapshot.protocol.readerFeatures.exists(_.contains(VacuumProtocolCheckTableFeature.name))
        == expectEnabled)
    assert(DeltaConfigs.IN_COMMIT_TIMESTAMPS_ENABLED.fromMetaData(snapshot.metadata)
      == expectEnabled)
  }

  // ---- Tests START: Enablement at commit 0 ----
  test("enablement at commit 0: CC should enable ICT and VacuumProtocolCheck" +
    " --- writeintodelta api") {
    withTempDir { tempDir =>
      val tablePath = tempDir.getAbsolutePath
      Seq(1).toDF().write.format("delta").mode("overwrite").save(tablePath)
      val log = DeltaLog.forTable(spark, tablePath)
      validateCoordinatedCommitsCompleteEnablement(log.snapshot, expectEnabled = true)
    }
  }

  test("enablement at commit 0: CC should enable ICT and VacuumProtocolCheck" +
    " --- simple create table") {
    withTempDir { tempDir =>
      val tablePath = tempDir.getAbsolutePath
      sql(s"CREATE TABLE delta.`$tablePath` (id LONG) USING delta")
      val log = DeltaLog.forTable(spark, tablePath)
      validateCoordinatedCommitsCompleteEnablement(log.snapshot, expectEnabled = true)
    }
  }

  test("enablement at commit 0: CC should enable ICT and VacuumProtocolCheck" +
    " --- create or replace") {
    withTempDir { tempDir =>
      val tablePath = tempDir.getAbsolutePath
      Seq(1).toDF().write.format("delta").mode("overwrite").save(tablePath)
      Seq(1).toDF().write.format("delta").mode("overwrite").save(tablePath)
      val log = DeltaLog.forTable(spark, tablePath)
      validateCoordinatedCommitsCompleteEnablement(log.snapshot, expectEnabled = true)
    }
  }
  // ---- Tests END: Enablement at commit 0 ----

  // ---- Tests START: Enablement after commit 0 ----
  testWithDefaultCommitCoordinatorUnset(
    "enablement after commit 0: CC should enable ICT and VacuumProtocolCheck" +
      " --- update tblproperty") {
    withTempDir { tempDir =>
      val tablePath = tempDir.getAbsolutePath
      Seq(1).toDF().write.format("delta").mode("overwrite").save(tablePath) // commit 0
      Seq(1).toDF().write.format("delta").mode("append").save(tablePath) // commit 1
      val log = DeltaLog.forTable(spark, tablePath)
      validateCoordinatedCommitsCompleteEnablement(log.snapshot, expectEnabled = false)
      sql(s"ALTER TABLE delta.`$tablePath` SET TBLPROPERTIES " + // Enable CC
        s"('${DeltaConfigs.COORDINATED_COMMITS_COORDINATOR_NAME.key}' = 'tracking-in-memory')")
      Seq(1).toDF().write.format("delta").mode("overwrite").save(tablePath) // commit 3
      validateCoordinatedCommitsCompleteEnablement(log.update(), expectEnabled = true)
    }
  }

  testWithDefaultCommitCoordinatorUnset(
    "enablement after commit 0: CC should enable ICT and VacuumProtocolCheck" +
      " --- replace table") {
    withTempDir { tempDir =>
      val tablePath = tempDir.getAbsolutePath
      Seq(1).toDF().write.format("delta").mode("overwrite").save(tablePath) // commit 0
      Seq(1).toDF().write.format("delta").mode("append").save(tablePath) // commit 1
      val log = DeltaLog.forTable(spark, tablePath)
      validateCoordinatedCommitsCompleteEnablement(log.snapshot, expectEnabled = false)
      sql(s"REPLACE TABLE delta.`$tablePath` (value int) USING delta TBLPROPERTIES " + // Enable CC
        s"('${DeltaConfigs.COORDINATED_COMMITS_COORDINATOR_NAME.key}' = 'tracking-in-memory')")
      Seq(1).toDF().write.format("delta").mode("overwrite").save(tablePath) // commit 3
      validateCoordinatedCommitsCompleteEnablement(log.update(), expectEnabled = true)
    }
  }
  // ---- Tests END: Enablement after commit 0 ----
}
