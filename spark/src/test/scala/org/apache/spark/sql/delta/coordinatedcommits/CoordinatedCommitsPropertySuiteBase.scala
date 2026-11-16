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

import org.apache.spark.sql.delta.DeltaConfigs.{COORDINATED_COMMITS_COORDINATOR_CONF, COORDINATED_COMMITS_COORDINATOR_NAME, COORDINATED_COMMITS_TABLE_CONF}
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.delta.util.JsonUtils
import io.delta.storage.commit.CommitCoordinatorClient

import org.apache.spark.sql.{QueryTest, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.test.SharedSparkSession

trait CoordinatedCommitsPropertySuiteBase extends QueryTest
  with SharedSparkSession
  with DeltaSQLCommandTest
  with CoordinatedCommitsTestUtils {

  private def getRandomTableName: String = scala.util.Random.alphanumeric.take(10).mkString("")

  override def beforeEach(): Unit = {
    super.beforeEach()
    target = getRandomTableName
    source = getRandomTableName
    CommitCoordinatorProvider.clearNonDefaultBuilders()
    CommitCoordinatorProvider.registerBuilder(CommitCoordinatorBuilder1())
    CommitCoordinatorProvider.registerBuilder(CommitCoordinatorBuilder2())
  }

  protected val cc1: String = "commit-coordinator-1"

  private case class CommitCoordinatorBuilder1() extends CommitCoordinatorBuilder {
    private val commitCoordinator = new InMemoryCommitCoordinator(batchSize = 1000L)
    override def getName: String = cc1
    override def build(spark: SparkSession, conf: Map[String, String]): CommitCoordinatorClient =
      commitCoordinator
  }

  protected val cc2: String = "commit-coordinator-2"

  private case class CommitCoordinatorBuilder2() extends CommitCoordinatorBuilder {
    private val commitCoordinator = new InMemoryCommitCoordinator(batchSize = 1000L)
    override def getName: String = cc2
    override def build(spark: SparkSession, conf: Map[String, String]): CommitCoordinatorClient =
      commitCoordinator
  }

  protected var target: String = getRandomTableName
  protected var source: String = getRandomTableName

  protected val coordinatorNameKey: String = COORDINATED_COMMITS_COORDINATOR_NAME.key
  protected val coordinatorConfKey: String = COORDINATED_COMMITS_COORDINATOR_CONF.key
  protected val coordinatorNameDefaultKey: String =
    COORDINATED_COMMITS_COORDINATOR_NAME.defaultTablePropertyKey
  protected val coordinatorConfDefaultKey: String =
    COORDINATED_COMMITS_COORDINATOR_CONF.defaultTablePropertyKey
  protected val tableConfDefaultKey: String =
    COORDINATED_COMMITS_TABLE_CONF.defaultTablePropertyKey

  protected val randomCoordinatorConf: String =
    JsonUtils.toJson(Map("randomCoordinatorConf" -> "randomCoordinatorConfValue"))
  protected val randomTableConf: String =
    JsonUtils.toJson(Map("randomTableConf" -> "randomTableConfValue"))

  def getCCPropertiesClause(properties: Seq[(String, String)]): String = {
    if (properties.nonEmpty) {
      " TBLPROPERTIES (" +
        properties.map { case (k, v) => s"'$k' = '$v'" }.mkString(", ") +
        ")"
    } else {
      ""
    }
  }

  def verifyCommitCoordinator(table: String, expectedCoordinator: Option[String]): Unit = {
    assert(DeltaLog.forTable(spark, TableIdentifier(table))
      .update().metadata.coordinatedCommitsCoordinatorName == expectedCoordinator)
  }

  def testImpl(
    commandConfs: Seq[(String, String)] = Seq(),
    defaultConfs: Seq[(String, String)] = Seq(),
    targetConfs: Seq[(String, String)] = Seq(),
    sourceConfs: Seq[(String, String)] = Seq(),
    expectedCoordinator: Option[String] = None): Unit
}

trait CoordinatedCommitsPropertyCreateTableSuiteBase extends CoordinatedCommitsPropertySuiteBase {

  test("Commit coordinators are picked from command specification.") {
    testImpl(
      commandConfs = Seq(
        coordinatorNameKey -> cc1,
        coordinatorConfKey -> randomCoordinatorConf),
      expectedCoordinator = Some(cc1))
  }

  test("Commit coordinators are picked from default configurations if not specified in command.") {
    testImpl(
      defaultConfs = Seq(
        coordinatorNameDefaultKey -> cc1,
        coordinatorConfDefaultKey -> randomCoordinatorConf),
      expectedCoordinator = Some(cc1))
  }

  test("Command-specified commit coordinators take precedence over default configurations.") {
    testImpl(
      commandConfs = Seq(
        coordinatorNameKey -> cc1,
        coordinatorConfKey -> randomCoordinatorConf),
      defaultConfs = Seq(
        coordinatorNameDefaultKey -> cc2,
        coordinatorConfDefaultKey -> randomCoordinatorConf),
      expectedCoordinator = Some(cc1))
  }
}

class CoordinatedCommitsPropertyCreateTableSuite
  extends CoordinatedCommitsPropertyCreateTableSuiteBase {

  override def testImpl(
      commandConfs: Seq[(String, String)],
      defaultConfs: Seq[(String, String)],
      targetConfs: Seq[(String, String)],
      sourceConfs: Seq[(String, String)],
      expectedCoordinator: Option[String]): Unit = {
    withTable(target) {
      withSQLConf(defaultConfs: _*) {
        sql(s"CREATE TABLE $target (id LONG) USING delta" + getCCPropertiesClause(commandConfs))
      }
      verifyCommitCoordinator(target, expectedCoordinator)
    }
  }
}

class CoordinatedCommitsPropertyCreateTableAsSelectSuite
  extends CoordinatedCommitsPropertyCreateTableSuiteBase {

  override def testImpl(
      commandConfs: Seq[(String, String)],
      defaultConfs: Seq[(String, String)],
      targetConfs: Seq[(String, String)],
      sourceConfs: Seq[(String, String)],
      expectedCoordinator: Option[String]): Unit = {
    withTable(target, source) {
      sql(s"CREATE TABLE $source (id LONG) USING delta")
      sql(s"INSERT INTO $source VALUES (1)")
      withSQLConf(defaultConfs: _*) {
        sql(s"CREATE TABLE $target USING delta" +
          getCCPropertiesClause(commandConfs) + s" AS SELECT * FROM $source")
      }
      verifyCommitCoordinator(target, expectedCoordinator)
    }
  }
}

class CoordinatedCommitsPropertyCreateTableWithShallowCloneSuite
  extends CoordinatedCommitsPropertyCreateTableSuiteBase {

  override def testImpl(
      commandConfs: Seq[(String, String)] = Seq(),
      defaultConfs: Seq[(String, String)] = Seq(),
      targetConfs: Seq[(String, String)] = Seq(),
      sourceConfs: Seq[(String, String)] = Seq(),
      expectedCoordinator: Option[String] = None): Unit = {
    withTable(target, source) {
      sql(s"CREATE TABLE $source (id LONG) USING delta" + getCCPropertiesClause(sourceConfs))
      withSQLConf(defaultConfs: _*) {
        sql(s"CREATE TABLE $target SHALLOW CLONE $source" + getCCPropertiesClause(commandConfs))
      }
      verifyCommitCoordinator(target, expectedCoordinator)
    }
  }

  test("Source table's commit coordinator should never be copied to the target table: no commit " +
      "coordinators are specified") {
    testImpl(
      sourceConfs = Seq(
        coordinatorNameKey -> cc1,
        coordinatorConfKey -> randomCoordinatorConf),
      expectedCoordinator = None)
  }

  test("Source table's commit coordinator should never be copied to the target table: command " +
      "specifies a commit coordinator") {
    testImpl(
      commandConfs = Seq(
        coordinatorNameKey -> cc1,
        coordinatorConfKey -> randomCoordinatorConf),
      sourceConfs = Seq(
        coordinatorNameKey -> cc2,
        coordinatorConfKey -> randomCoordinatorConf),
      expectedCoordinator = Some(cc1))
  }

  test("Source table's commit coordinator should never be copied to the target table: default " +
      "configurations specify a commit coordinator") {
    testImpl(
      defaultConfs = Seq(
        coordinatorNameDefaultKey -> cc1,
        coordinatorConfDefaultKey -> randomCoordinatorConf),
      sourceConfs = Seq(
        coordinatorNameKey -> cc2,
        coordinatorConfKey -> randomCoordinatorConf),
      expectedCoordinator = Some(cc1))
  }
}

trait CoordinatedCommitsPropertyReplaceTableSuiteBase extends CoordinatedCommitsPropertySuiteBase {

  test("Default Coordinated Commits configurations from SparkSession are ignored") {
    testImpl(
      defaultConfs = Seq(
        coordinatorNameDefaultKey -> cc1,
        coordinatorConfDefaultKey -> randomCoordinatorConf),
      expectedCoordinator = None)

    testImpl(
      defaultConfs = Seq(
        coordinatorNameDefaultKey -> cc1,
        coordinatorConfDefaultKey -> randomCoordinatorConf,
        tableConfDefaultKey -> randomTableConf),
      expectedCoordinator = None)
  }

  test("Existing Coordinated Commits configurations from the target table are retained.") {
    testImpl(
      targetConfs = Seq(
        coordinatorNameKey -> cc1,
        coordinatorConfKey -> randomCoordinatorConf),
      expectedCoordinator = Some(cc1))
  }
}

class CoordinatedCommitsPropertyReplaceTableSuite
  extends CoordinatedCommitsPropertyReplaceTableSuiteBase {

  override def testImpl(
      commandConfs: Seq[(String, String)],
      defaultConfs: Seq[(String, String)],
      targetConfs: Seq[(String, String)],
      sourceConfs: Seq[(String, String)],
      expectedCoordinator: Option[String]): Unit = {
    withTable(target) {
      sql(s"CREATE TABLE $target (id LONG) USING delta" + getCCPropertiesClause(targetConfs))
      withSQLConf(defaultConfs: _*) {
        sql(s"REPLACE TABLE $target (id STRING) USING delta" + getCCPropertiesClause(commandConfs))
      }
      verifyCommitCoordinator(target, expectedCoordinator)
    }
  }
}

class CoordinatedCommitsPropertyReplaceTableAsSelectSuite
  extends CoordinatedCommitsPropertyReplaceTableSuiteBase {

  override def testImpl(
      commandConfs: Seq[(String, String)],
      defaultConfs: Seq[(String, String)],
      targetConfs: Seq[(String, String)],
      sourceConfs: Seq[(String, String)],
      expectedCoordinator: Option[String]): Unit = {
    withTable(target, source) {
      sql(s"CREATE TABLE $source (id LONG) USING delta")
      sql(s"INSERT INTO $source VALUES (1)")
      sql(s"CREATE TABLE $target (id LONG) USING delta" + getCCPropertiesClause(targetConfs))
      withSQLConf(defaultConfs: _*) {
        sql(s"REPLACE TABLE $target USING delta" +
          getCCPropertiesClause(commandConfs) + s" AS SELECT * FROM $source")
      }
      verifyCommitCoordinator(target, expectedCoordinator)
    }
  }
}

class CoordinatedCommitsPropertyReplaceTableWithShallowCloneSuite
  extends CoordinatedCommitsPropertyReplaceTableSuiteBase {

  override def testImpl(
      commandConfs: Seq[(String, String)] = Seq(),
      defaultConfs: Seq[(String, String)] = Seq(),
      targetConfs: Seq[(String, String)] = Seq(),
      sourceConfs: Seq[(String, String)] = Seq(),
      expectedCoordinator: Option[String] = None): Unit = {
    withTable(target, source) {
      sql(s"CREATE TABLE $target (id LONG) USING delta" + getCCPropertiesClause(targetConfs))
      sql(s"CREATE TABLE $source (id LONG) USING delta" + getCCPropertiesClause(sourceConfs))
      withSQLConf(defaultConfs: _*) {
        sql(s"REPLACE TABLE $target SHALLOW CLONE $source" + getCCPropertiesClause(commandConfs))
      }
      verifyCommitCoordinator(target, expectedCoordinator)
    }
  }

  test("Source table's commit coordinator should never be copied to the target table: target " +
      "table does not have any coordinator") {
    testImpl(
      sourceConfs = Seq(
        coordinatorNameKey -> cc1,
        coordinatorConfKey -> randomCoordinatorConf),
      expectedCoordinator = None)
  }

  test("Source table's commit coordinator should never be copied to the target table: target " +
      "table has a coordinator") {
    testImpl(
      targetConfs = Seq(
        coordinatorNameKey -> cc1,
        coordinatorConfKey -> randomCoordinatorConf),
      sourceConfs = Seq(
        coordinatorNameKey -> cc2,
        coordinatorConfKey -> randomCoordinatorConf),
      expectedCoordinator = Some(cc1))
  }
}
