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

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.delta.DeltaConfigs.{COORDINATED_COMMITS_COORDINATOR_CONF, COORDINATED_COMMITS_COORDINATOR_NAME, COORDINATED_COMMITS_TABLE_CONF}
import org.apache.spark.sql.delta.DeltaIllegalArgumentException
import org.scalatest.Tag

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SharedSparkSession

class CoordinatedCommitsUtilsSuite extends QueryTest
  with SharedSparkSession
  with CoordinatedCommitsTestUtils {

  /////////////////////////////////////////////////////////////////////////////////////////////
  //     Test CoordinatedCommitsUtils.validateCoordinatedCommitsConfigurationsImpl STARTS    //
  /////////////////////////////////////////////////////////////////////////////////////////////

  def gridTest[A](testNamePrefix: String, testTags: Tag*)(params: Seq[A])(
    testFun: A => Unit): Unit = {
    for (param <- params) {
      test(testNamePrefix + s" ($param)", testTags: _*)(testFun(param))
    }
  }

  private val cNameKey = COORDINATED_COMMITS_COORDINATOR_NAME.key
  private val cConfKey = COORDINATED_COMMITS_COORDINATOR_CONF.key
  private val tableConfKey = COORDINATED_COMMITS_TABLE_CONF.key
  private val cName = cNameKey -> "some-cc-name"
  private val cConf = cConfKey -> "some-cc-conf"
  private val tableConf = tableConfKey -> "some-table-conf"

  private val cNameDefaultKey = COORDINATED_COMMITS_COORDINATOR_NAME.defaultTablePropertyKey
  private val cConfDefaultKey = COORDINATED_COMMITS_COORDINATOR_CONF.defaultTablePropertyKey
  private val tableConfDefaultKey = COORDINATED_COMMITS_TABLE_CONF.defaultTablePropertyKey
  private val cNameDefault = cNameDefaultKey -> "some-cc-name"
  private val cConfDefault = cConfDefaultKey -> "some-cc-conf"
  private val tableConfDefault = tableConfDefaultKey -> "some-table-conf"

  private val command = "CLONE"

  private def errCannotOverride = new DeltaIllegalArgumentException(
    "DELTA_CANNOT_OVERRIDE_COORDINATED_COMMITS_CONFS", Array(command))

  private def errMissingConfInCommand(key: String) = new DeltaIllegalArgumentException(
    "DELTA_MUST_SET_ALL_COORDINATED_COMMITS_CONFS_IN_COMMAND", Array(command, key))

  private def errMissingConfInSession(key: String) = new DeltaIllegalArgumentException(
    "DELTA_MUST_SET_ALL_COORDINATED_COMMITS_CONFS_IN_SESSION", Array(command, key))

  private def errTableConfInCommand = new DeltaIllegalArgumentException(
    "DELTA_CONF_OVERRIDE_NOT_SUPPORTED_IN_COMMAND", Array(command, tableConfKey))

  private def errTableConfInSession = new DeltaIllegalArgumentException(
    "DELTA_CONF_OVERRIDE_NOT_SUPPORTED_IN_SESSION",
    Array(command, tableConfDefaultKey, tableConfDefaultKey))

  private def testValidationForCreateDeltaTableCommand(
      tableExists: Boolean,
      propertyOverrides: Map[String, String],
      defaultConfs: Seq[(String, String)],
      errorOpt: Option[DeltaIllegalArgumentException]): Unit = {
    withoutCoordinatedCommitsDefaultTableProperties {
      withSQLConf(defaultConfs: _*) {
        if (errorOpt.isDefined) {
          val e = intercept[DeltaIllegalArgumentException] {
            CoordinatedCommitsUtils.validateConfigurationsForCreateDeltaTableCommandImpl(
              spark, propertyOverrides, tableExists, command)
          }
          checkError(
            exception = e,
            condition = errorOpt.get.getErrorClass,
            sqlState = errorOpt.get.getSqlState,
            parameters = errorOpt.get.getMessageParameters.asScala.toMap)
        } else {
          CoordinatedCommitsUtils.validateConfigurationsForCreateDeltaTableCommandImpl(
            spark, propertyOverrides, tableExists, command)
        }
      }
    }
  }

  // tableExists: True
  //            | False
  //
  // propertyOverrides: Map.empty
  //                  | Map(cName)
  //                  | Map(cName, cConf)
  //                  | Map(cName, cConf, tableConf)
  //                  | Map(tableConf)
  //
  // defaultConf: Seq.empty
  //            | Seq(cNameDefault)
  //            | Seq(cNameDefault, cConfDefault)
  //            | Seq(cNameDefault, cConfDefault, tableConfDefault)
  //            | Seq(tableConfDefault)
  //
  // errorOpt: None
  //         | Some(errCannotOverride)
  //         | Some(errMissingConfInCommand(cConfKey))
  //         | Some(errMissingConfInSession(cConfKey))
  //         | Some(errTableConfInCommand)
  //         | Some(errTableConfInSession)

  gridTest("During CLONE, CoordinatedCommitsUtils.validateCoordinatedCommitsConfigurationsImpl " +
      "passes for existing target tables with no explicit Coordinated Commits Configurations.") (
    Seq(
      Seq.empty,
      // Not having any explicit Coordinated Commits configurations, but having an illegal
      // combination of Coordinated Commits configurations in default: pass.
      // This is because we don't consider default configurations when the table exists.
      Seq(cNameDefault),
      Seq(cNameDefault, cConfDefault),
      Seq(cNameDefault, cConfDefault, tableConfDefault),
      Seq(tableConfDefault)
    )
  ) { defaultConfs: Seq[(String, String)] =>
    testValidationForCreateDeltaTableCommand(
      tableExists = true,
      propertyOverrides = Map.empty,
      defaultConfs,
      errorOpt = None)
  }

  gridTest("During CLONE, CoordinatedCommitsUtils.validateCoordinatedCommitsConfigurationsImpl " +
      "fails for existing target tables with any explicit Coordinated Commits Configurations.") (
    Seq(
      (Map(cName), Seq.empty),
      (Map(cName), Seq(cNameDefault)),
      (Map(cName), Seq(cNameDefault, cConfDefault)),
      (Map(cName), Seq(cNameDefault, cConfDefault, tableConfDefault)),
      (Map(cName), Seq(tableConfDefault)),

      (Map(cName, cConf), Seq.empty),
      (Map(cName, cConf), Seq(cNameDefault)),
      (Map(cName, cConf), Seq(cNameDefault, cConfDefault)),
      (Map(cName, cConf), Seq(cNameDefault, cConfDefault, tableConfDefault)),
      (Map(cName, cConf), Seq(tableConfDefault)),

      (Map(cName, cConf, tableConf), Seq.empty),
      (Map(cName, cConf, tableConf), Seq(cNameDefault)),
      (Map(cName, cConf, tableConf), Seq(cNameDefault, cConfDefault)),
      (Map(cName, cConf, tableConf), Seq(cNameDefault, cConfDefault, tableConfDefault)),
      (Map(cName, cConf, tableConf), Seq(tableConfDefault)),

      (Map(tableConf), Seq.empty),
      (Map(tableConf), Seq(cNameDefault)),
      (Map(tableConf), Seq(cNameDefault, cConfDefault)),
      (Map(tableConf), Seq(cNameDefault, cConfDefault, tableConfDefault)),
      (Map(tableConf), Seq(tableConfDefault))
    )
  ) { case (
      propertyOverrides: Map[String, String],
      defaultConfs: Seq[(String, String)]) =>
    testValidationForCreateDeltaTableCommand(
      tableExists = true,
      propertyOverrides,
      defaultConfs,
      errorOpt = Some(errCannotOverride))
  }

  gridTest("During CLONE, CoordinatedCommitsUtils.validateCoordinatedCommitsConfigurationsImpl " +
      "works correctly for new target tables with default Coordinated Commits Configurations.") (
    Seq(
      (Seq.empty, None),
      (Seq(cNameDefault), Some(errMissingConfInSession(cConfDefaultKey))),
      (Seq(cNameDefault, cConfDefault), None),
      (Seq(cNameDefault, cConfDefault, tableConfDefault), Some(errTableConfInSession)),
      (Seq(tableConfDefault), Some(errTableConfInSession))
    )
  ) { case (
      defaultConfs: Seq[(String, String)],
      errorOpt: Option[DeltaIllegalArgumentException]) =>
    testValidationForCreateDeltaTableCommand(
      tableExists = false,
      propertyOverrides = Map.empty,
      defaultConfs,
      errorOpt)
  }

  gridTest("During CLONE, CoordinatedCommitsUtils.validateCoordinatedCommitsConfigurationsImpl " +
      "fails for new target tables with any illegal explicit Coordinated Commits Configurations.") (
    Seq(
      (Map(cName), Seq.empty, Some(errMissingConfInCommand(cConfKey))),
      (Map(cName), Seq(cNameDefault), Some(errMissingConfInCommand(cConfKey))),
      (Map(cName), Seq(cNameDefault, cConfDefault), Some(errMissingConfInCommand(cConfKey))),
      (Map(cName), Seq(cNameDefault, cConfDefault, tableConfDefault),
        Some(errMissingConfInCommand(cConfKey))),
      (Map(cName), Seq(tableConfDefault), Some(errMissingConfInCommand(cConfKey))),

      (Map(cName, cConf, tableConf), Seq.empty, Some(errTableConfInCommand)),
      (Map(cName, cConf, tableConf), Seq(cNameDefault), Some(errTableConfInCommand)),
      (Map(cName, cConf, tableConf), Seq(cNameDefault, cConfDefault), Some(errTableConfInCommand)),
      (Map(cName, cConf, tableConf), Seq(cNameDefault, cConfDefault, tableConfDefault),
        Some(errTableConfInCommand)),
      (Map(cName, cConf, tableConf), Seq(tableConfDefault), Some(errTableConfInCommand)),

      (Map(tableConf), Seq.empty, Some(errTableConfInCommand)),
      (Map(tableConf), Seq(cNameDefault), Some(errTableConfInCommand)),
      (Map(tableConf), Seq(cNameDefault, cConfDefault), Some(errTableConfInCommand)),
      (Map(tableConf), Seq(cNameDefault, cConfDefault, tableConfDefault),
        Some(errTableConfInCommand)),
      (Map(tableConf), Seq(tableConfDefault), Some(errTableConfInCommand))
    )
  ) { case (
      propertyOverrides: Map[String, String],
      defaultConfs: Seq[(String, String)],
      errorOpt: Option[DeltaIllegalArgumentException]) =>
    testValidationForCreateDeltaTableCommand(
      tableExists = false,
      propertyOverrides,
      defaultConfs,
      errorOpt)
  }

  gridTest("During CLONE, CoordinatedCommitsUtils.validateCoordinatedCommitsConfigurationsImpl " +
      "passes for new target tables with legal explicit Coordinated Commits Configurations.") (
    Seq(
      // Having exactly Coordinator Name and Coordinator Conf explicitly, but having an illegal
      // combination of Coordinated Commits configurations in default: pass.
      // This is because we don't consider default configurations when explicit ones are provided.
      Seq.empty,
      Seq(cNameDefault),
      Seq(cNameDefault, cConfDefault),
      Seq(cNameDefault, cConfDefault, tableConfDefault),
      Seq(tableConfDefault)
    )
  ) { defaultConfs: Seq[(String, String)] =>
    testValidationForCreateDeltaTableCommand(
      tableExists = false,
      propertyOverrides = Map(cName, cConf),
      defaultConfs,
      errorOpt = None)
  }

  private def testValidateConfigurationsForAlterTableSetPropertiesDeltaCommand(
      existingConfs: Map[String, String],
      propertyOverrides: Map[String, String],
      errorOpt: Option[DeltaIllegalArgumentException]): Unit = {
    if (errorOpt.isDefined) {
      val e = intercept[DeltaIllegalArgumentException] {
        CoordinatedCommitsUtils.validateConfigurationsForAlterTableSetPropertiesDeltaCommand(
          existingConfs, propertyOverrides)
      }
      checkError(
        exception = e,
        condition = errorOpt.get.getErrorClass,
        sqlState = errorOpt.get.getSqlState,
        parameters = errorOpt.get.getMessageParameters.asScala.toMap)
    } else {
      CoordinatedCommitsUtils.validateConfigurationsForAlterTableSetPropertiesDeltaCommand(
        existingConfs, propertyOverrides)
    }
  }

  gridTest("During ALTER, `validateConfigurationsForAlterTableSetPropertiesDeltaCommand` " +
      "works correctly for tables without Coordinated Commits configurations.") {
    Seq(
      (Map.empty, None),
      (Map(cName), Some(new DeltaIllegalArgumentException(
        "DELTA_MUST_SET_ALL_COORDINATED_COMMITS_CONFS_IN_COMMAND", Array("ALTER", cConfKey)))),
      (Map(cName, cConf), None),
      (Map(cName, cConf, tableConf), Some(new DeltaIllegalArgumentException(
        "DELTA_CONF_OVERRIDE_NOT_SUPPORTED_IN_COMMAND", Array("ALTER", tableConfKey)))),
      (Map(tableConf), Some(new DeltaIllegalArgumentException(
        "DELTA_CONF_OVERRIDE_NOT_SUPPORTED_IN_COMMAND", Array("ALTER", tableConfKey))))
    )
  } { case (
      propertyOverrides: Map[String, String],
      errorOpt: Option[DeltaIllegalArgumentException]) =>
    testValidateConfigurationsForAlterTableSetPropertiesDeltaCommand(
      existingConfs = Map.empty,
      propertyOverrides,
      errorOpt)
  }

  test("During ALTER, `validateConfigurationsForAlterTableSetPropertiesDeltaCommand` " +
    "passes with no overrides for tables with Coordinated Commits configurations.") {
    testValidateConfigurationsForAlterTableSetPropertiesDeltaCommand(
      existingConfs = Map(cName, cConf, tableConf),
      propertyOverrides = Map.empty,
      errorOpt = None)
  }

  gridTest("During ALTER, `validateConfigurationsForAlterTableSetPropertiesDeltaCommand` " +
    "fails with overrides for tables with Coordinated Commits configurations.") (
    Seq(
      Map(cName),
      Map(cName, cConf),
      Map(cName, cConf, tableConf),
      Map(tableConf)
    )
  ) { propertyOverrides: Map[String, String] =>
    testValidateConfigurationsForAlterTableSetPropertiesDeltaCommand(
      existingConfs = Map(cName, cConf, tableConf),
      propertyOverrides,
      errorOpt = Some(new DeltaIllegalArgumentException(
        "DELTA_CANNOT_OVERRIDE_COORDINATED_COMMITS_CONFS", Array("ALTER"))))
  }

  private def errCannotUnset = new DeltaIllegalArgumentException(
    "DELTA_CANNOT_UNSET_COORDINATED_COMMITS_CONFS", Array.empty)

  private def testValidateConfigurationsForAlterTableUnsetPropertiesDeltaCommand(
      existingConfs: Map[String, String],
      propKeysToUnset: Seq[String],
      errorOpt: Option[DeltaIllegalArgumentException]): Unit = {
    if (errorOpt.isDefined) {
      val e = intercept[DeltaIllegalArgumentException] {
        CoordinatedCommitsUtils.validateConfigurationsForAlterTableUnsetPropertiesDeltaCommand(
          existingConfs, propKeysToUnset)
      }
      checkError(
        exception = e,
        condition = errorOpt.get.getErrorClass,
        sqlState = errorOpt.get.getSqlState,
        parameters = errorOpt.get.getMessageParameters.asScala.toMap)
    } else {
      CoordinatedCommitsUtils.validateConfigurationsForAlterTableUnsetPropertiesDeltaCommand(
        existingConfs, propKeysToUnset)
    }
  }

  gridTest("During ALTER, `validateConfigurationsForAlterTableUnsetPropertiesDeltaCommand` " +
    "fails with overrides for tables with Coordinated Commits configurations.") {
    Seq(
      Seq(cNameKey),
      Seq(cNameKey, cConfKey),
      Seq(cNameKey, cConfKey, tableConfKey),
      Seq(tableConfKey)
    )
  } { propKeysToUnset: Seq[String] =>
    testValidateConfigurationsForAlterTableUnsetPropertiesDeltaCommand(
      existingConfs = Map(cName, cConf, tableConf),
      propKeysToUnset,
      errorOpt = Some(errCannotUnset))
  }

  gridTest("During ALTER, `validateConfigurationsForAlterTableUnsetPropertiesDeltaCommand` " +
    "passes with no overrides for tables with or without Coordinated Commits configurations.") {
    Seq(
      Map.empty,
      Map(cName, cConf, tableConf)
    )
  } { case existingConfs: Map[String, String] =>
    testValidateConfigurationsForAlterTableUnsetPropertiesDeltaCommand(
      existingConfs,
      propKeysToUnset = Seq.empty,
      errorOpt = None)
  }

  gridTest("During ALTER, `validateConfigurationsForAlterTableUnsetPropertiesDeltaCommand` " +
    "passes with overrides for tables without Coordinated Commits configurations.") {
    Seq(
      Seq(cNameKey),
      Seq(cNameKey, cConfKey),
      Seq(cNameKey, cConfKey, tableConfKey),
      Seq(tableConfKey)
    )
  } { propKeysToUnset: Seq[String] =>
    testValidateConfigurationsForAlterTableUnsetPropertiesDeltaCommand(
      existingConfs = Map.empty,
      propKeysToUnset,
      errorOpt = None)
  }

  /////////////////////////////////////////////////////////////////////////////////////////////
  //      Test CoordinatedCommitsUtils.validateCoordinatedCommitsConfigurationsImpl ENDS     //
  /////////////////////////////////////////////////////////////////////////////////////////////
}
