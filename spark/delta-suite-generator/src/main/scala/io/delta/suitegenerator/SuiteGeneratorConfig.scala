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

package io.delta.suitegenerator

import scala.collection.mutable.ListBuffer

/**
 * Represents a configuration trait that changes how the tests are executed. This can include Spark
 * configs, overrides, test excludes, and more.
 * @param name the name of the dimension.
 * @param values the possible values for this dimension, which when prepended with the name should
 * equal to the desired trait name that needs to be mixed in to generated suites.
 */
abstract class Dimension(val name: String, val values: List[String]) {
  /**
   * All trait names for this dimension
   */
  lazy val traitNames: List[String] = values.map(value => name + value)

  val isOptional: Boolean = false

  /**
   * same [[Dimension]] with an additional state of not being added to the suite.
   */
  def asOptional: Dimension = new OptionalDimension(name, values)

  private class OptionalDimension(
    override val name: String,
    override val values: List[String]
  ) extends Dimension(name, values) {
    override val isOptional: Boolean = true
    override def asOptional: Dimension = this
  }
}

/**
 * A default [[Dimension]] implementation for dimensions with multiple possible values, such as
 * column mapping
 */
case class DimensionWithMultipleValues(
    override val name: String,
    override val values: List[String]
) extends Dimension(name, values)

/**
 * A specialized [[Dimension]] that does not have any values, it is either present or not.
 */
case class DimensionMixin(
    override val name: String,
    suffix: String = "Mixin"
) extends Dimension(name, List(suffix)) {
  lazy val traitName: String = name + suffix
}

/**
 * Main configuration class for the suite generator. It allows defining a set of base suites and the
 * dimension combinations that should be used to generate the test configurations. Suites are
 * generated for each base suite and for each value combination of the dimension combinations.
 * @param baseSuites a list of base class or trait names that contains the actual test cases.
 * Ideally, these should not contain any configuration logic, and instead rely on [[Dimension]]s to
 * make the necessary setup.
 */
case class TestConfig(
    baseSuites: List[String],
    dimensionCombinations: List[List[Dimension]] = List.empty
)

object SuiteGeneratorConfig {
  private object Dims {
    val PATH_BASED = DimensionMixin("DeltaDMLTestUtils", suffix = "PathBased")
    val NAME_BASED = DimensionMixin("DeltaDMLTestUtils", suffix = "NameBased")
    val MERGE_SQL = DimensionMixin("MergeIntoSQL")
    val MERGE_SCALA = DimensionMixin("MergeIntoScala")
    val MERGE_DVS = DimensionMixin("MergeIntoDVs")
    val PREDPUSH = DimensionWithMultipleValues("PredicatePushdown", List("Disabled", "Enabled"))
    val CDC = DimensionMixin("CDC", suffix = "Enabled")
    val PERSISTENT_DV = DimensionWithMultipleValues("PersistentDV", List("Disabled", "Enabled"))
    val PERSISTENT_DV_OFF = DimensionMixin(PERSISTENT_DV.name, suffix = PERSISTENT_DV.values.head)
    val PERSISTENT_DV_ON = DimensionMixin(PERSISTENT_DV.name, suffix = PERSISTENT_DV.values.last)
    val ROW_TRACKING = DimensionWithMultipleValues("RowTracking", List("Disabled", "Enabled"))
    val MERGE_PERSISTENT_DV_OFF = DimensionMixin("MergePersistentDV", suffix = "Disabled")
    val MERGE_ROW_TRACKING_DV = DimensionMixin("RowTrackingMergeDV")
    val COLUMN_MAPPING = DimensionWithMultipleValues(
      "DeltaColumnMapping", List("EnableIdMode", "EnableNameMode"))
    val UPDATE_SCALA = DimensionMixin("UpdateScala")
    val UPDATE_SQL = DimensionMixin("UpdateSQL")
    val UPDATE_DVS = DimensionMixin("UpdateSQLWithDeletionVectors")
    val UPDATE_ROW_TRACKING_DV = DimensionMixin("RowTrackingUpdateDV")
    val DELETE_SCALA = DimensionMixin("DeleteScala")
    val DELETE_SQL = DimensionMixin("DeleteSQL")
    val DELETE_WITH_DVS = DimensionMixin("DeleteSQLWithDeletionVectors")
  }

  private object Tests {
    val MERGE_BASE = List(
      "MergeIntoBasicTests",
      "MergeIntoTempViewsTests",
      "MergeIntoNestedDataTests",
      "MergeIntoUnlimitedMergeClausesTests",
      "MergeIntoSuiteBaseMiscTests",
      "MergeIntoNotMatchedBySourceSuite",
      "MergeIntoSchemaEvolutionCoreTests",
      "MergeIntoSchemaEvolutionBaseTests",
      "MergeIntoSchemaEvolutionNotMatchedBySourceTests",
      "MergeIntoNestedStructEvolutionTests"
    )
    val MERGE_SQL = List(
      "MergeIntoSQLTests",
      "MergeIntoSQLNondeterministicOrderTests"
    )
    val UPDATE_BASE = List(
      "UpdateBaseTempViewTests",
      "UpdateBaseMiscTests"
    )
    val DELETE_BASE = List(
      "DeleteTempViewTests",
      "DeleteBaseTests"
    )
  }


  implicit class DimensionListExt(val commonDims: List[Dimension]) extends AnyVal {
    /**
     * @return a new list of dimension combinations where each combination has the
     * [[commonDims]] prepended to it.
     */
    def prependToAll(dimensionCombinations: List[Dimension]*): List[List[Dimension]] = {
      dimensionCombinations.toList.map(commonDims ::: _)
    }
  }

  /**
   * All fileName, [[TestConfig]] list groupings. The generated suites of each group will be written
   * to a file named after the group name. Keep in mind that [[isExcluded]] can be used to filter
   * out some of the test configurations, so defining a configuration here does not guarantee
   * generation of a suite for it.
   */
  lazy val GROUPS_WITH_TEST_CONFIGS: List[(String, List[TestConfig])] = List(
    "MergeSuites" -> List(
      TestConfig(
        "MergeIntoScalaTests" :: Tests.MERGE_BASE,
        List(
          List(Dims.MERGE_SCALA)
        )
      ),
      TestConfig(
        "MergeCDCTests" :: "MergeIntoDVsTests" :: Tests.MERGE_SQL ::: Tests.MERGE_BASE,
        List(Dims.MERGE_SQL).prependToAll(
          List(Dims.NAME_BASED),
          List(Dims.PATH_BASED, Dims.COLUMN_MAPPING.asOptional),
          List(Dims.PATH_BASED, Dims.MERGE_DVS, Dims.PREDPUSH),
          List(Dims.PATH_BASED, Dims.CDC),
          List(Dims.PATH_BASED, Dims.CDC, Dims.MERGE_DVS, Dims.PREDPUSH)
        )
      ),
      TestConfig(
        List("MergeIntoMaterializeSourceTests"),
        List(
          List(Dims.MERGE_PERSISTENT_DV_OFF)
        )
      ),
      TestConfig(
        List("RowTrackingMergeCommonTests"),
        List(Dims.CDC.asOptional).prependToAll(
          List(Dims.MERGE_ROW_TRACKING_DV.asOptional),
          List(Dims.PERSISTENT_DV_OFF, Dims.MERGE_PERSISTENT_DV_OFF)
        ) :::
        List(Dims.COLUMN_MAPPING).prependToAll(
          List(),
          List(Dims.CDC, Dims.MERGE_ROW_TRACKING_DV)
        )
      )
    ),
    "UpdateSuites" -> List(
      TestConfig(
        "UpdateScalaTests" :: Tests.UPDATE_BASE,
        List(
          List(Dims.UPDATE_SCALA)
        )
      ),
      TestConfig(
        "UpdateCDCWithDeletionVectorsTests" ::
          "UpdateCDCTests" ::
          "UpdateSQLWithDeletionVectorsTests" ::
          "UpdateSQLTests" ::
          Tests.UPDATE_BASE,
        List(
          List(Dims.UPDATE_SQL, Dims.CDC.asOptional, Dims.ROW_TRACKING.asOptional),
          List(Dims.UPDATE_SQL, Dims.CDC, Dims.UPDATE_DVS),
          List(Dims.UPDATE_SQL, Dims.UPDATE_DVS, Dims.PREDPUSH)
        )
      ),
      TestConfig(
        List("RowTrackingUpdateCommonTests"),
        List(
          List(Dims.CDC.asOptional, Dims.COLUMN_MAPPING.asOptional),
          List(Dims.UPDATE_ROW_TRACKING_DV),
          List(Dims.UPDATE_ROW_TRACKING_DV, Dims.CDC, Dims.COLUMN_MAPPING.asOptional)
        )
      )
    ),
    "DeleteSuites" -> List(
      TestConfig(
        "DeleteScalaTests" :: Tests.DELETE_BASE,
        List(
          List(Dims.DELETE_SCALA)
        )
      ),
      TestConfig(
        "DeleteCDCTests" :: "DeleteSQLTests" :: Tests.DELETE_BASE,
        List(
          List(Dims.DELETE_SQL, Dims.COLUMN_MAPPING.asOptional),
          List(Dims.DELETE_SQL, Dims.DELETE_WITH_DVS, Dims.PREDPUSH),
          List(Dims.DELETE_SQL, Dims.CDC)
        )
      ),
      TestConfig(
        List("RowTrackingDeleteSuiteBase", "RowTrackingDeleteDvBase"),
        List(
          List(Dims.CDC.asOptional, Dims.PERSISTENT_DV),
          List(Dims.PERSISTENT_DV_OFF, Dims.COLUMN_MAPPING),
          List(Dims.CDC, Dims.PERSISTENT_DV_ON, Dims.COLUMN_MAPPING)
        )
      )
    )
  )

  /**
   * Decides if a suite with the given base test and mixins should be generated or not. This is used
   * to exclude certain combinations of base suites and dimensions that are known to not work
   * together, or it can also be used to enforce presence of some dimensions for a certain base
   * suite.
   */
  def isExcluded(base: String, mixins: List[String]): Boolean = {
    base match {
      // Exclude tempViews, because DeltaTable.forName does not resolve them correctly, so no one
      // can use them anyway with the Scala API.
      case "MergeIntoTempViewsTests" => mixins.contains(Dims.MERGE_SCALA.traitName)
      case "UpdateBaseTempViewTests" => mixins.contains(Dims.UPDATE_SCALA.traitName)
      case "DeleteTempViewTests" => mixins.contains(Dims.DELETE_SCALA.traitName)
      // The following tests only make sense if the dimension is present
      case "MergeCDCTests" | "UpdateCDCTests" | "DeleteCDCTests" =>
        !mixins.contains(Dims.CDC.traitName)
      case "MergeIntoDVsTests" => !mixins.contains(Dims.MERGE_DVS.traitName)
      case "UpdateSQLWithDeletionVectorsTests" =>
        !mixins.contains(Dims.UPDATE_DVS.traitName)
      case "UpdateCDCWithDeletionVectorsTests" =>
        !List(Dims.UPDATE_DVS, Dims.CDC).map(_.traitName).forall(mixins.contains)
      case "RowTrackingDeleteDvBase" => !mixins.contains(Dims.PERSISTENT_DV_ON.traitName)
      case _ => false
    }
  }

  /**
   * Used to add custom traits to some combinations of base suites and dimensions.
   * @return all traits that needs to be extended for this test combination (incl. provided mixins).
   */
  def applyCustomRulesAndGetAllMixins(base: String, mixins: List[String]): List[String] = {
    var finalMixins = new ListBuffer[String]
    finalMixins ++= mixins

    if (mixins.contains(Dims.MERGE_SQL.traitName)) {
      if (Dims.COLUMN_MAPPING.traitNames.exists(mixins.contains)) {
        finalMixins += "MergeIntoSQLColumnMappingOverrides"
      }

      if (mixins.contains(Dims.CDC.traitName)) {
        finalMixins += "MergeCDCMixin"
        if (mixins.contains(Dims.MERGE_DVS.traitName)) {
          finalMixins += "MergeCDCWithDVsMixin"
        }
      }
    }

    if (mixins.contains(Dims.UPDATE_SQL.traitName)) {
      if (mixins.contains(Dims.ROW_TRACKING.traitNames.last)) {
        finalMixins += "UpdateWithRowTrackingOverrides"
      }
    }

    if (mixins.contains(Dims.DELETE_SQL.traitName)) {
      if (mixins.contains(Dims.CDC.traitName)) {
        finalMixins += "DeleteCDCMixin"
      }
      if (mixins.contains(Dims.COLUMN_MAPPING.traitNames.last)) {
        finalMixins += "DeleteSQLNameColumnMappingMixin"
      }
    }

    finalMixins.result()
  }
}
