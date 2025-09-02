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
import scala.meta._

/**
 * Represents a configuration trait that changes how the tests are executed. This can include Spark
 * configs, overrides, test excludes, and more.
 * @param name the name of the dimension.
 * @param values the possible values for this dimension, which when prepended with the name should
 * equal to the desired trait name that needs to be mixed in to generated suites.
 * @param alias an optional short alias to be used when naming suites instead of the [[name]].
 */
abstract class Dimension(val name: String, val values: List[String], val alias: Option[String]) {
  /**
   * All trait names for this dimension
   */
  lazy val traitNames: List[String] = values.map(value => name + value)

  lazy val traitsWithAliases: List[(String, String)] = values
    .map(value => (
      name + value,
      alias.getOrElse(name) + value.replace("Enabled", "On").replace("Disabled", "Off")
    ))

  val isOptional: Boolean = false

  /**
   * same [[Dimension]] with an additional state of not being added to the suite.
   */
  def asOptional: Dimension = new OptionalDimension(name, values)

  private class OptionalDimension(
      override val name: String,
      override val values: List[String]
  ) extends Dimension(name, values, alias) {
    override val isOptional: Boolean = true
    override def asOptional: Dimension = this
  }

  // A bit of DSL for better readability of the test configs.
  def and(other: Dimension): List[Dimension] = this :: other :: Nil
  def and(others: List[Dimension]): List[Dimension] = this :: others
  def alone: List[Dimension] = this :: Nil
}

/**
 * A default [[Dimension]] implementation for dimensions with multiple possible values, such as
 * column mapping
 */
case class DimensionWithMultipleValues(
    override val name: String,
    override val values: List[String],
    override val alias: Option[String] = None
) extends Dimension(name, values, alias) {
  /**
   * Shortcut to create a [[DimensionMixin]] with the same name and one of the values as the suffix.
   * @param valueSelector a functions that selects a value from this dimension's values
   */
  def withValueAsDimension(valueSelector: List[String] => String): DimensionMixin = {
    DimensionMixin(name, valueSelector(values), alias)
  }
}

/**
 * A specialized [[Dimension]] that does not have any values, it is either present or not.
 */
case class DimensionMixin(
    override val name: String,
    suffix: String = "Mixin",
    override val alias: Option[String] = None
) extends Dimension(name, List(suffix), alias) {
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

/**
 * Represents a generated Scala file with suite definitions.
 * @param name the name of the generated file.
 * @param imports a list of packages that needs to be imported in this file.
 * @param testConfigs a list of [[TestConfig]]s that should be generated in this file.
 */
case class TestGroup(
    name: String,
    imports: List[Importer],
    testConfigs: List[TestConfig]
)

object SuiteGeneratorConfig {
  private object Dims {
    // Just to improve readability of the test configurations a bit.
    // `Dims.NONE` is clearer than just `Nil`.
    val NONE: List[Dimension] = Nil

    val TABLE_ACCESS = DimensionWithMultipleValues( // no alias needed, value is self-explanatory
      "DeltaDMLTestUtils", List("NameBased", "PathBased"), alias = Some(""))
    val PATH_BASED = TABLE_ACCESS.withValueAsDimension(_.last)
    val NAME_BASED = TABLE_ACCESS.withValueAsDimension(_.head)
    val MERGE_SQL = DimensionMixin("MergeIntoSQL", alias = Some("SQL"))
    val MERGE_SCALA = DimensionMixin("MergeIntoScala", alias = Some("Scala"))
    val MERGE_DVS = DimensionMixin("MergeIntoDVs", alias = Some("DVs"))
    val PREDPUSH = DimensionWithMultipleValues(
      "PredicatePushdown", List("Disabled", "Enabled"), alias = Some("PredPush"))
    val CDC = DimensionMixin("CDC", suffix = "Enabled")
    // These enables/disable DVs on new tables, but leave DML command configs untouched.
    val PERSISTENT_DV = DimensionWithMultipleValues(
      "PersistentDV", List("Disabled", "Enabled"), alias = Some("DV"))
    val PERSISTENT_DV_OFF = PERSISTENT_DV.withValueAsDimension(_.head)
    val PERSISTENT_DV_ON = PERSISTENT_DV.withValueAsDimension(_.last)
    val ROW_TRACKING = DimensionWithMultipleValues("RowTracking", List("Disabled", "Enabled"))
    val ROW_TRACKING_ON = ROW_TRACKING.withValueAsDimension(_.last)
    val MERGE_PERSISTENT_DV_OFF = DimensionMixin("MergePersistentDV", suffix = "Disabled")
    val MERGE_ROW_TRACKING_DV = DimensionMixin("RowTrackingMergeDV")
    val COLUMN_MAPPING = DimensionWithMultipleValues(
      "DeltaColumnMappingEnable", List("IdMode", "NameMode"), alias = Some("ColMap"))
    val UPDATE_SCALA = DimensionMixin("UpdateScala", alias = Some("Scala"))
    val UPDATE_SQL = DimensionMixin("UpdateSQL", alias = Some("SQL"))
    val UPDATE_DVS = DimensionMixin("UpdateSQLWithDeletionVectors", alias = Some("DV"))
    val UPDATE_ROW_TRACKING_DV = DimensionMixin("RowTrackingUpdateDV")
    val DELETE_SCALA = DimensionMixin("DeleteScala", alias = Some("Scala"))
    val DELETE_SQL = DimensionMixin("DeleteSQL", alias = Some("SQL"))
    val DELETE_WITH_DVS = DimensionMixin("DeleteSQLWithDeletionVectors", alias = Some("DV"))
  }

  private object Tests {
    val MERGE_BASE = List(
      "MergeIntoBasicTests",
      "MergeIntoTempViewsTests",
      "MergeIntoNestedDataTests",
      "MergeIntoUnlimitedMergeClausesTests",
      "MergeIntoAnalysisExceptionTests",
      "MergeIntoExtendedSyntaxTests",
      "MergeIntoSuiteBaseMiscTests",
      "MergeIntoNotMatchedBySourceSuite",
      "MergeIntoNotMatchedBySourceCDCPart1Tests",
      "MergeIntoNotMatchedBySourceCDCPart2Tests",
      "MergeIntoSchemaEvolutionCoreTests",
      "MergeIntoSchemaEvolutionBaseTests",
      "MergeIntoSchemaEvolutionStoreAssignmentPolicyTests",
      "MergeIntoSchemaEvolutionNotMatchedBySourceTests",
      "MergeIntoNestedStructInMapEvolutionTests",
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

  implicit class DimensionListExt(val dims: List[Dimension]) {
    /**
     * @return a new list of dimension combinations where each combination has the
     * [[commonDims]] prepended to it.
     */
    def prependToAll(dimensionCombinations: List[Dimension]*): List[List[Dimension]] = {
      dimensionCombinations.toList.map(dims ::: _)
    }

    // Continued DSL from the Dimension class above to work around the different
    // operator precedence between :: and `and`.
    def and(other: Dimension): List[Dimension] = dims ::: other :: Nil
    def and(others: List[Dimension]): List[Dimension] = dims ::: others
  }

  /**
   * All [[TestGroup]] definitions. The generated suites of each group will be written
   * to a file named after the group name. Keep in mind that [[isExcluded]] can be used to filter
   * out some of the test configurations, so defining a configuration here does not guarantee
   * generation of a suite for it.
   */
  lazy val TEST_GROUPS: List[TestGroup] = List(
    // scalastyle:off line.size.limit
    TestGroup(
      name = "MergeSuites",
      imports = List(
        importer"org.apache.spark.sql.delta._",
        importer"org.apache.spark.sql.delta.cdc._",
        importer"org.apache.spark.sql.delta.rowid._"
      ),
      testConfigs = List(
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
          List("MergeIntoMaterializeSourceTests", "MergeIntoMaterializeSourceErrorTests"),
          List(
            List(Dims.MERGE_PERSISTENT_DV_OFF)
          )
        ),
        TestConfig(
          List("RowTrackingMergeCommonTests"),
          List(Dims.NAME_BASED, Dims.CDC.asOptional).prependToAll(
            List(Dims.MERGE_ROW_TRACKING_DV.asOptional),
            List(Dims.PERSISTENT_DV_OFF, Dims.MERGE_PERSISTENT_DV_OFF)
          ) :::
          List(Dims.NAME_BASED, Dims.COLUMN_MAPPING).prependToAll(
            List(),
            List(Dims.CDC, Dims.MERGE_ROW_TRACKING_DV)
          )
        )
      )
    ),
    TestGroup(
      name = "UpdateSuites",
      imports = List(
        importer"org.apache.spark.sql.delta._",
        importer"org.apache.spark.sql.delta.cdc._",
        importer"org.apache.spark.sql.delta.rowid._",
        importer"org.apache.spark.sql.delta.rowtracking._"
      ),
      testConfigs = List(
        TestConfig(
          "UpdateScalaTests" :: Tests.UPDATE_BASE,
          List(
            List(Dims.UPDATE_SCALA)
          )
        ),
        TestConfig(
          "UpdateSQLTests" :: Tests.UPDATE_BASE,
          List(
            List(Dims.UPDATE_SQL, Dims.NAME_BASED)
          )
        ),
        TestConfig(
          "UpdateCDCWithDeletionVectorsTests" ::
            "UpdateCDCTests" ::
            "UpdateSQLWithDeletionVectorsTests" ::
            "UpdateSQLTests" ::
            Tests.UPDATE_BASE,
          List(
            List(Dims.UPDATE_SQL, Dims.PATH_BASED, Dims.CDC.asOptional, Dims.ROW_TRACKING.asOptional),
            List(Dims.UPDATE_SQL, Dims.PATH_BASED, Dims.CDC, Dims.UPDATE_DVS),
            List(Dims.UPDATE_SQL, Dims.PATH_BASED, Dims.UPDATE_DVS, Dims.PREDPUSH)
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
      )
    ),
    TestGroup(
      name = "DeleteSuites",
      imports = List(
        importer"org.apache.spark.sql.delta._",
        importer"org.apache.spark.sql.delta.cdc._",
        importer"org.apache.spark.sql.delta.rowid._"
      ),
      testConfigs = List(
        TestConfig(
          "DeleteScalaTests" :: Tests.DELETE_BASE,
          List(
            List(Dims.DELETE_SCALA)
          )
        ),
        TestConfig(
          "DeleteCDCTests" :: "DeleteSQLTests" :: Tests.DELETE_BASE,
          List(
            List(Dims.DELETE_SQL, Dims.NAME_BASED),
            List(Dims.DELETE_SQL, Dims.PATH_BASED, Dims.COLUMN_MAPPING.asOptional),
            List(Dims.DELETE_SQL, Dims.PATH_BASED, Dims.DELETE_WITH_DVS, Dims.PREDPUSH),
            List(Dims.DELETE_SQL, Dims.PATH_BASED, Dims.CDC)
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
    ),
    )
    // scalastyle:on line.size.limit
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
