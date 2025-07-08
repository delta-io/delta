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

/**
 * Represents a configuration trait that changes how the tests are executed. This can include Spark
 * configs, overrides, test excludes, and more.
 * @param name the name of the dimension.
 * @param values the possible values for this dimension, which when prepended with the name should
 * equal to the desired trait name that needs to be mixed in to generated suites.
 */
abstract class Dimension(val name: String, val values: Seq[String]) {
  /**
   * Creates a sequence of all dimension combinations where each combination contains this dimension
   * and at most one of the provided dimensions.
   */
  def selfAndCombineWithOneOf(dimensions: Dimension*): Seq[Seq[Dimension]] = {
    Seq(this) +: dimensions.map { dimension =>
      Seq(this, dimension)
    }
  }
}

/**
 * A default [[Dimension]] implementation for dimensions with multiple possible values, such as
 * column mapping
 */
case class DimensionWithMultipleValues(
    override val name: String,
    override val values: Seq[String]
) extends Dimension(name, values)

/**
 * A specialized [[Dimension]] that does not have any values, it is either present or not.
 */
case class DimensionMixin(
    override val name: String,
    suffix: String = "Mixin"
) extends Dimension(name, Seq(suffix)) {
  lazy val traitName: String = name + suffix
}

/**
 * Main configuration class for the suite generator. It allows defining a set of base suites and the
 * dimension combinations that should be used to generate the test configurations. Suites are
 * generated for each base suite and for each value combination of the dimension combinations.
 * @param baseSuites a sequence of base class or trait names that contains the actual test cases.
 * Ideally, these should not contain any configuration logic, and instead rely on [[Dimension]]s to
 * make the necessary setup.
 */
case class TestConfig(
    baseSuites: Seq[String],
    dimensionCombinations: Seq[Seq[Dimension]] = Seq.empty
)

object SuiteGeneratorConfig {
  private object Dims {
    val PATH_BASED = DimensionMixin("DeltaDMLTestUtils", suffix = "PathBased")
    val NAME_BASED = DimensionMixin("DeltaDMLTestUtils", suffix = "NameBased")
    val MERGE_SQL = DimensionMixin("MergeIntoSQL")
    val MERGE_SCALA = DimensionMixin("MergeIntoScala")
    val MERGE_CDC = DimensionMixin("MergeCDC")
    val MERGE_DVS = DimensionMixin("MergeIntoDVs")
    val MERGE_CDC_DVS = DimensionMixin("MergeCDCWithDVs")
    val MERGE_DVS_OVERRIDES = DimensionMixin("MergeIntoDVs", suffix = "Overrides")
    val MERGE_DVS_PREDPUSH = DimensionMixin("MergeIntoDVsWithPredicatePushdown")
    val CDC = DimensionMixin("CDC", suffix = "Enabled")
    val COLUMN_MAPPING = DimensionWithMultipleValues(
      "DeltaColumnMapping", Seq("EnableIdMode", "EnableNameMode"))
    val MERGE_SQL_COLMAP = DimensionMixin("MergeIntoSQLColumnMapping", suffix = "Overrides")
  }

  private object Tests {
    val MERGE_BASE = Seq(
      "MergeIntoBasicTests",
      "MergeIntoTempViewsTests",
      "MergeIntoNestedDataTests",
      "MergeIntoUnlimitedMergeClausesTests",
      "MergeIntoSuiteBaseMiscTests",
      "MergeIntoNotMatchedBySourceSuite",
      "MergeIntoSchemaEvolutionAllTests"
    )
    val MERGE_SQL = Seq(
      "MergeIntoSQLTests",
      "MergeIntoSQLNondeterministicOrderTests"
    )
  }

  /**
   * All fileName, [[TestConfig]] list groupings. The generated suites of each group will be written
   * to a file named after the group name.
   */
  // scalastyle:off line.size.limit
  lazy val GROUPS_WITH_TEST_CONFIGS: Seq[(String, Seq[TestConfig])] = Seq(
    "GeneratedSuites" -> Seq(
      TestConfig(
        // Exclude tempViews, because DeltaTable.forName does not resolve them correctly, so no one
        // can use them anyway with the Scala API.
        "MergeIntoScalaTests" +: Tests.MERGE_BASE.filterNot(_ == "MergeIntoTempViewsTests"),
        Seq(
          Seq(Dims.MERGE_SCALA)
        )
      ),
      TestConfig(
        Tests.MERGE_SQL ++: Tests.MERGE_BASE,
        Seq(
          Seq(Dims.MERGE_SQL, Dims.PATH_BASED),
          Seq(Dims.MERGE_SQL, Dims.NAME_BASED),
          Seq(Dims.MERGE_SQL, Dims.PATH_BASED, Dims.COLUMN_MAPPING, Dims.MERGE_SQL_COLMAP)
        )
      ),
      TestConfig(
        "MergeIntoDVsTests" +: Tests.MERGE_SQL ++: Tests.MERGE_BASE,
        Seq(
          Seq(Dims.MERGE_SQL, Dims.PATH_BASED, Dims.MERGE_DVS, Dims.MERGE_DVS_OVERRIDES),
          Seq(Dims.MERGE_SQL, Dims.PATH_BASED, Dims.MERGE_DVS_PREDPUSH)
        )
      ),
      TestConfig(
        "MergeCDCTests" +: "MergeIntoDVsTests" +: Tests.MERGE_SQL ++: Tests.MERGE_BASE,
        Seq(
          Seq(Dims.MERGE_SQL, Dims.CDC, Dims.MERGE_CDC, Dims.MERGE_DVS, Dims.MERGE_CDC_DVS),
          Seq(Dims.MERGE_SQL, Dims.CDC, Dims.MERGE_CDC, Dims.MERGE_DVS_PREDPUSH, Dims.MERGE_CDC_DVS)
        )
      ),
      TestConfig(
        "MergeCDCTests" +: Tests.MERGE_SQL ++: Tests.MERGE_BASE,
        Seq(
          Seq(Dims.MERGE_SQL, Dims.CDC, Dims.MERGE_CDC)
        )
      )
    )
  )
  // scalastyle:on line.size.limit

  /**
   * Used to add custom traits to some combinations of base suites and dimensions.
   * @return all traits that needs to be extended for this test combination (incl. provided mixins).
   */
  def applyCustomRulesAndGetAllMixins(base: String, mixins: Seq[String]): Seq[String] = {
    mixins
  }
}
