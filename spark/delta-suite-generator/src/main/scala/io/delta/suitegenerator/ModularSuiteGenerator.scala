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

import java.nio.file.{Files, Paths}

import scala.meta._
import scala.util.hashing.MurmurHash3

import org.apache.commons.cli.{DefaultParser, Option, Options}
import org.apache.commons.codec.binary.Base32

/**
 * The main generator for the Modular Delta Suites. Generated suite combinations can be configured
 * in [[SuiteGeneratorConfig]].
 *
 * Can be run via the sbt command: `deltaSuiteGenerator / run`
 */
object ModularSuiteGenerator {

  val GENERATED_PACKAGE = s"org.apache.spark.sql.delta.generatedsuites"

  lazy val OUTPUT_PATH: String = "spark/src/test/scala/" + GENERATED_PACKAGE.replace('.', '/')

  private val DEFAULT_REPO_PATH = "~/delta"

  private val CODE_LINE_LENGTH_CHAR_LIMIT = 100

  def main(args: Array[String]): Unit = {
    val suitesWriter = parseArgsAndGetWriter(args)
    generateSuites(suitesWriter)
  }

  def generateSuites(suitesWriter: SuitesWriter): Unit = {
    for ((group, testConfigs) <- SuiteGeneratorConfig.GROUPS_WITH_TEST_CONFIGS) {
      val suites = for {
        testConfig <- testConfigs
        baseSuite <- testConfig.baseSuites
        dimensions <- testConfig.dimensionCombinations
      } yield dimensions
        // Generate all combinations of dimension traits
        .foldLeft(List(List.empty[String])) {
          (acc, dimension) =>
            (if (dimension.isOptional) acc else List.empty) :::
            (for {
              accValue <- acc
              traitName <- dimension.traitNames
            } yield accValue :+ traitName)
        }
        .filterNot(dimensionTraits => SuiteGeneratorConfig.isExcluded(baseSuite, dimensionTraits))
        .map(dimensionTraits => generateCode(baseSuite, dimensionTraits))

      suitesWriter.writeGeneratedSuitesOfGroup(suites.flatten, group)
    }
    suitesWriter.conclude()
  }

  private lazy val OPT_REPO_PATH = new Option(
    /* option = */ "p",
    /* longOption = */ "repo-path",
    /* hasArg = */ true,
    /* description = */ s"Path to the repository root. Defaults to $DEFAULT_REPO_PATH")
  private lazy val OPTIONS = new Options().addOption(OPT_REPO_PATH)

  private def parseArgsAndGetWriter(args: Array[String]): SuitesWriter = {
    val cmd = new DefaultParser().parse(OPTIONS, args)
    var repoPath = cmd.getOptionValue(OPT_REPO_PATH, DEFAULT_REPO_PATH)

    // Expand `~` prefix to the user's home directory
    if (repoPath.startsWith("~")) {
      repoPath = System.getProperty("user.home") + repoPath.substring(1)
    }

    val outputPath = Paths.get(repoPath, OUTPUT_PATH)
    assert(
      Files.exists(outputPath.getParent),
      s"Repository could not be detected at $repoPath. Make sure to provide the " +
        s"repository path using the --${OPT_REPO_PATH.getLongOpt} option.")

    new SuitesWriter(outputPath)
  }

  private lazy val BASE32 = new Base32()

  private def generateCode(
    baseSuite: String,
    mixins: List[String]): TestSuite = {
    val allMixins = SuiteGeneratorConfig.applyCustomRulesAndGetAllMixins(baseSuite, mixins)
    val suiteParents = (baseSuite :: allMixins).map(_.parse[Init].get)

    // Generate suite name by combining the names of base suite, base mixins, and dimensions.
    // Only get the class name part if the mixin is a fully qualified name.
    // Remove "Suite" / "Mixin" substrings for better readability
    val baseSuitePrefix = baseSuite.stripSuffix("Suite")
    val mixinSuffix = mixins
      .map(_.split('.').last)
      .map(_.replace("Mixin", ""))
      .mkString("")
    var suiteName = baseSuitePrefix + mixinSuffix

    // Truncate the name and replace with a consistent hash if line becomes longer than the limit
    val maxSuiteNameLength = CODE_LINE_LENGTH_CHAR_LIMIT - "class Suite".length
    if (suiteName.length > maxSuiteNameLength) {
      val hashBytes = BigInt(MurmurHash3.stringHash(suiteName)).toByteArray
      val hashEncoded = BASE32.encodeToString(hashBytes).replace("=", "")
      suiteName = suiteName.substring(0, maxSuiteNameLength - hashEncoded.length) + hashEncoded
    }

    suiteName += "Suite"
    TestSuite(
      suiteName,
      q"""class ${Type.Name(suiteName)}
          extends ..$suiteParents""")
  }
}

case class TestSuite(
    name: String,
    classDefinition: Defn.Class
)
