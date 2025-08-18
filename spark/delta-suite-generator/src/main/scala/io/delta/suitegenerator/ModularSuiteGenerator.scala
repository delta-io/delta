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

import org.apache.commons.cli.{CommandLine, DefaultParser, HelpFormatter, Option, Options}
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

  /**
   * Controls when to start truncating and hashing the suite names to prevent extremely long names.
   */
  private val SUITE_NAME_CHAR_LIMIT = 255 - 136

  private lazy val OPT_REPO_PATH = new Option(
    /* option = */ "p",
    /* longOption = */ "repo-path",
    /* hasArg = */ true,
    /* description = */ s"Path to the repository root. Defaults to $DEFAULT_REPO_PATH")
  private lazy val OPT_HELP = new Option(
    /* option = */ "h",
    /* longOption = */ "help",
    /* hasArg = */ false,
    /* description = */ "Print help")
  private lazy val OPTIONS = new Options().addOption(OPT_REPO_PATH).addOption(OPT_HELP)

  def main(args: Array[String]): Unit = {
    val cmd = new DefaultParser().parse(OPTIONS, args)

    if (cmd.hasOption(OPT_HELP)) {
      val formatter = new HelpFormatter()
      formatter.printHelp(
        "bazel run //sql/core/delta_suite_generator:generate -- <options>",
        OPTIONS)
      System.exit(0)
    }

    val suitesWriter = getWriter(cmd)

    // scalastyle:off println
    println("Generating suites...")
    generateSuites(suitesWriter)
    println("Suite generation completed successfully.")
    // scalastyle:on println
  }

  def generateSuites(suitesWriter: SuitesWriter): Unit = {
    for (testGroup <- SuiteGeneratorConfig.TEST_GROUPS) {
      val suites = for {
        testConfig <- testGroup.testConfigs
        baseSuite <- testConfig.baseSuites
        dimensions <- testConfig.dimensionCombinations
      } yield dimensions
        // Generate all combinations of dimension traits
        .foldLeft(List(List.empty[(String, String)])) {
          (acc, dimension) =>
            (if (dimension.isOptional) acc else List.empty) :::
            (for {
              accValue <- acc
              traitWithAlias <- dimension.traitsWithAliases
            } yield accValue :+ traitWithAlias)
        }
        .filterNot(dimTraits => SuiteGeneratorConfig.isExcluded(baseSuite, dimTraits.map(_._1)))
        .map(dimTraits => generateCode(baseSuite, dimTraits))

      suitesWriter.writeGeneratedSuitesOfGroup(suites.flatten, testGroup)
    }
    suitesWriter.conclude()
  }

  private def getWriter(cmd: CommandLine): SuitesWriter = {
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

    // Prevent people with multiple repository copies/worktrees accidentally generating into
    // the wrong one.
    // We assume if it's specified explicitly, it's specified correctly, and we don't need to
    // double-check.
    if (!cmd.hasOption(OPT_REPO_PATH)) {
      // scalastyle:off println
      if (System.console() == null) {
        // This is not an interactive shell, we can't ask for input.
        println(
          s"""Verified that a matching repository exists at target.
             |Generation target path is: '${outputPath}'
             |The path can be customised with the --${OPT_REPO_PATH.getLongOpt} option."""
             .stripMargin)
      } else {
        println(
          s"""Verified that a matching repository exists at target.
             |Please double check the path: '${outputPath}'
             |The path can be customised with the --${OPT_REPO_PATH.getLongOpt} option.
             |If correct, press <enter> to generate or <ctrl>+c to abort.""".stripMargin)
        scala.io.StdIn.readLine()
      }
      // scalastyle:on println
    }

    new SuitesWriter(outputPath)
  }

  private lazy val BASE32 = new Base32()

  private def generateCode(
      baseSuite: String,
      mixinsAndAliases: List[(String, String)]): TestSuite = {
    val allMixins = SuiteGeneratorConfig
      .applyCustomRulesAndGetAllMixins(baseSuite, mixinsAndAliases.map(_._1))
    val suiteParents = (baseSuite :: allMixins).map(_.parse[Init].get)

    // Generate suite name by combining the names of base suite and dimension aliases.
    // Remove some redundant substrings for better readability
    val baseSuitePrefix = baseSuite.stripSuffix("Suite").stripSuffix("Tests")
    val mixinSuffix = mixinsAndAliases
      .map(_._2.replace("Mixin", ""))
      .mkString("")
    var suiteName = baseSuitePrefix + mixinSuffix

    // Truncate the name and replace with a consistent hash if line becomes longer than the limit
    val maxSuiteNameLength = SUITE_NAME_CHAR_LIMIT - "Suite".length
    if (suiteName.length > maxSuiteNameLength) {
      // scalastyle:off println
      println(s"WARNING: Suite name is too long, truncating and hashing to fit within the limit. " +
        s"Please consider renaming the base suite or defining shorter dimension aliases. " +
        s"Suite: $suiteName (${suiteName.length} characters > $maxSuiteNameLength limit).")
      // scalastyle:on println

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
