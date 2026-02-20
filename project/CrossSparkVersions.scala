import sbt._
import sbt.Keys._
import sbt.complete.DefaultParsers._
import com.simplytyped.Antlr4Plugin
import com.simplytyped.Antlr4Plugin.autoImport._
import sbtrelease.ReleasePlugin.autoImport.ReleaseStep
import Unidoc._

/** 
 * ========================================================
 * Cross-Spark Build and Publish System
 * ========================================================
 * 
 * This SBT plugin enables Delta Lake to be built and published for multiple Spark versions.
 * It provides version-specific configurations, artifact naming, and publishing workflows.
 *
 * ========================================================
 * Spark Version Definitions
 * ========================================================
 * 
 * The Spark versions used for Delta is defined in the SparkVersionSpec object, and controlled by the sparkVersion property.
 * There are 2 keys labels assigned to the Spark versions: DEFAULT and MASTER.
 * - DEFAULT VERSION: This is the default when no sparkVersion property is specified.
 *
 * - MASTER VERSION: The Spark master/development branch version
 *   This is optional and typically 
 *   - set in the Delta master branch to a Spark released or snapshot version .
 *   - not set in the Delta release branches as we want to avoid building against Spark unreleased version.
 *   If MASTER is defined, then it can be selected by setting the sparkVersion property to "master".
 *   Spark-dependent artifacts for this version HAVE a Spark version suffix in their artifact names (e.g., delta-spark_4.0_2.13 if MASTER is defined as Spark 4.0 branch).
 *
 * - OTHER VERSIONS: Any non-default Spark version specified in ALL_SPECS.
 *   Spark-dependent artifacts of all non-default versions get a Spark version suffix in their artifact names (e.g., delta-spark_4.1_2.13 if one of the other versions is defined as Spark 4.1 branch).
 *
 * To configure versions, update the SparkVersionSpec values (e.g., spark35, spark40, etc.) below.
 *
 * ========================================================
 * The sparkVersion Property
 * ========================================================
 * 
 * The sparkVersion system property controls which Spark version to build against.
 * It accepts the following formats:
 *
 * 1. Full version string (e.g., "3.5.7", "4.0.2-SNAPSHOT")
 * 2. Short version string (e.g., "3.5", "4.0")
 * 3. Aliases:
 *    - "default" -> maps to DEFAULT version (e.g., spark35)
 *    - "master" -> maps to MASTER version (e.g., spark40), if configured
 *
 * If not specified, it defaults to the DEFAULT version.
 *
 * Examples:
 *   build/sbt                                    # Uses default version
 *   build/sbt -DsparkVersion=4.0                 # Uses Spark 4.0.x
 *   build/sbt -DsparkVersion=4.0.1               # Uses Spark 4.0.1 only if this version is defined in ALL_SPECS
 *   build/sbt -DsparkVersion=4.1                 # Uses Spark 4.1.x whatever it is defined in ALL_SPECS
 *   build/sbt -DsparkVersion=default             # Uses default version
 *   build/sbt -DsparkVersion=master              # Uses master version (if defined)
 *
 * ========================================================
 * Cross-Building for Development and Testing
 * ========================================================
 * 
 * To build/test against a specific Spark version:
 *   build/sbt -DsparkVersion=<version> compile
 *   build/sbt -DsparkVersion=<version> test
 *   build/sbt -DsparkVersion=master compile test
 *
 * To publish to local Maven for testing:
 *   # Publish all modules for default Spark version
 *   build/sbt publishM2
 *
 *   # Publish only Spark-dependent modules for other versions
 *   build/sbt -DsparkVersion=master "runOnlyForReleasableSparkModules publishM2"
 *
 * ========================================================
 * Module Types
 * ========================================================
 * 
 * Modules are automatically classified based on their settings:
 *
 * 1. Spark-Dependent Published Modules:
 *    - Use CrossSparkVersions.sparkDependentSettings(sparkVersion)
 *    - Include releaseSettings (publishable)
 *    - Examples: delta-spark, delta-connect-*, delta-sharing-spark, delta-iceberg, delta-hudi, delta-contribs
 *    - These modules get version-specific artifact names for non-default Spark versions
 *    - Automatically included in cross-Spark publishing
 *
 * 2. Spark-Dependent Internal Modules:
 *    - Use CrossSparkVersions.sparkDependentSettings(sparkVersion)
 *    - Include skipReleaseSettings (not published)
 *    - Examples: sparkV1, sparkV2
 *    - These modules are built for each Spark version but not published
 *    - Automatically excluded from cross-Spark publishing
 *
 * 3. Spark-Independent Modules:
 *    - Do not use CrossSparkVersions settings
 *    - Examples: delta-storage, delta-kernel-*, delta-standalone
 *    - These modules are built once and work with all Spark versions
 *
 * ========================================================
 * Artifact Naming Convention of Spark-dependent modules
 * ========================================================
 * 
 * By default, Spark-dependent modules ALWAYS include the Spark version suffix:
 *   io.delta:delta-spark_4.0_2.13:4.1.0
 *   io.delta:delta-spark_4.1_2.13:4.1.0
 *   io.delta:delta-connect-server_4.0_2.13:4.1.0
 *   io.delta:delta-storage:4.1.0  (Spark-independent, no suffix)
 *
 * During release, backward-compatible artifacts are ALSO published (without suffix):
 *   io.delta:delta-spark_2.13:4.1.0       (backward compatibility)
 *   io.delta:delta-connect-server_2.13:4.1.0
 *
 * This means during release, Spark-dependent modules are published TWICE:
 *   - With suffix (e.g., delta-spark_4.1_2.13) - the default/normal name
 *   - Without suffix (e.g., delta-spark_2.13) - for backward compatibility
 *
 * ========================================================
 * Cross-Release Workflow
 * ========================================================
 * 
 * The cross-release workflow publishes artifacts for all Spark versions:
 *
 * Step 1: Publish ALL modules WITHOUT Spark suffix (backward compatibility)
 *   build/sbt -DskipSparkSuffix=true publishSigned
 *   # Publishes: delta-spark_2.13, delta-storage, delta-kernel-api, etc.
 *
 * Step 2: Publish Spark-dependent modules WITH suffix for each non-master Spark version
 *   build/sbt -DsparkVersion=4.0 "runOnlyForReleasableSparkModules publishSigned"
 *   build/sbt -DsparkVersion=4.1 "runOnlyForReleasableSparkModules publishSigned"
 *   # Publishes: delta-spark_4.0_2.13, delta-spark_4.1_2.13, etc.
 *
 * This workflow is automated via crossSparkReleaseSteps() in the release process.
 * See releaseProcess in build.sbt for integration.
 *
 * Why this approach?
 * - Default behavior always includes Spark suffix for clarity
 * - Release also publishes without suffix for backward compatibility
 * - Spark-independent modules (kernel, storage) are built once
 * - Spark-dependent modules are built for each Spark version
 *
 * For manual testing during development:
 *   build/sbt publishM2  # Publishes delta-spark_4.0_2.13 (default, with suffix)
 *
 * For manual release testing:
 *   build/sbt -DskipSparkSuffix=true publishM2  # Without suffix (backward compat)
 *   build/sbt -DsparkVersion=4.0 "runOnlyForReleasableSparkModules publishM2"
 *   build/sbt -DsparkVersion=4.1 "runOnlyForReleasableSparkModules publishM2"
 *   # Verify JARs in ~/.m2/repository/io/delta/
 *
 * ========================================================
 * Commands Provided
 * ========================================================
 * 
 * runOnlyForReleasableSparkModules <task>
 *   Runs the specified task only on publishable Spark-dependent modules.
 *   Automatically detects modules that:
 *   1. Have the sparkVersion setting (use Spark-aware configuration)
 *   2. Are publishable (publish/skip is not true)
 *
 *   Used for publishing Spark-dependent modules for non-default Spark versions.
 *
 *   Example:
 *     build/sbt -DsparkVersion=4.0 "runOnlyForReleasableSparkModules publishM2"
 *
 * showSparkVersions
 *   Lists all configured Spark versions (for testing/debugging).
 *
 *   Example:
 *     build/sbt showSparkVersions
 *
 * exportSparkVersionsJson
 *   Exports Spark version information to target/spark-versions.json.
 *   This is the SINGLE SOURCE OF TRUTH for Spark versions used by:
 *   - GitHub Actions workflows (for dynamic matrix generation)
 *   - CI/CD scripts (for version-specific configuration)
 *
 *   The JSON is an array where each element contains:
 *   - fullVersion: Full version string (e.g., "4.0.1", "4.1.0")
 *   - shortVersion: Short version string (e.g., "4.0", "4.1")
 *   - isMaster: Whether this is the master/snapshot version
 *   - isDefault: Whether this is the default Spark version
 *   - targetJvm: Target JVM version (e.g., "17")
 *   - packageSuffix: Maven artifact suffix for this version (e.g., "_4.0", "_4.1")
 *
 *   Example:
 *     build/sbt exportSparkVersionsJson
 *     # Generates: target/spark-versions.json
 *     # Output: [{"fullVersion": "4.0.1", "shortVersion": "4.0", "isMaster": false, "isDefault": true, "targetJvm": "17", "packageSuffix": "_4.0"}, ...]
 *
 *   Use with Python utilities to extract specific fields:
 *     python3 project/scripts/get_spark_version_info.py --all-spark-versions
 *     # Output: ["4.0", "4.1"] or ["master", "4.0"] if master is present
 *     python3 project/scripts/get_spark_version_info.py --get-field "4.0" targetJvm
 *     python3 project/scripts/get_spark_version_info.py --get-field "master" targetJvm
 *
 *   This ensures GitHub Actions always uses the versions defined here,
 *   eliminating manual synchronization across multiple files.
 *
 * ========================================================
 */


/**
 * Specification for a Spark version with all its build configuration.
 *
 * @param fullVersion The full Spark version (e.g., "3.5.7", "4.0.2-SNAPSHOT")
 * @param targetJvm Target JVM version (e.g., "11", "17")
 * @param additionalSourceDir Optional version-specific source directory suffix (e.g., "scala-spark-3.5")
 * @param antlr4Version ANTLR version to use (e.g., "4.9.3", "4.13.1")
 * @param additionalJavaOptions Additional JVM options for tests (e.g., Java 17 --add-opens flags)
 */
case class SparkVersionSpec(
  fullVersion: String,
  targetJvm: String,
  additionalSourceDir: Option[String] = None,
  supportIceberg: Boolean,
  supportHudi: Boolean = true,
  antlr4Version: String,
  additionalJavaOptions: Seq[String] = Seq.empty,
  jacksonVersion: String = "2.15.2",
  additionalResolvers: Seq[Resolver] = Seq.empty
) {
  /** Returns the Spark short version (e.g., "3.5", "4.0") */
  def shortVersion: String = {
    Mima.getMajorMinorPatch(fullVersion) match {
      case (maj, min, _) => s"$maj.$min"
    }
  }

  /** Whether this is the default Spark version */
  def isDefault: Boolean = this == SparkVersionSpec.DEFAULT

  /** Whether this is the master Spark version */
  def isMaster: Boolean = SparkVersionSpec.MASTER.contains(this)

  /** Returns log4j config file */
  def log4jConfig: String = "log4j2.properties"

  /** Whether to export JARs instead of class directories (needed for Spark Connect on master) */
  def exportJars: Boolean = additionalSourceDir.exists(_.contains("master"))

  /** Whether to generate Javadoc/Scaladoc for this version */
  def generateDocs: Boolean = isDefault
}

object SparkVersionSpec {

  private val java17TestSettings = Seq(
    // Copied from SparkBuild.scala to support Java 17 for unit tests (see apache/spark#34153)
    "--add-opens=java.base/java.lang=ALL-UNNAMED",
    "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
    "--add-opens=java.base/java.io=ALL-UNNAMED",
    "--add-opens=java.base/java.net=ALL-UNNAMED",
    "--add-opens=java.base/java.nio=ALL-UNNAMED",
    "--add-opens=java.base/java.util=ALL-UNNAMED",
    "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
    "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
    "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
    "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
    "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED"
  )

  private val spark40 = SparkVersionSpec(
    fullVersion = "4.0.1",
    targetJvm = "17",
    additionalSourceDir = Some("scala-shims/spark-4.0"),
    supportIceberg = true,
    antlr4Version = "4.13.1",
    additionalJavaOptions = java17TestSettings,
    jacksonVersion = "2.18.2"
  )

  private val spark41 = SparkVersionSpec(
    fullVersion = "4.1.0",
    targetJvm = "17",
    additionalSourceDir = Some("scala-shims/spark-4.1"),
    supportIceberg = false,
    supportHudi = false,
    antlr4Version = "4.13.1",
    additionalJavaOptions = java17TestSettings,
    jacksonVersion = "2.18.2"
  )

  private val spark42Snapshot = SparkVersionSpec(
    fullVersion = "4.2.0-SNAPSHOT",
    targetJvm = "17",
    additionalSourceDir = Some("scala-shims/spark-4.2"),
    supportIceberg = false,
    supportHudi = false,
    antlr4Version = "4.13.1",
    additionalJavaOptions = java17TestSettings,
    jacksonVersion = "2.18.2",
    // Artifact updates in maven central for roaringbitmap stopped after 1.3.0.
    // Spark master uses 1.5.3. Relevant Spark PR here https://github.com/apache/spark/pull/52892
    additionalResolvers = Seq("jitpack" at "https://jitpack.io")
  )

  /** Default Spark version */
  val DEFAULT = spark41

  /** Spark master branch version (optional). Release branches should not build against master */
  val MASTER: Option[SparkVersionSpec] = None

  /** All supported Spark versions - internal use only */
  val ALL_SPECS = Seq(spark40, spark41)
}

/** See docs on top of this file */
object CrossSparkVersions extends AutoPlugin {

  override def trigger = allRequirements

  /**
   * Returns the current configured Spark version spec based on the `sparkVersion` property.
   */
  def getSparkVersionSpec(): SparkVersionSpec = {
    val input = sys.props.getOrElse("sparkVersion", SparkVersionSpec.DEFAULT.fullVersion)

    // Resolve aliases first
    val resolvedInput = input match {
      case "default" => SparkVersionSpec.DEFAULT.fullVersion
      case "master" => SparkVersionSpec.MASTER match {
        case Some(masterSpec) => masterSpec.fullVersion
        case None => throw new IllegalArgumentException(
          "No master Spark version is configured. Available versions: " +
          SparkVersionSpec.ALL_SPECS.map(_.fullVersion).mkString(", ")
        )
      }
      case other => other
    }

    // Find spec by full version or short version
    SparkVersionSpec.ALL_SPECS.find { spec =>
      spec.fullVersion == resolvedInput || spec.shortVersion == resolvedInput
    }.getOrElse {
      val aliases = Seq("default") ++ SparkVersionSpec.MASTER.map(_ => "master").toSeq
      val validInputs = SparkVersionSpec.ALL_SPECS.flatMap { spec =>
        Seq(spec.fullVersion, spec.shortVersion)
      } ++ aliases
      throw new IllegalArgumentException(
        s"Invalid sparkVersion: $input. Valid values: ${validInputs.mkString(", ")}"
      )
    }
  }

  /**
   * Returns the current configured Spark version based on the `sparkVersion` property.
   */
  def getSparkVersion(): String = getSparkVersionSpec().fullVersion

  /**
   * Returns module name with Spark version suffix.
   * 
   * By default, ALL Spark-dependent modules include the Spark version suffix:
   *   delta-spark_4.0_2.13, delta-spark_4.1_2.13, etc.
   *
   * During release, the `skipSparkSuffix=true` property is used to also publish
   * backward-compatible artifacts without the suffix (e.g., delta-spark_2.13).
   */
  private def moduleName(baseName: String, sparkVer: String): String = {
    val spec = SparkVersionSpec.ALL_SPECS.find(_.fullVersion == sparkVer)
      .getOrElse(throw new IllegalArgumentException(s"Unknown Spark version: $sparkVer"))

    // skipSparkSuffix removes the suffix (used during release for backward compatibility)
    val skipSparkSuffix = sys.props.getOrElse("skipSparkSuffix", "false").toBoolean

    if (skipSparkSuffix) {
      baseName
    } else {
      s"${baseName}_${spec.shortVersion}"
    }
  }

  // Scala version constant (Scala 2.12 support was dropped)
  private val scala213 = "2.13.17"

  /**
   * Common Spark version-specific settings used by all Spark-aware modules.
   * Returns Scala version, source directories, ANTLR version, JVM options, etc.
   */
  private def sparkVersionAwareSettings(sparkVersionKey: SettingKey[String]): Seq[Setting[_]] = {
    val spec = getSparkVersionSpec()

    val baseSettings = Seq(
      scalaVersion := scala213,
      crossScalaVersions := Seq(scala213),
      resolvers ++= spec.additionalResolvers,
      Antlr4 / antlr4Version := spec.antlr4Version,
      Test / javaOptions ++= (Seq(s"-Dlog4j.configurationFile=${spec.log4jConfig}") ++ spec.additionalJavaOptions)
    )

    val additionalSourceDirSettings = spec.additionalSourceDir.map { dir =>
      // Add both scala-shims and java-shims directories
      val javaShimsDir = dir.replace("scala-shims", "java-shims")
      Seq(
        Compile / unmanagedSourceDirectories += (Compile / baseDirectory).value / "src" / "main" / dir,
        Compile / unmanagedSourceDirectories += (Compile / baseDirectory).value / "src" / "main" / javaShimsDir,
        Test / unmanagedSourceDirectories += (Test / baseDirectory).value / "src" / "test" / dir
      )
    }.getOrElse(Seq.empty)

    val conditionalSettings = Seq(
      if (spec.exportJars) Seq(exportJars := true) else Nil,
      if (spec.generateDocs)
        Seq(unidocSourceFilePatterns := Seq(SourceFilePattern("io/delta/tables/", "io/delta/exceptions/")))
      else Nil
    ).flatten

    // Jackson dependency overrides to match Spark version and avoid conflicts
    val jacksonOverrides = Seq(
      dependencyOverrides ++= {
        val sparkVer = sparkVersionKey.value
        val jacksonVer = SparkVersionSpec.ALL_SPECS.find(_.fullVersion == sparkVer)
          .getOrElse(throw new IllegalArgumentException(s"Unknown Spark version: $sparkVer"))
          .jacksonVersion
        Seq(
          "com.fasterxml.jackson.core" % "jackson-databind" % jacksonVer,
          "com.fasterxml.jackson.core" % "jackson-core" % jacksonVer,
          "com.fasterxml.jackson.core" % "jackson-annotations" % jacksonVer,
          "com.fasterxml.jackson.datatype" % "jackson-datatype-jdk8" % jacksonVer,
          "com.fasterxml.jackson.module" %% "jackson-module-scala" % jacksonVer
        )
      }
    )

    baseSettings ++ additionalSourceDirSettings ++ conditionalSettings ++ jacksonOverrides
  }

  /**
   * Just the module name setting for Spark-dependent modules that don't need full Spark integration.
   * Use this for modules that need versioned artifacts but use default Scala settings.
   *
   * @param sparkVersionKey The sparkVersion setting key for this project
   */
  def sparkDependentModuleName(sparkVersionKey: SettingKey[String]): Seq[Setting[_]] = {
    Seq(
      sparkVersionKey := getSparkVersion(),
      // Dynamically modify moduleName to add Spark version suffix
      Keys.moduleName := moduleName(Keys.name.value, sparkVersionKey.value)
    )
  }

  /**
   * Unified settings for Spark-dependent modules.
   * Use this for modules that need to be built for multiple Spark versions.
   * Works for both published modules and internal modules.
   *
   * @param sparkVersionKey The sparkVersion setting key for this project
   */
  def sparkDependentSettings(sparkVersionKey: SettingKey[String]): Seq[Setting[_]] = {
    sparkDependentModuleName(sparkVersionKey) ++ sparkVersionAwareSettings(sparkVersionKey)
  }

  /**
   * Generates release steps for cross-Spark publishing.
   *
   * Returns a sequence of release steps that:
   * 1. Publishes all modules WITHOUT Spark suffix (backward compatibility)
   * 2. Publishes Spark-dependent modules WITH Spark suffix for each non-master version
   *
   * For example, with Spark versions 4.0 (default) and 4.1:
   * - Step 1 publishes: delta-spark_2.13, delta-storage, delta-kernel-api, etc. (no suffix)
   * - Step 2 publishes: delta-spark_4.0_2.13, delta-spark_4.1_2.13, etc. (with suffix)
   *
   * Each step runs as a separate SBT subprocess so the build reloads with
   * the correct sparkVersion/skipSparkSuffix settings (SBT settings like
   * moduleName are evaluated once at build load time and can't be changed
   * at runtime).
   *
   * Usage in build.sbt:
   *   releaseProcess := Seq[ReleaseStep](
   *     ...,
   *   ) ++ CrossSparkVersions.crossSparkReleaseSteps("publishSigned") ++ Seq(
   *     ...
   *   )
   */
  def crossSparkReleaseSteps(task: String): Seq[ReleaseStep] = {
    // SBT settings (like moduleName) are evaluated once at build load time.
    // To publish with different Spark versions or suffix modes, we must run
    // separate SBT processes so the build reloads with the correct settings.
    // The release version is already committed to version.sbt by prior steps,
    // so subprocess SBT instances will pick up the correct version.

    def runSbtSubprocess(state: State, sbtArgs: Seq[String], description: String): State = {
      val extracted = Project.extract(state)
      val baseDir = extracted.get(ThisBuild / Keys.baseDirectory)
      val cmd = Seq(s"${baseDir.getAbsolutePath}/build/sbt") ++ sbtArgs
      println(s"[info] ========================================")
      println(s"[info] $description")
      println(s"[info] Running: ${cmd.mkString(" ")}")
      println(s"[info] ========================================")
      val exitCode = scala.sys.process.Process(cmd, baseDir).!
      if (exitCode != 0) {
        sys.error(s"$description failed with exit code $exitCode")
      }
      state
    }

    // Step 1: Publish ALL modules WITHOUT Spark suffix (backward compatibility)
    // Uses skipSparkSuffix=true to get artifact names like delta-spark_2.13
    val backwardCompatStep: ReleaseStep = { (state: State) =>
      runSbtSubprocess(
        state,
        Seq("-DskipSparkSuffix=true", task),
        "Publishing all modules without Spark suffix (backward compat)"
      )
    }

    // Step 2+: Publish Spark-dependent modules WITH suffix for each non-master Spark version
    // This gives users versioned artifacts like delta-spark_4.0_2.13, delta-spark_4.1_2.13
    val suffixedSparkSteps: Seq[ReleaseStep] = SparkVersionSpec.ALL_SPECS
      .filterNot(_.isMaster) // Exclude master/snapshot versions
      .map { spec =>
        { (state: State) =>
          runSbtSubprocess(
            state,
            Seq(s"-DsparkVersion=${spec.fullVersion}",
                s"runOnlyForReleasableSparkModules $task"),
            s"Publishing Spark-dependent modules with suffix for Spark ${spec.fullVersion}"
          )
        }: ReleaseStep
      }

    backwardCompatStep +: suffixedSparkSteps
  }

  override lazy val projectSettings = Seq(
    commands += Command.args("runOnlyForReleasableSparkModules", "<task>") { (state, args) =>
      // Used for cross-Spark publishing of Spark-dependent modules only.
      // Runs the specified task only on publishable Spark-dependent projects.
      if (args.isEmpty) {
        sys.error("Usage: runOnlyForReleasableSparkModules <task>\nExample: build/sbt -DsparkVersion=<version> \"runOnlyForReleasableSparkModules publishM2\"")
      }

      val task = args.mkString(" ")

      // Discover Spark-dependent projects dynamically
      // A project is Spark-dependent if:
      // 1. It has the sparkVersion setting (uses Spark-aware configuration)
      // 2. It is publishable (publishArtifact is not false)
      val extracted = sbt.Project.extract(state)
      val sparkVersionKey = SettingKey[String]("sparkVersion")
      val publishArtifactKey = SettingKey[Boolean]("publishArtifact")
      val sparkDependentProjects = extracted.structure.allProjectRefs.filter { projRef =>
        val hasSparkVersion = (projRef / sparkVersionKey).get(extracted.structure.data).isDefined
        val isPublishable = (projRef / publishArtifactKey).get(extracted.structure.data).getOrElse(true)
        hasSparkVersion && isPublishable
      }

      if (sparkDependentProjects.isEmpty) {
        println(s"[warn] No publishable projects with sparkVersion setting found")
        state
      } else {
        val projectNames = sparkDependentProjects.map(_.project).mkString(", ")
        val sparkVer = getSparkVersion()
        println(s"[info] Running '$task' for Spark-dependent modules with Spark $sparkVer")
        println(s"[info] Spark-dependent projects: $projectNames")
        println(s"[info] ========================================")

        // Build scoped task for each Spark-dependent project sequentially
        sparkDependentProjects.foldLeft(state) { (currentState, projRef) =>
          // Handle SBT cross-build prefix: "+publishSigned" must become
          // "+project/publishSigned", not "project/+publishSigned"
          val scopedTask = if (task.startsWith("+")) {
            s"+${projRef.project}/${task.stripPrefix("+")}"
          } else {
            s"${projRef.project}/$task"
          }
          Command.process(scopedTask, currentState)
        }
      }
    },
    commands += Command.command("showSparkVersions") { state =>
      // Used for testing the cross-Spark publish workflow
      SparkVersionSpec.ALL_SPECS.foreach { spec =>
        println(spec.fullVersion)
      }
      state
    },
    commands += Command.command("exportSparkVersionsJson") { state =>
      // Export Spark version information as JSON for use by CI/CD and other tools
      import java.io.{File, PrintWriter}

      val outputFile = new File("target/spark-versions.json")
      outputFile.getParentFile.mkdirs()
      
      val writer = new PrintWriter(outputFile)
      // scalastyle:off
      try {
        writer.println("[")
        SparkVersionSpec.ALL_SPECS.zipWithIndex.foreach { case (spec, idx) =>
          val comma = if (idx < SparkVersionSpec.ALL_SPECS.size - 1) "," else ""
          val isMaster = SparkVersionSpec.MASTER.contains(spec)
          val isDefault = spec == SparkVersionSpec.DEFAULT
          // Package suffix always includes Spark version (e.g., "_4.0", "_4.1")
          val packageSuffix = s"_${spec.shortVersion}"
          writer.println(s"""  {""")
          writer.println(s"""    "fullVersion": "${spec.fullVersion}",""")
          writer.println(s"""    "shortVersion": "${spec.shortVersion}",""")
          writer.println(s"""    "isMaster": $isMaster,""")
          writer.println(s"""    "isDefault": $isDefault,""")
          writer.println(s"""    "targetJvm": "${spec.targetJvm}",""")
          writer.println(s"""    "packageSuffix": "$packageSuffix",""")
          writer.println(s"""    "supportIceberg": "${spec.supportIceberg}",""")
          writer.println(s"""    "supportHudi": "${spec.supportHudi}"""")
          writer.println(s"""  }$comma""")
        }
        writer.println("]")
        
        println(s"[info] Spark version information exported to: ${outputFile.getAbsolutePath}")
      } finally {
        writer.close()
      }
      // scalastyle:on
      
      state
    }
  )
}
