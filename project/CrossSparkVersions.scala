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
 * - DEFAULT VERSION: This is the default when no sparkVersion property is specified
 *   Artifacts for this version have NO Spark version suffix (e.g., delta-spark_2.13).
 *
 * - MASTER VERSION: The Spark master/development branch version
 *   This is optional and typically 
 *   - set in the Delta master branch to a Spark released or snapshot version .
 *   - not set in the Delta release branches as we want to avoid building against Spark unreleased version.
 *   If MASTER is defined, then it can be selected by setting the sparkVersion property to "master".
 *   Artifacts for this version HAVE a Spark version suffix (e.g., delta-spark_4.0_2.13).
 *
 * - OTHER VERSIONS: Any non-default Spark version specified in ALL_SPECS.
 *   All non-default versions get a Spark version suffix in their artifact names.
 *
 * To configure versions, update the SparkVersionSpec values (spark35, spark40, etc.) below.
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
 *    - "default" -> maps to DEFAULT version (spark35)
 *    - "master" -> maps to MASTER version (spark40), if configured
 *
 * If not specified, it defaults to the DEFAULT version.
 *
 * Examples:
 *   build/sbt                                    # Uses default version (3.5.7)
 *   build/sbt -DsparkVersion=3.5                 # Uses 3.5.7
 *   build/sbt -DsparkVersion=3.5.7               # Uses 3.5.7
 *   build/sbt -DsparkVersion=4.0                 # Uses 4.0.2-SNAPSHOT
 *   build/sbt -DsparkVersion=4.0.2-SNAPSHOT      # Uses 4.0.2-SNAPSHOT
 *   build/sbt -DsparkVersion=default             # Uses 3.5.7
 *   build/sbt -DsparkVersion=master              # Uses 4.0.2-SNAPSHOT
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
 *    - Examples: delta-spark, delta-connect-*, delta-sharing-spark, delta-iceberg
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
 * Artifact Naming Convention
 * ========================================================
 * 
 * Default Spark version artifacts (no suffix):
 *   io.delta:delta-spark_2.13:3.4.0
 *   io.delta:delta-connect-server_2.13:3.4.0
 *   io.delta:delta-storage:3.4.0
 *
 * Other Spark version artifacts (with suffix):
 *   io.delta:delta-spark_4.0_2.13:3.4.0
 *   io.delta:delta-connect-server_4.0_2.13:3.4.0
 *   io.delta:delta-storage:3.4.0  (no change, Spark-independent)
 *
 * ========================================================
 * Cross-Release Workflow
 * ========================================================
 * 
 * The cross-release workflow publishes artifacts for all Spark versions in two steps:
 *
 * Step 1: Publish ALL modules for the default Spark version
 *   build/sbt publishSigned  (or publishM2 for local testing)
 *
 * Step 2: Publish ONLY Spark-dependent modules for each non-default Spark version
 *   build/sbt -DsparkVersion=4.0 "runOnlyForReleasableSparkModules publishSigned"
 *
 * This workflow is automated in the release process via crossSparkReleaseSteps().
 * See releaseProcess in build.sbt for integration.
 *
 * Why this approach?
 * - Spark-independent modules (kernel, storage) are built once with default Spark
 * - Spark-dependent modules are built multiple times, once per Spark version
 * - This avoids redundant builds and conflicting artifacts
 *
 * For manual release testing:
 *   build/sbt publishM2
 *   build/sbt -DsparkVersion=4.0 "runOnlyForReleasableSparkModules publishM2"
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
  additionalSourceDir: Option[String],
  antlr4Version: String,
  additionalJavaOptions: Seq[String] = Seq.empty,
  jacksonVersion: String = "2.15.2"
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

  /** Returns log4j config file based on source directory */
  def log4jConfig: String = {
    if (additionalSourceDir.exists(_.contains("master"))) "log4j2_spark_master.properties"
    else "log4j2.properties"
  }

  /** Whether to export JARs instead of class directories (needed for Spark Connect on master) */
  def exportJars: Boolean = additionalSourceDir.exists(_.contains("master"))

  /** Whether to generate Javadoc/Scaladoc for this version */
  def generateDocs: Boolean = isDefault
}

object SparkVersionSpec {

  private val spark35 = SparkVersionSpec(
    fullVersion = "3.5.7",
    targetJvm = "11",
    additionalSourceDir = Some("scala-spark-3.5"),
    antlr4Version = "4.9.3",
    additionalJavaOptions = Seq.empty
  )

  private val spark40Snapshot = SparkVersionSpec(
    fullVersion = "4.0.2-SNAPSHOT",
    targetJvm = "17",
    additionalSourceDir = Some("scala-spark-master"),
    antlr4Version = "4.13.1",
    additionalJavaOptions = Seq(
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
    ),
    jacksonVersion = "2.18.2"
  )

  /** Default Spark version */
  val DEFAULT = spark35

  /** Spark master branch version (optional). Release branches should not build against master */
  val MASTER: Option[SparkVersionSpec] = Some(spark40Snapshot)

  /** All supported Spark versions - internal use only */
  val ALL_SPECS = Seq(spark35, spark40Snapshot)
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
   * Returns module name with optional Spark version suffix.
   * Default Spark version: "module-name" (e.g., delta-spark_2.13)
   * Other Spark versions: "module-name_X.Y" (e.g., delta-spark_4.0_2.13)
   */
  private def moduleName(baseName: String, sparkVer: String): String = {
    val spec = SparkVersionSpec.ALL_SPECS.find(_.fullVersion == sparkVer)
      .getOrElse(throw new IllegalArgumentException(s"Unknown Spark version: $sparkVer"))

    if (spec.isDefault) {
      baseName
    } else {
      s"${baseName}_${spec.shortVersion}"
    }
  }

  // Scala version constant (Scala 2.12 support was dropped)
  private val scala213 = "2.13.16"

  /**
   * Common Spark version-specific settings used by all Spark-aware modules.
   * Returns Scala version, source directories, ANTLR version, JVM options, etc.
   */
  private def sparkVersionAwareSettings(sparkVersionKey: SettingKey[String]): Seq[Setting[_]] = {
    val spec = getSparkVersionSpec()

    val baseSettings = Seq(
      scalaVersion := scala213,
      crossScalaVersions := Seq(scala213),
      // For adding staged Spark RC versions, e.g.:
      // resolvers += "Apache Spark 3.5.0 (RC1) Staging" at "https://repository.apache.org/content/repositories/orgapachespark-1444/",
      Antlr4 / antlr4Version := spec.antlr4Version,
      Test / javaOptions ++= (Seq(s"-Dlog4j.configurationFile=${spec.log4jConfig}") ++ spec.additionalJavaOptions)
    )

    val additionalSourceDirSettings = spec.additionalSourceDir.map { dir =>
      Seq(
        Compile / unmanagedSourceDirectories += (Compile / baseDirectory).value / "src" / "main" / dir,
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
   * 1. Publishes all modules for the default Spark version
   * 2. Publishes only Spark-dependent modules for other Spark versions
   *
   * Usage in build.sbt:
   *   releaseProcess := Seq[ReleaseStep](
   *     ...,
   *   ) ++ CrossSparkVersions.crossSparkReleaseSteps("+publishSigned") ++ Seq(
   *     ...
   *   )
   */
  def crossSparkReleaseSteps(task: String): Seq[ReleaseStep] = {
    import sbtrelease.ReleasePlugin.autoImport._
    import sbtrelease.ReleaseStateTransformations._

    // Step 1: Publish all modules for default Spark version
    val defaultSparkStep: ReleaseStep = releaseStepCommand(task)

    // Step 2: Publish only Spark-dependent modules for other Spark versions
    val otherSparkSteps: Seq[ReleaseStep] = SparkVersionSpec.ALL_SPECS
      .filter(_ != SparkVersionSpec.DEFAULT)
      .flatMap { spec =>
        Seq[ReleaseStep](
          // Custom release step that sets system property and runs command
          { (state: State) =>
            // Set the sparkVersion system property
            sys.props("sparkVersion") = spec.fullVersion

            // Run the runOnlyForReleasableSparkModules command
            Command.process(s"runOnlyForReleasableSparkModules $task", state)
          }: ReleaseStep
        )
      }

    defaultSparkStep +: otherSparkSteps
  }

  override lazy val projectSettings = Seq(
    commands += Command.args("runOnlyForReleasableSparkModules", "<task>") { (state, args) =>
      // Used mainly for cross-Spark publishing of the Spark-dependent modules
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
          val scopedTask = s"${projRef.project}/$task"
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
    }
  )
}
