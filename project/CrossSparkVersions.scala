import sbt._
import sbt.Keys._
import sbt.complete.DefaultParsers._
import com.simplytyped.Antlr4Plugin
import com.simplytyped.Antlr4Plugin.autoImport._
import sbtrelease.ReleasePlugin.autoImport.ReleaseStep
import Unidoc._

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
  /** Spark 3.5.7 - default version */
  val spark35 = SparkVersionSpec(
    fullVersion = "3.5.7",
    targetJvm = "11",
    additionalSourceDir = Some("scala-spark-3.5"),
    antlr4Version = "4.9.3",
    additionalJavaOptions = Seq.empty
  )

  /** Spark 4.0.2-SNAPSHOT - master branch */
  val spark40 = SparkVersionSpec(
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
  val MASTER: Option[SparkVersionSpec] = Some(spark40)

  /** All supported Spark versions */
  val ALL_SPECS = Seq(spark35, spark40)
}

/**
 * SBT plugin to support cross-building for multiple Spark versions.
 *
 * Provides the `runOnlyForSparkModules` command to run a task only for Spark-dependent modules.
 *
 * Artifact Naming:
 * - Latest Spark version (3.5.x): delta-spark_2.13 (no version suffix)
 * - Other Spark versions (e.g., 4.0.x): delta-spark_4.0_2.13 (includes binary version)
 *
 * Usage:
 *   # Publish all modules for Spark 3.5.7 (latest)
 *   build/sbt publishM2
 *
 *   # Publish only Spark-dependent modules for Spark 4.0
 *   build/sbt -DsparkVersion=4.0.2-SNAPSHOT "runOnlyForSparkModules publishM2"
 *
 *   # Or use aliases
 *   build/sbt -DsparkVersion=master "runOnlyForSparkModules publishM2"
 *
 * To publish for all Spark versions, run both commands sequentially.
 */
object CrossSparkVersions extends AutoPlugin {

  override def trigger = allRequirements

  // Settings keys
  object autoImport {
    val requiresCrossSparkBuild = settingKey[Boolean]("Whether this module requires cross-Spark version building")
  }
  import autoImport._

  /**
   * Returns the current spark version spec.
   * Supports inputs: full version (e.g., "3.5.7"), short version (e.g., "3.5"), or aliases ("default", "master")
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
   * Returns the current spark version.
   * Supports aliases: "default"/"3.5" -> 3.5.7, "master"/"4.0" -> 4.0.2-SNAPSHOT
   */
  def getSparkVersion(): String = getSparkVersionSpec().fullVersion

  /**
   * Returns the Jackson version for a given Spark version.
   * This is used to override Jackson dependencies to match what Spark expects.
   *
   * @param sparkVersionKey The sparkVersion setting key
   * @return Def.Initialize[String] containing the Jackson version
   */
  def jacksonVersionForSpark(sparkVersionKey: SettingKey[String]): Def.Initialize[String] = Def.setting {
    val sparkVer = sparkVersionKey.value
    val spec = SparkVersionSpec.ALL_SPECS.find(_.fullVersion == sparkVer)
      .getOrElse(throw new IllegalArgumentException(s"Unknown Spark version: $sparkVer"))
    spec.jacksonVersion
  }

  /**
   * Returns module name with optional Spark version suffix.
   * Default Spark version: "module-name" (e.g., delta-spark)
   * Other Spark versions: "module-name_X.Y" (e.g., delta-spark_4.0)
   */
  def moduleName(baseName: String, sparkVer: String): String = {
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
  private def sparkVersionAwareSettings(): Seq[Setting[_]] = {
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

    baseSettings ++ additionalSourceDirSettings ++ conditionalSettings
  }

  /**
   * Minimal settings for Spark-dependent modules that don't need full Spark integration.
   * Use this for modules that need versioned artifacts but use default Scala settings.
   *
   * @param sparkVersionKey The sparkVersion setting key for this project
   */
  def sparkDependentModuleName(sparkVersionKey: SettingKey[String]): Seq[Setting[_]] = {
    Seq(
      sparkVersionKey := getSparkVersion(),
      // Dynamically modify moduleName to add Spark version suffix
      Keys.moduleName := moduleName(Keys.name.value, sparkVersionKey.value),
      requiresCrossSparkBuild := true
    )
  }

  /**
   * Unified settings for Spark-dependent modules.
   * Use this for modules that need to be built for multiple Spark versions.
   *
   * @param sparkVersionKey The sparkVersion setting key for this project
   */
  def sparkDependentSettings(sparkVersionKey: SettingKey[String]): Seq[Setting[_]] = {
    sparkDependentModuleName(sparkVersionKey) ++ sparkVersionAwareSettings()
  }

  /**
   * Spark version-aware settings for internal modules (not published).
   * Use this for internal modules that need Spark version-specific configuration
   * but are not published to Maven (e.g., sparkV1, sparkV2).
   *
   * @param sparkVersionKey The sparkVersion setting key for this project
   */
  def sparkInternalSettings(sparkVersionKey: SettingKey[String]): Seq[Setting[_]] = {
    Seq(
      sparkVersionKey := getSparkVersion(),
      // Dynamically modify moduleName to add Spark version suffix
      Keys.moduleName := moduleName(Keys.name.value, sparkVersionKey.value),
    ) ++ sparkVersionAwareSettings()
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

            // Run the runOnlyForSparkModules command
            Command.process(s"runOnlyForSparkModules $task", state)
          }: ReleaseStep
        )
      }

    defaultSparkStep +: otherSparkSteps
  }

  override lazy val projectSettings = Seq(
    commands += Command.args("runOnlyForSparkModules", "<task>") { (state, args) =>
      // Used mainly for cross-Spark publishing of the Spark-dependent modules
      if (args.isEmpty) {
        sys.error("Usage: runOnlyForSparkModules <task>\nExample: build/sbt -DsparkVersion=4.0.2-SNAPSHOT \"runOnlyForSparkModules publishM2\"")
      }

      val task = args.mkString(" ")

      // Discover Spark-dependent projects dynamically
      val extracted = sbt.Project.extract(state)
      val sparkDependentProjects = extracted.structure.allProjectRefs.filter { projRef =>
        (projRef / requiresCrossSparkBuild).get(extracted.structure.data).getOrElse(false)
      }

      if (sparkDependentProjects.isEmpty) {
        println(s"[warn] No projects with requiresCrossSparkBuild := true found")
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
