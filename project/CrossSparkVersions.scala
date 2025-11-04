import sbt._
import sbt.Keys._
import sbt.complete.DefaultParsers._
import com.simplytyped.Antlr4Plugin
import com.simplytyped.Antlr4Plugin.autoImport._
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
  additionalJavaOptions: Seq[String] = Seq.empty
) {
  /** Returns the Spark short version (e.g., "3.5", "4.0") */
  def shortVersion: String = {
    Mima.getMajorMinorPatch(fullVersion) match {
      case (maj, min, _) => s"$maj.$min"
    }
  }

  /** Whether this is the latest released Spark version */
  def isLatestReleased: Boolean = this == SparkVersionSpec.LATEST_RELEASED

  /** Whether this is the master Spark version */
  def isMaster: Boolean = this == SparkVersionSpec.MASTER

  /** Returns log4j config file based on source directory */
  def log4jConfig: String = {
    if (additionalSourceDir.exists(_.contains("master"))) "log4j2_spark_master.properties"
    else "log4j2.properties"
  }

  /** Whether to export JARs instead of class directories (needed for Spark Connect on master) */
  def exportJars: Boolean = additionalSourceDir.exists(_.contains("master"))

  /** Whether to generate Javadoc/Scaladoc for this version */
  def generateDocs: Boolean = isLatestReleased
}

object SparkVersionSpec {
  /** Spark 3.5.7 - latest released version */
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
    )
  )

  /** Latest released Spark version */
  val LATEST_RELEASED = spark35

  /** Spark master branch version */
  val MASTER = spark40

  /** All supported Spark versions */
  val allSpecs = Seq(spark35, spark40)
}

/**
 * SBT plugin to support cross-building for multiple Spark versions.
 *
 * Provides the `crossSparkRelease` command to run a task for all configured Spark versions.
 *
 * Artifact Naming:
 * - Latest Spark version (3.5.x): delta-spark_2.13 (no version suffix)
 * - Other Spark versions (e.g., 4.0.x): delta-spark_4.0_2.13 (includes binary version)
 *
 * Usage:
 *   build/sbt spark/publishM2                        # Publish delta-spark_2.13 (Spark 3.5.7)
 *   build/sbt -DsparkVersion=master spark/publishM2  # Publish delta-spark_4.0_2.13 (Spark 4.0.2-SNAPSHOT)
 *   build/sbt "crossSparkRelease publishM2"          # Publish all Spark versions
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
   * Supports inputs: full version (e.g., "3.5.7"), short version (e.g., "3.5"), or aliases ("latest", "master")
   */
  def getSparkVersionSpec(): SparkVersionSpec = {
    val input = sys.props.getOrElse("sparkVersion", SparkVersionSpec.LATEST_RELEASED.fullVersion)

    // Resolve aliases first
    val resolvedInput = input match {
      case "latest" => SparkVersionSpec.LATEST_RELEASED.fullVersion
      case "master" => SparkVersionSpec.MASTER.fullVersion
      case other => other
    }

    // Find spec by full version or short version
    SparkVersionSpec.allSpecs.find { spec =>
      spec.fullVersion == resolvedInput || spec.shortVersion == resolvedInput
    }.getOrElse {
      val validInputs = SparkVersionSpec.allSpecs.flatMap { spec =>
        Seq(spec.fullVersion, spec.shortVersion)
      } ++ Seq("latest", "master")
      throw new IllegalArgumentException(
        s"Invalid sparkVersion: $input. Valid values: ${validInputs.mkString(", ")}"
      )
    }
  }

  /**
   * Returns the current spark version.
   * Supports aliases: "latest"/"3.5" -> 3.5.7, "master"/"4.0" -> 4.0.2-SNAPSHOT
   */
  def getSparkVersion(): String = getSparkVersionSpec().fullVersion

  /**
   * Returns module name with optional Spark version suffix.
   * Latest released Spark version: "module-name" (e.g., delta-spark)
   * Other Spark versions: "module-name_X.Y" (e.g., delta-spark_4.0)
   */
  def moduleName(baseName: String, sparkVer: String): String = {
    val spec = SparkVersionSpec.allSpecs.find(_.fullVersion == sparkVer)
      .getOrElse(throw new IllegalArgumentException(s"Unknown Spark version: $sparkVer"))

    if (spec.isLatestReleased) {
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
   * Unified settings for Spark-dependent modules.
   * Use this for modules that need to be built for multiple Spark versions.
   *
   * Sets:
   * - sparkVersion (from system property or default)
   * - moduleName (with Spark version suffix for non-latest versions)
   * - requiresCrossSparkBuild := true
   * - All sparkVersionAwareSettings (Scala, source dirs, ANTLR, JVM options, etc.)
   *
   * @param sparkVersionKey The sparkVersion setting key for this project
   */
  def sparkDependentSettings(sparkVersionKey: SettingKey[String]): Seq[Setting[_]] = {
    Seq(
      sparkVersionKey := getSparkVersion(),
      // Dynamically modify moduleName to add Spark version suffix
      Keys.moduleName := moduleName(Keys.name.value, sparkVersionKey.value),
      requiresCrossSparkBuild := true
    ) ++ sparkVersionAwareSettings()
  }

  /**
   * Minimal settings for Spark-dependent modules that don't need full Spark integration.
   * Use this for modules that need versioned artifacts but use default Scala settings.
   *
   * Sets:
   * - sparkVersion (from system property or default)
   * - moduleName (with Spark version suffix for non-latest versions)
   * - requiresCrossSparkBuild := true
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
   * Spark version-aware settings for internal modules (not published).
   * Use this for internal modules that need Spark version-specific configuration
   * but are not published to Maven (e.g., sparkV1, sparkV2).
   *
   * Sets sparkVersion and all sparkVersionAwareSettings without publishing-related
   * settings like requiresCrossSparkBuild, so these modules won't be included in
   * cross-Spark builds.
   *
   * @param sparkVersionKey The sparkVersion setting key for this project
   */
  def sparkInternalSettings(sparkVersionKey: SettingKey[String]): Seq[Setting[_]] = {
    Seq(sparkVersionKey := getSparkVersion()) ++ sparkVersionAwareSettings()
  }

  override lazy val projectSettings = Seq(
    commands += Command.args("crossSparkRelease", "<task>") { (state, args) =>
        if (args.isEmpty) {
          sys.error("Usage: crossSparkRelease <task>")
        }

        val task = args.mkString(" ")

        println(s"[info] Running '$task' for Spark versions: ${SparkVersionSpec.allSpecs.map(_.fullVersion).mkString(", ")}")

        // First pass: Build everything for the first (latest) Spark version
        val firstSparkVer = SparkVersionSpec.LATEST_RELEASED.fullVersion
        println(s"[info] ========================================")
        println(s"[info] Building ALL modules with Spark $firstSparkVer")
        println(s"[info] ========================================")
        System.setProperty("sparkVersion", firstSparkVer)
        val afterReload1 = Command.process("reload", state)
        val afterFirstBuild = Command.process(task, afterReload1)

        // Subsequent passes: Build only Spark-dependent modules for other Spark versions
        val remainingVersions = SparkVersionSpec.allSpecs.filterNot(_.fullVersion == firstSparkVer)

        val results = remainingVersions.foldLeft(afterFirstBuild) { (currentState, sparkSpec) =>
          val sparkVer = sparkSpec.fullVersion
          println(s"[info] ========================================")
          println(s"[info] Building Spark-dependent modules with Spark $sparkVer")
          println(s"[info] ========================================")

          // Set system property and reload
          System.setProperty("sparkVersion", sparkVer)
          val afterReload = Command.process("reload", currentState)

          // Discover Spark-dependent projects dynamically
          val extracted = sbt.Project.extract(afterReload)
          val sparkDependentProjects = extracted.structure.allProjectRefs.filter { projRef =>
            (projRef / requiresCrossSparkBuild).get(extracted.structure.data).getOrElse(false)
          }

          if (sparkDependentProjects.isEmpty) {
            println(s"[warn] No projects with requiresCrossSparkBuild := true found")
            afterReload
          } else {
            val projectNames = sparkDependentProjects.map(_.project).mkString(", ")
            println(s"[info] Spark-dependent projects: $projectNames")

            // Build scoped task for each Spark-dependent project sequentially
            sparkDependentProjects.foldLeft(afterReload) { (currentState, projRef) =>
              // Clean before task to ensure rebuild with new Spark version
              val afterClean = Command.process(s"${projRef.project}/clean", currentState)
              val scopedTask = s"${projRef.project}/$task"
              Command.process(scopedTask, afterClean)
            }
          }
        }

      println(s"[info] ========================================")
      println(s"[info] Completed cross-Spark build for all versions")
      println(s"[info] ========================================")
      results
    }
  )
}
