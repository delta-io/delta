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
 * @param aliases Short aliases for this version (e.g., "latest", "master")
 * @param targetJvm Target JVM version (e.g., "11", "17")
 * @param additionalSourceDir Optional version-specific source directory suffix (e.g., "scala-spark-3.5")
 * @param antlr4Version ANTLR version to use (e.g., "4.9.3", "4.13.1")
 * @param additionalJavaOptions Additional JVM options for tests (e.g., Java 17 --add-opens flags)
 * @param isLatest Whether this is the latest released version (affects artifact naming and doc generation)
 */
case class SparkVersionSpec(
  fullVersion: String,
  aliases: Seq[String],
  targetJvm: String,
  additionalSourceDir: Option[String],
  antlr4Version: String,
  additionalJavaOptions: Seq[String] = Seq.empty,
  isLatest: Boolean = false
) {
  /** Returns the Spark binary version (e.g., "3.5", "4.0") */
  def binaryVersion: String = {
    Mima.getMajorMinorPatch(fullVersion) match {
      case (maj, min, _) => s"$maj.$min"
    }
  }

  /** Returns all valid input strings that should resolve to this version */
  def allValidInputs: Seq[String] = Seq(fullVersion, binaryVersion) ++ aliases

  /** Returns log4j config file based on source directory */
  def log4jConfig: String = {
    if (additionalSourceDir.exists(_.contains("master"))) "log4j2_spark_master.properties"
    else "log4j2.properties"
  }

  /** Whether to export JARs instead of class directories (needed for Spark Connect on master) */
  def exportJars: Boolean = additionalSourceDir.exists(_.contains("master"))

  /** Whether to generate Javadoc/Scaladoc for this version */
  def generateDocs: Boolean = isLatest
}

object SparkVersionSpec {
  /** Spark 3.5.7 - latest released version */
  val spark35 = SparkVersionSpec(
    fullVersion = "3.5.7",
    aliases = Seq("latest"),
    targetJvm = "11",
    additionalSourceDir = Some("scala-spark-3.5"),
    antlr4Version = "4.9.3",
    additionalJavaOptions = Seq.empty,
    isLatest = true
  )

  /** Spark 4.0.2-SNAPSHOT - master branch */
  val spark40 = SparkVersionSpec(
    fullVersion = "4.0.2-SNAPSHOT",
    aliases = Seq("master"),
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
    isLatest = false
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

  // Custom setting keys
  val targetJvm = settingKey[String]("Target JVM version")

  /**
   * Returns the current spark version spec.
   */
  def getSparkVersionSpec(): SparkVersionSpec = {
    val input = sys.props.getOrElse("sparkVersion", SparkVersionSpec.LATEST_RELEASED.fullVersion)
    SparkVersionSpec.allSpecs.find(_.allValidInputs.contains(input))
      .getOrElse {
        val allValidInputs = SparkVersionSpec.allSpecs.flatMap(_.allValidInputs).mkString(", ")
        throw new IllegalArgumentException(
          s"Invalid sparkVersion: $input. Valid values: $allValidInputs"
        )
      }
  }

  /**
   * Returns the current spark version.
   * Supports aliases: "latest"/"3.5" -> 3.5.7, "master"/"4.0" -> 4.0.2-SNAPSHOT
   */
  def getSparkVersion(): String = getSparkVersionSpec().fullVersion

  /**
   * Returns true if the current Spark version is the latest released version.
   */
  def isLatestReleasedVersion(): Boolean = {
    getSparkVersion() == SparkVersionSpec.LATEST_RELEASED.fullVersion
  }

  /**
   * Returns true if the current Spark version is the master version.
   */
  def isMasterVersion(): Boolean = {
    getSparkVersion() == SparkVersionSpec.MASTER.fullVersion
  }

  /**
   * Returns the Spark binary version (major.minor) for the given full Spark version.
   * E.g., "3.5.7" -> "3.5", "4.0.2-SNAPSHOT" -> "4.0"
   */
  def getSparkBinaryVersion(sparkVer: String): String = {
    Mima.getMajorMinorPatch(sparkVer) match {
      case (maj, min, _) => s"$maj.$min"
    }
  }

  /**
   * Returns module name with optional Spark version suffix.
   * Latest released Spark version: "module-name" (e.g., delta-spark)
   * Other Spark versions: "module-name_X.Y" (e.g., delta-spark_4.0)
   */
  def moduleName(baseName: String, sparkVer: String): String = {
    val spec = SparkVersionSpec.allSpecs.find(_.fullVersion == sparkVer)
      .getOrElse(throw new IllegalArgumentException(s"Unknown Spark version: $sparkVer"))

    if (spec.isLatest) {
      baseName
    } else {
      s"${baseName}_${spec.binaryVersion}"
    }
  }

  /**
   * Returns Spark version-specific build settings.
   * @param defaultScalaVersion The default Scala version setting key
   * @param allScalaVersions All Scala versions to cross-build
   * @param scala213 Scala 2.13 version string
   */
  def crossSparkSettings(defaultScalaVersion: SettingKey[String], allScalaVersions: Seq[String], scala213: String): Seq[Setting[_]] = {
    val spec = getSparkVersionSpec()

    val baseSettings = Seq(
      scalaVersion := (if (spec.isLatest) defaultScalaVersion.value else scala213),
      crossScalaVersions := (if (spec.isLatest) allScalaVersions else Seq(scala213)),
      targetJvm := spec.targetJvm,
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

  override lazy val projectSettings = Seq(
    commands += Command.args("crossSparkRelease", "<task>") { (state, args) =>
        if (args.isEmpty) {
          sys.error("Usage: crossSparkRelease <task>")
        }

        val task = args.mkString(" ")

        // Enable crossSparkRelease flag for release process
        System.setProperty("crossSparkRelease", "true")

        println(s"[info] Running '$task' for Spark versions: ${SparkVersionSpec.allSpecs.map(_.fullVersion).mkString(", ")}")

        val results = SparkVersionSpec.allSpecs.map(_.fullVersion).foldLeft(state) { (currentState, sparkVer) =>
          println(s"[info] ========================================")
          println(s"[info] Building with Spark $sparkVer")
          println(s"[info] ========================================")

          System.setProperty("sparkVersion", sparkVer)

          // Execute: reload, then the task
          val reloadCommand = "reload"
          val afterReload = Command.process(reloadCommand, currentState)
          Command.process(task, afterReload)
        }

      println(s"[info] ========================================")
      println(s"[info] Completed cross-Spark build for all versions")
      println(s"[info] ========================================")
      results
    }
  )
}
