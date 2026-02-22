import sbt._
import sbt.Keys._

/**
 * ========================================================
 * Cross-Flink Build and Publish Support
 * ========================================================
 *
 * This SBT plugin enables Delta Flink modules to be built and published against
 * multiple Flink versions. It derives the effective Flink version from a system
 * property and applies version-specific settings, including artifact naming and
 * dependency resolution.
 *
 * ========================================================
 * Flink Version Selection
 * ========================================================
 *
 * The Flink version is controlled by the {@code flinkVersion} system property.
 * If not specified, the build defaults to the configured DEFAULT Flink version.
 *
 * The property accepts:
 *  - Full version strings (e.g. {@code 1.20.1})
 *  - Short version strings (e.g. {@code 1.20})
 *  - Aliases:
 *      - {@code default}: the DEFAULT Flink version
 *
 * Non-default Flink versions produce Flink-suffixed artifacts, while the default
 * version preserves stable artifact coordinates.
 *
 * Examples:
 *   sbt compile
 *   sbt -DflinkVersion=1.20 compile
 *   sbt -DflinkVersion=master publishM2
 *
 * ========================================================
 */

/**
 *
 * @param version full version of Flink e.g., 1.20.3, 2.0.0
 * @param shortVersion Flink major distribution version, e.g., 1.20 / 2.0 / 2.1
 */
case class FlinkVersionSpec(version: String, shortVersion: String)

object FlinkVersionSpec {
  val flink120 = FlinkVersionSpec("1.20.3", "1.20")
  val flink20 = FlinkVersionSpec("2.0.0", "2.0")
  val DEFAULT = flink120
  val ALL_SPECS = Set(flink120, flink20)
}

object CrossFlinkVersions extends AutoPlugin {

  override def trigger = allRequirements

  def getFlinkVersionSpec(): FlinkVersionSpec = {
    val input = sys.props.getOrElse("flinkVersion", FlinkVersionSpec.DEFAULT.version)

    // Resolve aliases first
    val resolvedInput = input match {
      case "default" => FlinkVersionSpec.DEFAULT.version
      case other => other
    }

    // Find spec by full version or short version
    FlinkVersionSpec.ALL_SPECS.find { spec =>
      spec.version == resolvedInput || spec.shortVersion == resolvedInput
    }.getOrElse {
      val aliases = Seq("default")
      val validInputs = FlinkVersionSpec.ALL_SPECS.flatMap { spec =>
        Seq(spec.version, spec.shortVersion)
      } ++ aliases
      throw new IllegalArgumentException(
        s"Invalid flinkVersion: $input. Valid values: ${validInputs.mkString(", ")}"
      )
    }
  }
  /**
   * Returns the current configured Flink version based on the `flinkVersion` property.
   */
  def getFlinkVersion(): String = getFlinkVersionSpec().version

  val flinkVersion = settingKey[String]("Flink version")

  override def projectSettings: Seq[Def.Setting[_]] = Seq(
    // apply ONLY to project with id "flink"
    flinkVersion := {
      if (thisProject.value.id == "flink") {
        getFlinkVersion()
      } else {
        ""   // no-op for other projects
      }
    }
  )
}
