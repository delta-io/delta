import Mima.getMajorMinorPatch

object VersionUtils {

  ////////////////////////
  // Java version utils //
  ////////////////////////

  val DEFAULT_JAVA_VERSION = 8
  val JAVA_VERSION_FOR_SPARK_MASTER = 17
  val JAVA_VERSION_FOR_LATEST_RELEASED_SPARK = DEFAULT_JAVA_VERSION

  /** Java version of the current build. */
  lazy val buildJavaVersion: Int = {
    val versionStr = sys.props.getOrElse("java.version", "Unknown")
    if (versionStr.startsWith("1.")) {
      // Java 8 or lower: 1.6.0_23, 1.7.0, 1.7.0_80, 1.8.0_211
      versionStr.substring(2, 3).toInt
    } else {
      // Java 9 or higher: 9.0.1, 11.0.4, 12, 12.0.1
      val dot = versionStr.indexOf(".")
      if (dot != -1) versionStr.substring(0, dot).toInt
      else throw new IllegalArgumentException(s"Cannot parse Java version: $versionStr")
    }
  }

  /**
   * Get the Java compiler options that based on the target Java version, while
   * also validating that the buld Java version is compatible with the target.
   */
  def getJavacOptionForTargetJavaVersion(targetJavaVersion: Int): Seq[String] = {
    assert(buildJavaVersion >= targetJavaVersion,
      s"Java version $buildJavaVersion is not supported for building this project. " +
        s"Please use Java $targetJavaVersion or higher.")
    if (buildJavaVersion <= 8) {
      Seq.empty // `--release` is supported since JDK 9 and the minimum supported JDK is 8
    } else {
      Seq("--release", targetJavaVersion.toString)
    }
  }

  /////////////////////////
  // Spark version utils //
  /////////////////////////

  val LATEST_RELEASED_SPARK_VERSION = "3.5.3"
  val SPARK_MASTER_VERSION = "4.0.0-SNAPSHOT"

  val latestReleasedSparkVersionShort = getMajorMinorPatch(LATEST_RELEASED_SPARK_VERSION) match {
    case (maj, min, _) => s"$maj.$min"
  }

  val allValidSparkVersionInputs = Seq(
    "master",
    "latest",
    SPARK_MASTER_VERSION,
    LATEST_RELEASED_SPARK_VERSION,
    latestReleasedSparkVersionShort
  )

  /** Returns the current spark version, which is the same value as `sparkVersion.value`. */
  def getSparkVersion(): String = {
    // e.g. build/sbt -DsparkVersion=master, build/sbt -DsparkVersion=4.0.0-SNAPSHOT
    val input = sys.props.getOrElse("sparkVersion", LATEST_RELEASED_SPARK_VERSION)
    input match {
      case LATEST_RELEASED_SPARK_VERSION | "latest" | `latestReleasedSparkVersionShort` =>
        LATEST_RELEASED_SPARK_VERSION
      case SPARK_MASTER_VERSION | "master" =>
        SPARK_MASTER_VERSION
      case _ =>
        throw new IllegalArgumentException(s"Invalid sparkVersion: $input. Must be one of " +
          s"${allValidSparkVersionInputs.mkString("{", ",", "}")}")
    }
  }
}
