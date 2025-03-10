import Mima.getMajorMinorPatch

object VersionUtils {

  ////////////////////////
  // Java version utils //
  ////////////////////////

  val DEFAULT_JAVA_VERSION = 8
  val JAVA_VERSION_FOR_SPARK_MASTER = 17
  val JAVA_VERSION_FOR_LATEST_RELEASED_SPARK = DEFAULT_JAVA_VERSION

  lazy val buildJavaVersion = {
    var version = sys.props.getOrElse("java.version", "Unknown")
    if (version.startsWith("1.")) version = version.substring(2, 3)
    else {
      val dot = version.indexOf(".")
      if (dot != -1) version = version.substring(0, dot)
    }
    version.toInt
  }

  /** Ensure that the build is running on a minimum Java version. */
  def validateMinimumBuildJavaVersion(): Unit = {
    val minExpectedBuildJavaVersion = getSparkVersion() match {
      case SPARK_MASTER_VERSION => JAVA_VERSION_FOR_SPARK_MASTER  // this is the Spark master build
      case LATEST_RELEASED_SPARK_VERSION => 11
      case _ =>
        throw new IllegalArgumentException(s"Invalid sparkVersion: ${getSparkVersion()}")
    }
    assert(buildJavaVersion >= minExpectedBuildJavaVersion,
      s"Java version $buildJavaVersion is not supported. " +
        s"Please use Java $minExpectedBuildJavaVersion or higher.")
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
