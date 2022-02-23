package org.apache.spark

import benchmark.SparkEnvironmentInfo

import org.apache.spark.util.Utils

object SparkUtils {
  def getEnvironmentInfo(sc: SparkContext): SparkEnvironmentInfo = {
    val info = sc.statusStore.environmentInfo()
    val sparkBuildInfo = Map(
      "sparkBuildBranch" -> SPARK_BRANCH,
      "sparkBuildVersion" -> SPARK_VERSION,
      "sparkBuildDate" -> SPARK_BUILD_DATE,
      "sparkBuildUser" -> SPARK_BUILD_USER,
      "sparkBuildRevision" -> SPARK_REVISION
    )

    SparkEnvironmentInfo(
      sparkBuildInfo = sparkBuildInfo,
      runtimeInfo = caseClassToMap(info.runtime),
      sparkProps = Utils.redact(sc.conf, info.sparkProperties).toMap,
      hadoopProps =  Utils.redact(sc.conf, info.hadoopProperties).toMap
        .filterKeys(k => !k.startsWith("mapred") && !k.startsWith("yarn")),
      systemProps =  Utils.redact(sc.conf,  info.systemProperties).toMap,
      classpathEntries = info.classpathEntries.toMap
    )
  }

  def caseClassToMap(obj: Object): Map[String, String] = {
    obj.getClass.getDeclaredFields.flatMap { f =>
      f.setAccessible(true)
      val valueOption = f.get(obj) match {
        case o: Option[_] => o.map(_.toString)
        case s => Some(s.toString)
      }
      valueOption.map(value => f.getName -> value)
    }.toMap
  }
}
