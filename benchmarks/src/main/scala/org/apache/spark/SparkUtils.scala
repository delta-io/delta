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
