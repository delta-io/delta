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

package org.apache.spark.sql.delta.catalog

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.delta.sources.DeltaSQLConf

import java.util
import java.util.Locale
import scala.collection.JavaConverters._

case class DeltaTableProperties(properties: Map[String, String], options: Map[String, String])

object DeltaTableProperties {

  def apply(properties: util.Map[String, String],
            deltaOptions: Map[String, String] = Map.empty): DeltaTableProperties = {
    val (options, props) = split(properties.asScala.toMap)
    val writeOptions = if (deltaOptions.isEmpty && options.nonEmpty) options else deltaOptions
    val tableProperties = props ++ deltaProperties(writeOptions)

    new DeltaTableProperties(tableProperties, writeOptions)
  }

  private def deltaProperties(writeOptions: Map[String, String]): Map[String, String] = {
    val conf = SparkSession.active.sessionState.conf

    if (conf.getConf(DeltaSQLConf.DELTA_LEGACY_STORE_WRITER_OPTIONS_AS_PROPS)) {
      // Legacy behavior
      writeOptions
    } else {
      writeOptions.filter { case (k, _) =>
        k.toLowerCase(Locale.ROOT).startsWith("delta.") }
    }
  }

  private def split(properties: Map[String, String]): (Map[String, String], Map[String, String]) = {
    // Options passed in through the SQL API will show up both with an "option." prefix and
    // without in Spark 3.1, so we need to remove those from the properties
    val optionsThroughProperties = properties.collect {
      case (k, _) if k.startsWith("option.") => k.stripPrefix("option.")
    }.toSet

    val options = new util.HashMap[String, String]()
    val props = new util.HashMap[String, String]()

    properties.foreach { case (k, v) =>
      if (!k.startsWith("option.") && !optionsThroughProperties.contains(k) && !k.equals("path")) {
        // Do not add to properties
        props.put(k, v)
      } else if (optionsThroughProperties.contains(k)) {
        options.put(k, v)
      }
    }

    (options.asScala.toMap, props.asScala.toMap)
  }
}
