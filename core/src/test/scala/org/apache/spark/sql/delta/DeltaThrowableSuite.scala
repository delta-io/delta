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

package org.apache.spark.sql.delta

import java.net.URL

import org.apache.spark.sql.delta.DeltaThrowableHelper.{deltaErrorClassSource, deltaErrorClassToInfoMap}
import com.fasterxml.jackson.annotation.JsonInclude.Include
import com.fasterxml.jackson.core.JsonParser.Feature.STRICT_DUPLICATE_DETECTION
import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.databind.json.JsonMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.commons.io.IOUtils

import org.apache.spark.{ErrorInfo, SparkFunSuite}
import org.apache.spark.util.Utils

/** Test suite for Delta Throwables. */
class DeltaThrowableSuite extends SparkFunSuite {
  def checkIfUnique(ss: Seq[Any]): Unit = {
    val duplicatedKeys = ss.groupBy(identity).mapValues(_.size).filter(_._2 > 1).keys.toSeq
    assert(duplicatedKeys.isEmpty)
  }

  def checkCondition(ss: Seq[String], fx: String => Boolean): Unit = {
    ss.foreach { s =>
      assert(fx(s))
    }
  }

  test("No duplicate error classes in Delta") {
    // Enabling this feature incurs performance overhead (20-30%)
    val mapper = JsonMapper.builder()
      .addModule(DefaultScalaModule)
      .enable(STRICT_DUPLICATE_DETECTION)
      .build()
    mapper.readValue(deltaErrorClassSource, new TypeReference[Map[String, ErrorInfo]]() {})
  }

  test("No error classes are shared by Delta and Spark") {
    val mapper = JsonMapper.builder().addModule(DefaultScalaModule).build()
    val sparkErrorClassSource: URL =
      Utils.getSparkClassLoader.getResource("error/error-classes.json")
    val sparkErrorClasses = mapper.readValue(
      sparkErrorClassSource, new TypeReference[Map[String, ErrorInfo]]() {})
    assert(deltaErrorClassToInfoMap.keySet.intersect(sparkErrorClasses.keySet).isEmpty)
  }

  test("Delta error classes are correctly formatted") {
    val errorClassFileContents = IOUtils.toString(deltaErrorClassSource.openStream())
    val mapper = JsonMapper.builder()
      .addModule(DefaultScalaModule)
      .enable(SerializationFeature.INDENT_OUTPUT)
      .build()
    val rewrittenString = mapper.configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true)
      .setSerializationInclusion(Include.NON_ABSENT)
      .writeValueAsString(deltaErrorClassToInfoMap)
    assert(rewrittenString.trim == errorClassFileContents.trim)
  }

  test("Delta message format invariants") {
    val messageFormats = deltaErrorClassToInfoMap.values.toSeq.map(_.messageFormat)
    checkCondition(messageFormats, s => s != null)
    checkIfUnique(messageFormats)
  }
}
