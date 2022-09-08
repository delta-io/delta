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

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.Files

import scala.collection.immutable.SortedMap

import org.apache.spark.sql.delta.DeltaThrowableHelper.{deltaErrorClassSource, deltaErrorClassToInfoMap, sparkErrorClassesMap}
import com.fasterxml.jackson.annotation.JsonInclude.Include
import com.fasterxml.jackson.core.JsonParser.Feature.STRICT_DUPLICATE_DETECTION
import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.core.util.{DefaultIndenter, DefaultPrettyPrinter}
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.databind.json.JsonMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.commons.io.{FileUtils, IOUtils}

import org.apache.spark.{ErrorInfo, SparkFunSuite}

/** Test suite for Delta Throwables. */
class DeltaThrowableSuite extends SparkFunSuite {

   /* Used to regenerate the error class file. Run:
   {{{
      SPARK_GENERATE_GOLDEN_FILES=1 build/sbt \
        "sql/testOnly *DeltaThrowableSuite -- -t \"Error classes are correctly formatted\""
   }}}
   */

  private val regenerateGoldenFiles: Boolean = System.getenv("SPARK_GENERATE_GOLDEN_FILES") == "1"

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
    assert(deltaErrorClassToInfoMap.keySet.intersect(sparkErrorClassesMap.keySet).isEmpty)
  }

  test("No word 'databricks' in OSS Delta errors") {
    val errorClasses = deltaErrorClassToInfoMap.keys.toSeq
    val errorMsgs = deltaErrorClassToInfoMap.values.toSeq.flatMap(_.message)
    checkCondition(errorClasses ++ errorMsgs, s => !s.toLowerCase().contains("databricks"))
  }

  test("Delta error classes are correctly formatted") {
    lazy val ossDeltaErrorFile = new File(getWorkspaceFilePath(
      "delta", "core", "src", "main", "resources", "error").toFile,
      "delta-error-classes.json")
    val errorClassFileContents = {
      IOUtils.toString(deltaErrorClassSource.openStream())
    }
    val mapper = JsonMapper.builder()
      .addModule(DefaultScalaModule)
      .enable(SerializationFeature.INDENT_OUTPUT)
      .build()
    val prettyPrinter = new DefaultPrettyPrinter()
      .withArrayIndenter(DefaultIndenter.SYSTEM_LINEFEED_INSTANCE)
    val rewrittenString = {
      val writer = mapper.configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true)
        .setSerializationInclusion(Include.NON_ABSENT)
        .writer(prettyPrinter)
      writer.writeValueAsString(deltaErrorClassToInfoMap)
    }

    if (regenerateGoldenFiles) {
      if (rewrittenString.trim != errorClassFileContents.trim) {
        logInfo(s"Regenerating error class file $ossDeltaErrorFile")
        Files.delete(ossDeltaErrorFile.toPath)
        FileUtils.writeStringToFile(ossDeltaErrorFile, rewrittenString, StandardCharsets.UTF_8)
      }
    } else {
      assert(rewrittenString.trim == errorClassFileContents.trim)
    }
  }

  test("Delta message format invariants") {
    val messageFormats =
    deltaErrorClassToInfoMap.values.toSeq.map(_.messageFormat)
    checkCondition(messageFormats, s => s != null)
    checkIfUnique(messageFormats)
  }
}
