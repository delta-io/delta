/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * This file contains code from the Apache Spark project (original license above).
 * It contains modifications, which are licensed as follows:
 */

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

package org.apache.spark.sql.delta.logging

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.util.regex.Pattern

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.logging.log4j.Level

import org.apache.spark.SparkFunSuite
import org.apache.spark.internal.{LogEntry, Logging, LogKey, MDC}

class DeltaStructuredLoggingSuite extends SparkFunSuite with Logging {
  private def className: String = classOf[DeltaStructuredLoggingSuite].getSimpleName
  private def logFilePath: String = "target/structured.log"

  private lazy val logFile: File = {
    val pwd = new File(".").getCanonicalPath
    new File(pwd + "/" + logFilePath)
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    Logging.enableStructuredLogging()
  }

  override def afterAll(): Unit = {
    Logging.disableStructuredLogging()
    super.afterAll()
  }

  private val jsonMapper = new ObjectMapper().registerModule(DefaultScalaModule)
  private def compactAndToRegexPattern(json: String): String = {
    jsonMapper.readTree(json).toString.
      replace("<timestamp>", """[^"]+""").
      replace(""""<stacktrace>"""", """.*""").
      replace("{", """\{""") + "\n"
  }

  // Return the newly added log contents in the log file after executing the function `f`
  private def captureLogOutput(f: () => Unit): String = {
    val content = if (logFile.exists()) {
      new String(Files.readAllBytes(logFile.toPath), StandardCharsets.UTF_8)
    } else {
      ""
    }
    f()
    val newContent =
      new String(Files.readAllBytes(logFile.toPath), StandardCharsets.UTF_8)
    newContent.substring(content.length)
  }

  private def basicMsg: String = "This is a log message"

  private def msgWithMDC: LogEntry = log"Lost executor ${MDC(DeltaLogKeys.EXECUTOR_ID, "1")}."

  private def msgWithMDCValueIsNull: LogEntry =
    log"Lost executor ${MDC(DeltaLogKeys.EXECUTOR_ID, null)}."

  private def msgWithMDCAndException: LogEntry =
    log"Error in executor ${MDC(DeltaLogKeys.EXECUTOR_ID, "1")}."

  private def msgWithConcat: LogEntry = log"Min Size: ${MDC(DeltaLogKeys.MIN_SIZE, "2")}, " +
    log"Max Size: ${MDC(DeltaLogKeys.MAX_SIZE, "4")}. " +
    log"Please double check."

  private val customLog = log"${MDC(CustomLogKeys.CUSTOM_LOG_KEY, "Custom log message.")}"

  def expectedPatternForBasicMsg(level: Level): String = {
    compactAndToRegexPattern(
      s"""
        {
          "ts": "<timestamp>",
          "level": "$level",
          "msg": "This is a log message",
          "logger": "$className"
        }""")
  }

  def expectedPatternForBasicMsgWithException(level: Level): String = {
    compactAndToRegexPattern(
      s"""
        {
          "ts": "<timestamp>",
          "level": "$level",
          "msg": "This is a log message",
          "exception": {
            "class": "java.lang.RuntimeException",
            "msg": "OOM",
            "stacktrace": "<stacktrace>"
          },
          "logger": "$className"
        }""")
  }

  def expectedPatternForMsgWithMDC(level: Level): String = {
    compactAndToRegexPattern(
      s"""
        {
          "ts": "<timestamp>",
          "level": "$level",
          "msg": "Lost executor 1.",
          "context": {
             "executor_id": "1"
          },
          "logger": "$className"
        }""")
  }

  def expectedPatternForMsgWithMDCValueIsNull(level: Level): String = {
    compactAndToRegexPattern(
      s"""
        {
          "ts": "<timestamp>",
          "level": "$level",
          "msg": "Lost executor null.",
          "context": {
             "executor_id": null
          },
          "logger": "$className"
        }""")
  }

  def expectedPatternForMsgWithMDCAndException(level: Level): String = {
    compactAndToRegexPattern(
      s"""
        {
          "ts": "<timestamp>",
          "level": "$level",
          "msg": "Error in executor 1.",
          "context": {
            "executor_id": "1"
          },
          "exception": {
            "class": "java.lang.RuntimeException",
            "msg": "OOM",
            "stacktrace": "<stacktrace>"
          },
          "logger": "$className"
        }""")
  }

  def expectedPatternForCustomLogKey(level: Level): String = {
    compactAndToRegexPattern(
      s"""
        {
          "ts": "<timestamp>",
          "level": "$level",
          "msg": "Custom log message.",
          "logger": "$className"
        }"""
    )
  }

  def verifyMsgWithConcat(level: Level, logOutput: String): Unit = {
    val pattern1 = compactAndToRegexPattern(
      s"""
        {
          "ts": "<timestamp>",
          "level": "$level",
          "msg": "Min Size: 2, Max Size: 4. Please double check.",
          "context": {
            "min_size": "2",
            "max_size": "4"
          },
          "logger": "$className"
        }""")

    val pattern2 = compactAndToRegexPattern(
      s"""
        {
          "ts": "<timestamp>",
          "level": "$level",
          "msg": "Min Size: 2, Max Size: 4. Please double check.",
          "context": {
            "max_size": "4",
            "min_size": "2"
          },
          "logger": "$className"
        }""")
    assert(Pattern.compile(pattern1).matcher(logOutput).matches() ||
      Pattern.compile(pattern2).matcher(logOutput).matches())
  }

  test("Basic logging") {
    Seq(
      (Level.ERROR, () => logError(basicMsg)),
      (Level.WARN, () => logWarning(basicMsg)),
      (Level.INFO, () => logInfo(basicMsg))).foreach { case (level, logFunc) =>
      val logOutput = captureLogOutput(logFunc)
      assert(Pattern.compile(expectedPatternForBasicMsg(level)).matcher(logOutput).matches())
    }
  }

  test("Basic logging with Exception") {
    val exception = new RuntimeException("OOM")
    Seq(
      (Level.ERROR, () => logError(basicMsg, exception)),
      (Level.WARN, () => logWarning(basicMsg, exception)),
      (Level.INFO, () => logInfo(basicMsg, exception))).foreach { case (level, logFunc) =>
      val logOutput = captureLogOutput(logFunc)
      assert(
        Pattern.compile(expectedPatternForBasicMsgWithException(level)).matcher(logOutput)
          .matches())
    }
  }

  test("Logging with MDC") {
    Seq(
      (Level.ERROR, () => logError(msgWithMDC)),
      (Level.WARN, () => logWarning(msgWithMDC)),
      (Level.INFO, () => logInfo(msgWithMDC))).foreach {
      case (level, logFunc) =>
        val logOutput = captureLogOutput(logFunc)
        assert(
          Pattern.compile(expectedPatternForMsgWithMDC(level)).matcher(logOutput).matches())
    }
  }

  test("Logging with MDC(the value is null)") {
    Seq(
      (Level.ERROR, () => logError(msgWithMDCValueIsNull)),
      (Level.WARN, () => logWarning(msgWithMDCValueIsNull)),
      (Level.INFO, () => logInfo(msgWithMDCValueIsNull))).foreach {
      case (level, logFunc) =>
        val logOutput = captureLogOutput(logFunc)
        assert(
          Pattern.compile(expectedPatternForMsgWithMDCValueIsNull(level)).matcher(logOutput)
            .matches())
    }
  }

  test("Logging with MDC and Exception") {
    val exception = new RuntimeException("OOM")
    Seq(
      (Level.ERROR, () => logError(msgWithMDCAndException, exception)),
      (Level.WARN, () => logWarning(msgWithMDCAndException, exception)),
      (Level.INFO, () => logInfo(msgWithMDCAndException, exception))).foreach {
      case (level, logFunc) =>
        val logOutput = captureLogOutput(logFunc)
        assert(
          Pattern.compile(expectedPatternForMsgWithMDCAndException(level)).matcher(logOutput)
            .matches())
    }
  }

  test("Logging with custom LogKey") {
    Seq(
      (Level.ERROR, () => logError(customLog)),
      (Level.WARN, () => logWarning(customLog)),
      (Level.INFO, () => logInfo(customLog))).foreach {
      case (level, logFunc) =>
        val logOutput = captureLogOutput(logFunc)
        assert(Pattern.compile(expectedPatternForCustomLogKey(level)).matcher(logOutput).matches())
    }
  }

  test("Logging with concat") {
    Seq(
      (Level.ERROR, () => logError(msgWithConcat)),
      (Level.WARN, () => logWarning(msgWithConcat)),
      (Level.INFO, () => logInfo(msgWithConcat))).foreach {
      case (level, logFunc) =>
        val logOutput = captureLogOutput(logFunc)
        verifyMsgWithConcat(level, logOutput)
    }
  }
}

object CustomLogKeys {
  // Custom `LogKey` must extend LogKey
  case object CUSTOM_LOG_KEY extends DeltaLogKey
}
