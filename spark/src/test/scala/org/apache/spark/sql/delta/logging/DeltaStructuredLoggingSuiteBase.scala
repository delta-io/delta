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

import org.apache.logging.log4j.Level

import org.apache.spark.SparkFunSuite
import org.apache.spark.internal.{LogEntry, LoggingShims, LogKeyShims, MDC}

trait DeltaStructuredLoggingSuiteBase
  extends SparkFunSuite
    with LoggingShims {
  def className: String
  def logFilePath: String

  private lazy val logFile: File = {
    val pwd = new File(".").getCanonicalPath
    new File(pwd + "/" + logFilePath)
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

  def basicMsg: String = "This is a log message"

  def msgWithMDC: LogEntry = log"Lost executor ${MDC(DeltaLogKeys.EXECUTOR_ID, "1")}."

  def msgWithMDCValueIsNull: LogEntry = log"Lost executor ${MDC(DeltaLogKeys.EXECUTOR_ID, null)}."

  def msgWithMDCAndException: LogEntry =
    log"Error in executor ${MDC(DeltaLogKeys.EXECUTOR_ID, "1")}."

  def msgWithConcat: LogEntry = log"Min Size: ${MDC(DeltaLogKeys.MIN_SIZE, "2")}, " +
    log"Max Size: ${MDC(DeltaLogKeys.MAX_SIZE, "4")}. " +
    log"Please double check."

  // test for basic message (without any mdc)
  def expectedPatternForBasicMsg(level: Level): String

  // test for basic message and exception
  def expectedPatternForBasicMsgWithException(level: Level): String

  // test for message (with mdc)
  def expectedPatternForMsgWithMDC(level: Level): String

  // test for message (with mdc - the value is null)
  def expectedPatternForMsgWithMDCValueIsNull(level: Level): String

  // test for message and exception
  def expectedPatternForMsgWithMDCAndException(level: Level): String

  // test for custom LogKey
  def expectedPatternForCustomLogKey(level: Level): String

  def verifyMsgWithConcat(level: Level, logOutput: String): Unit

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

  private val customLog = log"${MDC(CustomLogKeys.CUSTOM_LOG_KEY, "Custom log message.")}"
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
  // Custom `LogKey` must be `extends LogKeyShims`
  case object CUSTOM_LOG_KEY extends LogKeyShims
}
