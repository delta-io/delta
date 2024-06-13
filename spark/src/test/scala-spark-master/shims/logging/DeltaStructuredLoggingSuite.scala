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

import java.util.regex.Pattern

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.logging.log4j.Level

class DeltaStructuredLoggingSuite extends DeltaStructuredLoggingSuiteBase {
  override def className: String = classOf[DeltaStructuredLoggingSuite].getSimpleName
  override def logFilePath: String = "target/structured.log"

  private val jsonMapper = new ObjectMapper().registerModule(DefaultScalaModule)
  private def compactAndToRegexPattern(json: String): String = {
    jsonMapper.readTree(json).toString.
      replace("<timestamp>", """[^"]+""").
      replace(""""<stacktrace>"""", """.*""").
      replace("{", """\{""") + "\n"
  }

  override def expectedPatternForBasicMsg(level: Level): String = {
    compactAndToRegexPattern(
      s"""
        {
          "ts": "<timestamp>",
          "level": "$level",
          "msg": "This is a log message",
          "logger": "$className"
        }""")
  }

  override def expectedPatternForBasicMsgWithException(level: Level): String = {
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

  override def expectedPatternForMsgWithMDC(level: Level): String = {
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

  override def expectedPatternForMsgWithMDCAndException(level: Level): String = {
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

  override def expectedPatternForCustomLogKey(level: Level): String = {
    compactAndToRegexPattern(
      s"""
        {
          "ts": "<timestamp>",
          "level": "$level",
          "msg": "Custom log message.",
          "context": {
              "custom_log_key": "Custom log message."
          },
          "logger": "$className"
        }"""
    )
  }

  override def verifyMsgWithConcat(level: Level, logOutput: String): Unit = {
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
}
