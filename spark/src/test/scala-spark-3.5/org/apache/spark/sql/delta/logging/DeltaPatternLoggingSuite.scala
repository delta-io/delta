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

import org.apache.logging.log4j.Level

class DeltaPatternLoggingSuite extends DeltaStructuredLoggingSuiteBase {
  override def className: String = classOf[DeltaPatternLoggingSuite].getSimpleName
  override def logFilePath: String = "target/pattern.log"

  override def expectedPatternForBasicMsg(level: Level): String = {
    s""".*$level $className: This is a log message\n"""
  }

  override def expectedPatternForBasicMsgWithException(level: Level): String = {
    s""".*$level $className: This is a log message\n[\\s\\S]*"""
  }

  override def expectedPatternForMsgWithMDC(level: Level): String =
    s""".*$level $className: Lost executor 1.\n"""

  override def expectedPatternForMsgWithMDCValueIsNull(level: Level): String =
    s""".*$level $className: Lost executor null.\n"""

  override def expectedPatternForMsgWithMDCAndException(level: Level): String =
    s""".*$level $className: Error in executor 1.\njava.lang.RuntimeException: OOM\n[\\s\\S]*"""

  override def expectedPatternForCustomLogKey(level: Level): String = {
    s""".*$level $className: Custom log message.\n"""
  }

  override def verifyMsgWithConcat(level: Level, logOutput: String): Unit = {
    val pattern =
      s""".*$level $className: Min Size: 2, Max Size: 4. Please double check.\n"""
    assert(Pattern.compile(pattern).matcher(logOutput).matches())
  }
}
