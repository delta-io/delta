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

package org.apache.spark.internal

// MDC is part of Spark's Structured Logging API and is not available in Spark 3.5.
case class MDC(key: LogKeyShims, value: Any) {
  require(!value.isInstanceOf[MessageWithContext],
    "the class of value cannot be MessageWithContext")
}

object MDC {
  def of(key: LogKeyShims, value: Any): MDC = MDC(key, value)
}

// MessageWithContext is part of Spark's Structured Logging API and is not available in Spark 3.5.
case class MessageWithContext(message: String, context: java.util.HashMap[String, String]) {
  def +(mdc: MessageWithContext): MessageWithContext = {
    MessageWithContext(message + mdc.message, new java.util.HashMap[String, String]())
  }

  def stripMargin: MessageWithContext = copy(message = message.stripMargin)
}

// LogEntry is part of Spark's Structured Logging API and is not available in Spark 3.5.
class LogEntry(messageWithContext: => MessageWithContext) {
  def message: String = messageWithContext.message

  def context: java.util.HashMap[String, String] = messageWithContext.context
}

object LogEntry {
  import scala.language.implicitConversions

  implicit def from(msgWithCtx: => MessageWithContext): LogEntry =
    new LogEntry(msgWithCtx)
}

trait LoggingShims extends Logging {
  implicit class LogStringContext(val sc: StringContext) {
    def log(args: MDC*): MessageWithContext = {
      val processedParts = sc.parts.iterator
      val sb = new StringBuilder(processedParts.next())

      args.foreach { mdc =>
        val value = if (mdc.value != null) mdc.value.toString else null
        sb.append(value)

        if (processedParts.hasNext) {
          sb.append(processedParts.next())
        }
      }

      MessageWithContext(sb.toString(), new java.util.HashMap[String, String]())
    }
  }

  protected def logInfo(entry: LogEntry): Unit = {
    if (log.isInfoEnabled) {
      log.info(entry.message)
    }
  }

  protected def logInfo(entry: LogEntry, throwable: Throwable): Unit = {
    if (log.isInfoEnabled) {
      log.info(entry.message, throwable)
    }
  }

  protected def logDebug(entry: LogEntry): Unit = {
    if (log.isDebugEnabled) {
      log.debug(entry.message)
    }
  }

  protected def logDebug(entry: LogEntry, throwable: Throwable): Unit = {
    if (log.isDebugEnabled) {
      log.debug(entry.message, throwable)
    }
  }

  protected def logTrace(entry: LogEntry): Unit = {
    if (log.isTraceEnabled) {
      log.trace(entry.message)
    }
  }

  protected def logTrace(entry: LogEntry, throwable: Throwable): Unit = {
    if (log.isTraceEnabled) {
      log.trace(entry.message, throwable)
    }
  }

  protected def logWarning(entry: LogEntry): Unit = {
    if (log.isWarnEnabled) {
      log.warn(entry.message)
    }
  }

  protected def logWarning(entry: LogEntry, throwable: Throwable): Unit = {
    if (log.isWarnEnabled) {
      log.warn(entry.message, throwable)
    }
  }

  protected def logError(entry: LogEntry): Unit = {
    if (log.isErrorEnabled) {
      log.error(entry.message)
    }
  }

  protected def logError(entry: LogEntry, throwable: Throwable): Unit = {
    if (log.isErrorEnabled) {
      log.error(entry.message, throwable)
    }
  }
}
