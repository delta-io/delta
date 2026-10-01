/*
 * Copyright (2025) The Delta Lake Project Authors.
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
package io.delta.kernel.defaults.metrics

import scala.collection.mutable.ArrayBuffer

import io.delta.kernel.defaults.engine.LoggingMetricsReporter
import io.delta.kernel.metrics.MetricsReport
import io.delta.kernel.shaded.com.fasterxml.jackson.databind.exc.MismatchedInputException
import io.delta.kernel.types.{FieldMetadata, StringType, StructType}

import org.apache.logging.log4j.{Level, LogManager}
import org.apache.logging.log4j.core.{LogEvent, Logger => Log4jLogger}
import org.apache.logging.log4j.core.appender.AbstractAppender
import org.apache.logging.log4j.core.config.Property
import org.scalatest.funsuite.AnyFunSuite

class LoggingMetricsReporterSuite extends AnyFunSuite {

  /** Captures logging events * */
  private class BufferingAppender(name: String)
      extends AbstractAppender(name, null, null, true, Property.EMPTY_ARRAY) {
    val events: ArrayBuffer[LogEvent] = ArrayBuffer.empty[LogEvent]
    override def append(event: LogEvent): Unit = events += event.toImmutable
  }

  private def withCapturedReporterLogger[T](f: BufferingAppender => T): T = {
    val log = LogManager.getLogger("io.delta.kernel.defaults.engine.LoggingMetricsReporter")
      .asInstanceOf[Log4jLogger]
    val app = new BufferingAppender("test-appender")
    app.start()
    val oldLevel = log.getLevel
    try {
      log.addAppender(app)
      log.setLevel(Level.ALL)
      f(app)
    } finally {
      log.removeAppender(app)
      log.setLevel(oldLevel)
      app.stop()
    }
  }

  test("LoggingMetricsReporter successfully logs a metrics report") {
    val fmNull = FieldMetadata.builder().putString("kNull", null).build()
    val fmArray =
      FieldMetadata.builder().putStringArray("arr", Array[String]("x", null, "z")).build()
    val schema = new StructType()
      .add("c1", StringType.STRING, fmNull)
      .add("c2", StringType.STRING, fmArray)
    val schemaStr = schema.toString

    val report = new MetricsReport {
      override def toJson: String = {
        val s = schemaStr.replace("\\", "\\\\").replace("\"", "\\\"")
        s"""{"tableSchema":"$s"}"""
      }
    }

    withCapturedReporterLogger { app =>
      new LoggingMetricsReporter().report(report)
      val msgs = app.events.map(e => (e.getLevel, e.getMessage.getFormattedMessage))
      assert(
        msgs.exists { case (lvl, msg) =>
          lvl == Level.INFO && msg.contains("tableSchema")
        },
        s"Expected INFO log with 'tableSchema' but got: ${msgs.mkString("; ")}")
      assert(
        msgs.exists { case (lvl, msg) =>
          lvl == Level.INFO && msg.contains("kNull=null") && msg.contains("arr=[x, null, z]")
        },
        s"Expected schema with null values and arrays to be serialized correctly")
    }
  }

  test("LoggingMetricsReporter catches and logs kernel-api shaded JsonProcessingException") {
    val shadedThrow = new MetricsReport {
      override def toJson: String = {
        val ex = MismatchedInputException.from(
          null.asInstanceOf[io.delta.kernel.shaded.com.fasterxml.jackson.core.JsonParser],
          classOf[Object],
          "test exception")
        throw ex
      }
    }

    withCapturedReporterLogger { app =>
      val reporter = new LoggingMetricsReporter()
      reporter.report(shadedThrow)
      val msgs = app.events.map(e => (e.getLevel, e.getMessage.getFormattedMessage))
      assert(
        msgs.exists { case (lvl, msg) =>
          lvl == Level.WARN && msg.contains("Serialization issue")
        },
        s"Expected WARN with 'Serialization issue' but got: ${msgs.mkString("; ")}")
    }
  }

  test("LoggingMetricsReporter logs generic Exception at WARN level") {
    val genericThrow = new MetricsReport {
      override def toJson: String = throw new RuntimeException("generic boom")
    }

    withCapturedReporterLogger { app =>
      val reporter = new LoggingMetricsReporter()
      reporter.report(genericThrow)
      val msgs = app.events.map(e => (e.getLevel, e.getMessage.getFormattedMessage))
      assert(
        msgs.exists { case (lvl, msg) =>
          lvl == Level.WARN && msg.contains("Unexpected error")
        },
        s"Expected WARN with 'Unexpected error' but got: ${msgs.mkString("; ")}")
    }
  }
}
