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
package io.delta.kernel.exceptions

import java.util.Optional

import scala.collection.JavaConverters._

import io.delta.kernel.exceptions.UnsupportedProtocolVersionException.ProtocolVersionType

import org.scalatest.funsuite.AnyFunSuite

/**
 * Unit tests for Kernel exception types.
 */
class ExceptionSuite extends AnyFunSuite {

  test("UnsupportedReaderFeatureException - basic functionality") {
    val tablePath = "/path/to/table"
    val features = Set("feature1", "feature2").asJava

    val ex = new UnsupportedReaderFeatureException(tablePath, features)

    assert(ex.getTablePath == tablePath)
    assert(ex.getUnsupportedFeatures.asScala == Set("feature1", "feature2"))
    assert(ex.getMessage.contains("reader table features"))
    assert(ex.getMessage.contains("feature1"))
    assert(ex.getMessage.contains("feature2"))
    assert(ex.isInstanceOf[UnsupportedTableFeatureException])
  }

  test("UnsupportedWriterFeatureException - basic functionality") {
    val tablePath = "/path/to/table"
    val features = Set("writerFeature").asJava

    val ex = new UnsupportedWriterFeatureException(tablePath, features)

    assert(ex.getTablePath == tablePath)
    assert(ex.getUnsupportedFeatures.asScala == Set("writerFeature"))
    assert(ex.getMessage.contains("writer table features"))
    assert(ex.getMessage.contains("writerFeature"))
    assert(ex.isInstanceOf[UnsupportedTableFeatureException])
  }

  test("UnsupportedTableFeatureException - custom message") {
    val tablePath = "/path/to/table"
    val feature = "singleFeature"

    val ex = new UnsupportedTableFeatureException(tablePath, feature, "Custom message")

    assert(ex.getTablePath == tablePath)
    assert(ex.getUnsupportedFeatures.asScala == Set(feature))
    assert(ex.getMessage == "Custom message")
  }

  test("UnsupportedTableFeatureException - null features returns empty set") {
    val ex = new UnsupportedTableFeatureException(
      "/table",
      null.asInstanceOf[java.util.Set[String]],
      "Message")

    assert(ex.getUnsupportedFeatures.isEmpty)
  }

  test("UnsupportedTableFeatureException - returned set is unmodifiable") {
    val features = new java.util.HashSet[String]()
    features.add("feature1")
    val ex = new UnsupportedTableFeatureException("/table", features, "Message")

    intercept[UnsupportedOperationException] {
      ex.getUnsupportedFeatures.add("feature2")
    }
  }

  test("UnsupportedProtocolVersionException - reader version") {
    val tablePath = "/path/to/table"
    val version = 3

    val ex = new UnsupportedProtocolVersionException(
      tablePath,
      version,
      ProtocolVersionType.READER)

    assert(ex.getTablePath == tablePath)
    assert(ex.getVersion == version)
    assert(ex.getVersionType == ProtocolVersionType.READER)
    assert(ex.getMessage.contains("reader"))
    assert(ex.getMessage.contains("version 3"))
  }

  test("UnsupportedProtocolVersionException - writer version") {
    val tablePath = "/path/to/table"
    val version = 7

    val ex = new UnsupportedProtocolVersionException(
      tablePath,
      version,
      ProtocolVersionType.WRITER)

    assert(ex.getTablePath == tablePath)
    assert(ex.getVersion == version)
    assert(ex.getVersionType == ProtocolVersionType.WRITER)
    assert(ex.getMessage.contains("writer"))
    assert(ex.getMessage.contains("version 7"))
  }

  test("CommitRangeNotFoundException - with end version") {
    val tablePath = "/path/to/table"
    val startVersion = 5L
    val endVersion = Optional.of(java.lang.Long.valueOf(10L))

    val ex = new CommitRangeNotFoundException(tablePath, startVersion, endVersion)

    assert(ex.getTablePath == tablePath)
    assert(ex.getStartVersion == startVersion)
    assert(ex.getEndVersion == endVersion)
    assert(ex.getMessage.contains("Requested table changes between [5, Optional[10]]"))
    assert(ex.getMessage.contains("no log files found"))
  }

  test("CommitRangeNotFoundException - without end version") {
    val tablePath = "/path/to/table"
    val startVersion = 100L
    val endVersion = Optional.empty[java.lang.Long]()

    val ex = new CommitRangeNotFoundException(tablePath, startVersion, endVersion)

    assert(ex.getTablePath == tablePath)
    assert(ex.getStartVersion == startVersion)
    assert(!ex.getEndVersion.isPresent)
    assert(ex.getMessage.contains("Requested table changes between [100, Optional.empty]"))
  }

  test("exception hierarchy - all extend KernelException") {
    assert(classOf[UnsupportedTableFeatureException].getSuperclass == classOf[KernelException])
    assert(classOf[UnsupportedReaderFeatureException].getSuperclass ==
      classOf[UnsupportedTableFeatureException])
    assert(classOf[UnsupportedWriterFeatureException].getSuperclass ==
      classOf[UnsupportedTableFeatureException])
    assert(classOf[UnsupportedProtocolVersionException].getSuperclass ==
      classOf[KernelException])
    assert(classOf[CommitRangeNotFoundException].getSuperclass == classOf[KernelException])
    assert(classOf[KernelException].getSuperclass == classOf[RuntimeException])
  }

  test("ProtocolVersionType enum - basic functionality") {
    // Test enum values
    assert(ProtocolVersionType.values().length == 2)
    assert(ProtocolVersionType.valueOf("READER") == ProtocolVersionType.READER)
    assert(ProtocolVersionType.valueOf("WRITER") == ProtocolVersionType.WRITER)

    // Test that it can be used in pattern matching
    val versionType = ProtocolVersionType.READER
    val result = versionType match {
      case ProtocolVersionType.READER => "reader"
      case ProtocolVersionType.WRITER => "writer"
    }
    assert(result == "reader")
  }

  test("exception messages are informative") {
    val readerEx = new UnsupportedReaderFeatureException(
      "/table",
      Set("deletionVectors", "columnMapping").asJava)
    assert(readerEx.getMessage.contains("deletionVectors"))
    assert(readerEx.getMessage.contains("columnMapping"))
    assert(readerEx.getMessage.contains("/table"))

    val protocolEx = new UnsupportedProtocolVersionException(
      "/another/table",
      5,
      ProtocolVersionType.WRITER)
    assert(protocolEx.getMessage.contains("/another/table"))
    assert(protocolEx.getMessage.contains("5"))
    assert(protocolEx.getMessage.contains("writer"))

    val commitRangeEx = new CommitRangeNotFoundException(
      "/third/table",
      10,
      Optional.of(20L))
    assert(commitRangeEx.getMessage.contains("/third/table"))
    assert(commitRangeEx.getMessage.contains("10"))
    assert(commitRangeEx.getMessage.contains("20"))
  }
}
