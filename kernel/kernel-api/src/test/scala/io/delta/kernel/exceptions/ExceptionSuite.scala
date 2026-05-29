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
import io.delta.kernel.internal.DeltaErrors

import org.scalatest.funsuite.AnyFunSuite

/**
 * Unit tests for Kernel exception types.
 */
class ExceptionSuite extends AnyFunSuite {

  test("UnsupportedReaderFeatureException - basic functionality") {
    val tablePath = "/path/to/table"
    val features = Set("feature1", "feature2").asJava

    val ex = DeltaErrors.unsupportedReaderFeatures(tablePath, features)

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

    val ex = DeltaErrors.unsupportedWriterFeatures(tablePath, features)

    assert(ex.getTablePath == tablePath)
    assert(ex.getUnsupportedFeatures.asScala == Set("writerFeature"))
    assert(ex.getMessage.contains("writer table features"))
    assert(ex.getMessage.contains("writerFeature"))
    assert(ex.isInstanceOf[UnsupportedTableFeatureException])
  }

  test("UnsupportedProtocolVersionException - reader version") {
    val tablePath = "/path/to/table"
    val version = 3

    val ex = DeltaErrors.unsupportedReaderProtocol(tablePath, version)

    assert(ex.getTablePath == tablePath)
    assert(ex.getVersion == version)
    assert(ex.getVersionType == ProtocolVersionType.READER)
    assert(ex.getMessage.contains("reader"))
    assert(ex.getMessage.contains("version 3"))
  }

  test("UnsupportedProtocolVersionException - writer version") {
    val tablePath = "/path/to/table"
    val version = 7

    val ex = DeltaErrors.unsupportedWriterProtocol(tablePath, version)

    assert(ex.getTablePath == tablePath)
    assert(ex.getVersion == version)
    assert(ex.getVersionType == ProtocolVersionType.WRITER)
    assert(ex.getMessage.contains("writer"))
    assert(ex.getMessage.contains("version 7"))
  }

  test("CommitRangeNotFoundException - with start and end version") {
    val tablePath = "/path/to/table"
    val startVersion = 5L
    val endVersion = Optional.of(java.lang.Long.valueOf(10L))

    val ex = DeltaErrors.noCommitFilesFoundForVersionRange(tablePath, startVersion, endVersion)

    assert(ex.getTablePath == tablePath)
    assert(ex.getStartVersion == startVersion)
    assert(ex.getEndVersion == endVersion)
    assert(ex.getMessage.contains("Requested table changes between [5, Optional[10]]"))
    assert(ex.getMessage.contains("no log files found"))
  }

  // KernelEngineException and KernelException are siblings (both extend RuntimeException), not
  // parent/child. Without an explicit catch clause, wrapEngineException's `catch (KernelException)`
  // fast-path doesn't match a KernelEngineException, so the outer call re-wraps an already-wrapped
  // engine exception into another KernelEngineException - hiding the original cause one extra
  // level deep and breaking consumers that intentionally inspect only the direct cause, such as
  // Spark streaming interrupt handling. These tests pin the "don't double-wrap" behavior.
  test("wrapEngineException does not double-wrap an already-wrapped KernelEngineException") {
    val root = new ClassCastException("root cause")
    val inner = new KernelEngineException("inner op", root)

    val outer = intercept[KernelEngineException] {
      DeltaErrors.wrapEngineException[Unit](
        () => throw inner,
        "outer op")
    }

    // Must be the same instance - not re-wrapped.
    assert(outer eq inner)
    assert(outer.getCause eq root)
  }

  test(
    "wrapEngineExceptionThrowsIO does not double-wrap an already-wrapped KernelEngineException") {
    val root = new java.nio.channels.ClosedByInterruptException()
    val inner = new KernelEngineException("inner op", root)

    val outer = intercept[KernelEngineException] {
      DeltaErrors.wrapEngineExceptionThrowsIO[Unit](
        () => throw inner,
        "outer op")
    }

    assert(outer eq inner)
    assert(outer.getCause eq root)
  }

  test("wrapEngineException rethrows KernelException as the same instance") {
    val inner = new KernelException("kernel error")

    val outer = intercept[KernelException] {
      DeltaErrors.wrapEngineException[Unit](
        () => throw inner,
        "outer op")
    }

    assert(outer eq inner)
  }

  test("wrapEngineExceptionThrowsIO rethrows KernelException as the same instance") {
    val inner = new KernelException("kernel error")

    val outer = intercept[KernelException] {
      DeltaErrors.wrapEngineExceptionThrowsIO[Unit](
        () => throw inner,
        "outer op")
    }

    assert(outer eq inner)
  }

  test("wrapEngineException still wraps plain RuntimeException") {
    val root = new IllegalStateException("some engine bug")

    val wrapped = intercept[KernelEngineException] {
      DeltaErrors.wrapEngineException[Unit](
        () => throw root,
        "outer op %s",
        "arg")
    }

    assert(wrapped.getCause eq root)
    assert(wrapped.getMessage.contains("outer op arg"))
  }

  test("wrapEngineExceptionThrowsIO still wraps plain RuntimeException") {
    val root = new IllegalStateException("some engine bug")

    val wrapped = intercept[KernelEngineException] {
      DeltaErrors.wrapEngineExceptionThrowsIO[Unit](
        () => throw root,
        "outer op %s",
        "arg")
    }

    assert(wrapped.getCause eq root)
    assert(wrapped.getMessage.contains("outer op arg"))
  }
}
