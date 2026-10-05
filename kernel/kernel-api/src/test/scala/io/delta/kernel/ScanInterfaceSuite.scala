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

package io.delta.kernel

import java.util.{Collections, Optional}

import io.delta.kernel.data.{FilteredColumnarBatch, Row}
import io.delta.kernel.engine.Engine
import io.delta.kernel.expressions.Predicate
import io.delta.kernel.internal.util.Utils
import io.delta.kernel.utils.CloseableIterator

import org.scalatest.funsuite.AnyFunSuite

/**
 * Tests for default methods on the [[Scan]] interface. Uses a bare anonymous implementation
 * (not [[io.delta.kernel.internal.ScanImpl]]) to exercise the default method paths.
 */
class ScanInterfaceSuite extends AnyFunSuite {

  /** Sentinel iterator returned by the anonymous Scan to verify delegation. */
  private val sentinelIterator: CloseableIterator[FilteredColumnarBatch] =
    Utils.toCloseableIterator(
      Collections.emptyIterator[FilteredColumnarBatch]())

  private val bareScan: Scan = new Scan {
    override def getScanFiles(engine: Engine): CloseableIterator[FilteredColumnarBatch] =
      sentinelIterator

    override def getRemainingFilter(): Optional[Predicate] = Optional.empty()

    override def getScanState(engine: Engine): Row =
      throw new UnsupportedOperationException("not needed for this test")
  }

  test("getScanFiles with includeStats=false delegates to getScanFiles(Engine)") {
    val result = bareScan.getScanFiles(null, false)
    assert(result eq sentinelIterator, "expected delegation to getScanFiles(Engine)")
  }

  test("getScanFiles with includeStats=true throws UnsupportedOperationException") {
    val ex = intercept[UnsupportedOperationException] {
      bareScan.getScanFiles(null, true)
    }
    assert(ex.getMessage.contains("includeStats=true"))
  }
}
