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

package io.delta.kernel.internal.actions

import org.scalatest.funsuite.AnyFunSuite

import scala.collection.JavaConverters._

class ProtocolSuite extends AnyFunSuite {

  test("instantiation -- bad minReaderVersion should throw") {
    val exMsg = intercept[IllegalArgumentException] {
      new Protocol(0, 1, null, null)
    }.getMessage

    assert(exMsg === "minReaderVersion must be >= 1")
  }

  test("instantiation -- bad minWriterVersion should throw") {
    val exMsg = intercept[IllegalArgumentException] {
      new Protocol(1, 0, null, null)
    }.getMessage

    assert(exMsg === "minWriterVersion must be >= 1")
  }

  test("instantiation -- minReaderVersion < 3 but readerFeatures non-empty should throw") {
    val exMsg = intercept[IllegalArgumentException] {
      new Protocol(1, 1, Set("columnMapping").asJava, null)
    }.getMessage

    assert(exMsg === "This protocol has minReaderVersion 1 but readerFeatures is not " +
      "empty: [columnMapping]. readerFeatures are only supported with minReaderVersion >= 3.")
  }

  test("instantiation -- minWriterVersion < 7 but writerFeatures non-empty should throw") {
    val exMsg = intercept[IllegalArgumentException] {
      new Protocol(1, 1, null, Set("appendOnly").asJava)
    }.getMessage

    assert(exMsg === "This protocol has minWriterVersion 1 but writerFeatures is not " +
      "empty: [appendOnly]. writerFeatures are only supported with minWriterVersion >= 7.")
  }

  test("instantiation -- minReaderVersion >= 3 but minWriterVersion < 7 should throw") {
    val exMsg = intercept[IllegalArgumentException] {
      new Protocol(3, 6, null, null)
    }.getMessage

    assert(exMsg === "This protocol has minReaderVersion 3 but minWriterVersion 6. When " +
      "minReaderVersion is >= 3, minWriterVersion must be >= 7.")
  }

}
