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

package io.delta.kernel.commit

import scala.collection.JavaConverters._

import io.delta.kernel.internal.actions.Protocol

import org.scalatest.funsuite.AnyFunSuite

class CatalogCommitterUtilsSuite extends AnyFunSuite {

  test("extractProtocolProperties - legacy protocol (1, 2)") {
    // ===== GIVEN =====
    val protocol = new Protocol(1, 2)

    // ===== WHEN =====
    val properties = CatalogCommitterUtils.extractProtocolProperties(protocol).asScala

    // ===== THEN =====
    assert(properties.size === 2)
    assert(properties("delta.minReaderVersion") === "1")
    assert(properties("delta.minWriterVersion") === "2")
  }

  test("extractProtocolProperties - protocol with overlapping reader and writer features") {
    // ===== GIVEN =====
    val readerFeatures = Set("columnMapping", "deletionVectors")
    val writerFeatures = Set("columnMapping", "appendOnly") // Note: columnMapping overlaps
    val protocol = new Protocol(3, 7, readerFeatures.asJava, writerFeatures.asJava)

    // ===== WHEN =====
    val properties = CatalogCommitterUtils.extractProtocolProperties(protocol).asScala

    // ===== THEN =====
    assert(properties.size === 2 + 3) // minReader + minWriter + 3 unique features
    assert(properties("delta.minReaderVersion") === "3")
    assert(properties("delta.minWriterVersion") === "7")
    assert(properties("delta.feature.columnMapping") === "supported")
    assert(properties("delta.feature.deletionVectors") === "supported")
    assert(properties("delta.feature.appendOnly") === "supported")
  }
}
