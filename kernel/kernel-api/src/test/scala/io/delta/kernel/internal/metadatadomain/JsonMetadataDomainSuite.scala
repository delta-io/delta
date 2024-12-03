/*
 * Copyright (2024) The Delta Lake Project Authors.
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
package io.delta.kernel.internal.metadatadomain

import io.delta.kernel.internal.rowtracking.RowTrackingMetadataDomain
import org.scalatest.funsuite.AnyFunSuite

import java.util.Optional

class JsonMetadataDomainSuite extends AnyFunSuite {

  test("JsonMetadataDomain can serialize/deserialize Optional fields") {
    // TestJsonMetadataDomain has two Optional<String> fields and one int primitive field
    val testMetadataDomain = new TestJsonMetadataDomain(Optional.of("value1"), Optional.empty(), 10)

    // Test the serialization, empty Optional fields should be omitted
    val config = testMetadataDomain.toJsonConfiguration
    assert(config === """{"field1":"value1","field3":10}""")

    // Test the deserialization, missing Optional fields should be initialized as empty Optional
    val deserializedDomain = TestJsonMetadataDomain.fromJsonConfiguration(config)
    assert(deserializedDomain.getField1.isPresent && deserializedDomain.getField1.get === "value1")
    assert(!deserializedDomain.getField2.isPresent)
    assert(deserializedDomain.getField3 === 10)
  }

  test("JsonMetadataDomain deserialization can handle the extra 'domainName' field") {
    // Delta Spark has a bug where the serialized JSON includes an unintended 'domainName' field.
    // Delta Kernel can gracefully handle this because 'domainName' is annotated with @JsonIgnore,
    // so this field is ignored if encountered without throwing exception.

    // This test explicitly verifies that the deserialization can handle input JSON both
    // with and without the 'domainName' field.

    // Test with TestJsonMetadataDomain
    val testJson1 = """{"field3":10}"""
    val testJson2 = """{"domainName":"testDomain","field3":10}"""

    val testMD1 = TestJsonMetadataDomain.fromJsonConfiguration(testJson1)
    val testMD2 = TestJsonMetadataDomain.fromJsonConfiguration(testJson2)

    assert(!testMD1.getField1.isPresent)
    assert(!testMD1.getField2.isPresent)
    assert(testMD1.getField3 === 10)
    assert(testMD1 === testMD2)

    // Also test with a concrete metadata domain - RowTrackingMetadataDomain
    val rowTrackingJson1 = """{"rowIdHighWaterMark":10}"""
    val rowTrackingJson2 = """{"domainName":"delta.rowTracking","rowIdHighWaterMark":10}"""

    val rowTrackingMD1 = RowTrackingMetadataDomain.fromJsonConfiguration(rowTrackingJson1)
    val rowTrackingMD2 = RowTrackingMetadataDomain.fromJsonConfiguration(rowTrackingJson2)

    assert(rowTrackingMD1.getRowIdHighWaterMark === 10)
    assert(rowTrackingMD1 === rowTrackingMD2)
  }
}
