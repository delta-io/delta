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

package io.delta.kernel.internal.util

import io.delta.kernel.exceptions.KernelException
import org.scalatest.funsuite.AnyFunSuite

import scala.collection.JavaConverters._

class JsonUtilsSuite extends AnyFunSuite {
  test("Parse Map[String, String] JSON - positive case") {
    val expMap = Map("key1" -> "string_value", "key2Int" -> "2", "key3ComplexStr" -> "\"hello\"")
    val input = """{"key1": "string_value", "key2Int": "2", "key3ComplexStr": "\"hello\""}"""
    assert(JsonUtils.parseJSONKeyValueMap(input) === expMap.asJava)

    assert(JsonUtils.parseJSONKeyValueMap("").isEmpty)
    assert(JsonUtils.parseJSONKeyValueMap(null).isEmpty)
  }

  test("Parse Map[String, String] JSON - negative case") {
    val e = intercept[KernelException] {
      JsonUtils.parseJSONKeyValueMap("""{"key1": "string_value", asdf"}""")
    }
    assert(e.getMessage.contains("Failed to parse JSON string:"))
  }
}
