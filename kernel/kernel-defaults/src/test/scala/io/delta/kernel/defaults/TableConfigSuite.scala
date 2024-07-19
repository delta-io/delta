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
package io.delta.kernel.defaults

import io.delta.kernel.internal.actions.{Format, Metadata}
import io.delta.kernel.internal.util.VectorUtils
import io.delta.kernel.internal.util.VectorUtils.stringArrayValue
import io.delta.kernel.internal.TableConfig

import java.util
import java.util.{Collections, Optional}

import scala.collection.JavaConverters._

class TableConfigSuite extends DeltaTableWriteSuiteBase {
  def getEmptyMetadata: Metadata = {
    new Metadata(
      util.UUID.randomUUID().toString,
      Optional.empty(),
      Optional.empty(),
      new Format(),
      "",
      null,
      stringArrayValue(Collections.emptyList()),
      Optional.empty(),
      VectorUtils.stringStringMapValue(Collections.emptyMap())
    )
  }

  test("Coordinated Commit Related Properties from Metadata") {
    withTempDirAndEngine { (tablePath, engine) =>
      val m1 = getEmptyMetadata.withNewConfiguration(
        Map(
          TableConfig.COORDINATED_COMMITS_COORDINATOR_NAME.getKey -> "cc-name",
          TableConfig.COORDINATED_COMMITS_COORDINATOR_CONF.getKey ->
            """{"key1": "string_value", "key2Int": "2", "key3ComplexStr": "\"hello\""}""",
          TableConfig.COORDINATED_COMMITS_TABLE_CONF.getKey ->
            """{"key1": "string_value", "key2Int": "2", "key3ComplexStr": "\"hello\""}""").asJava
      )
      assert(TableConfig.COORDINATED_COMMITS_COORDINATOR_NAME.fromMetadata(engine, m1) ===
        Optional.of("cc-name"))

      assert(TableConfig.COORDINATED_COMMITS_COORDINATOR_CONF.fromMetadata(engine, m1) ===
        Map("key1" -> "string_value", "key2Int" -> "2", "key3ComplexStr" -> "\"hello\"").asJava)

      assert(TableConfig.COORDINATED_COMMITS_TABLE_CONF.fromMetadata(engine, m1) ===
        Map("key1" -> "string_value", "key2Int" -> "2", "key3ComplexStr" -> "\"hello\"").asJava)

      val m2 = getEmptyMetadata.withNewConfiguration(
        Map(
          TableConfig.COORDINATED_COMMITS_COORDINATOR_NAME.getKey -> null,
          TableConfig.COORDINATED_COMMITS_COORDINATOR_CONF.getKey ->
            """{"key1": "string_value", "key2Int": "2""",
          TableConfig.COORDINATED_COMMITS_TABLE_CONF.getKey ->
            """{"key1": "string_value", "key2Int": "2""").asJava
      )
      assert(TableConfig.COORDINATED_COMMITS_COORDINATOR_NAME.fromMetadata(engine, m2) ===
        Optional.empty())
      intercept[RuntimeException] {
        TableConfig.COORDINATED_COMMITS_COORDINATOR_CONF.fromMetadata(engine, m2)
      }
      intercept[RuntimeException] {
        TableConfig.COORDINATED_COMMITS_TABLE_CONF.fromMetadata(engine, m2)
      }

      val m3 = getEmptyMetadata.withNewConfiguration(
        Map(
          TableConfig.COORDINATED_COMMITS_COORDINATOR_NAME.getKey -> "cc-name",
          TableConfig.COORDINATED_COMMITS_COORDINATOR_CONF.getKey -> """{"key1": -1}""",
          TableConfig.COORDINATED_COMMITS_TABLE_CONF.getKey -> """{"key1": false}""").asJava
      )
      val e1 = intercept[RuntimeException] {
        TableConfig.COORDINATED_COMMITS_COORDINATOR_CONF.fromMetadata(engine, m3)
      }
      val e2 = intercept[RuntimeException] {
        TableConfig.COORDINATED_COMMITS_TABLE_CONF.fromMetadata(engine, m3)
      }
      assert(e1.getMessage.contains("Couldn't decode -1, expected a string"))
      assert(e2.getMessage.contains("Couldn't decode false, expected a string"))
    }
  }
}
