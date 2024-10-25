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
package io.delta.kernel.internal

import io.delta.kernel.exceptions.KernelException
import org.scalatest.funsuite.AnyFunSuite

import scala.collection.JavaConverters._

class TableConfigSuite extends AnyFunSuite {

  test("Parse Map[String, String] type table config") {
    val expMap = Map("key1" -> "string_value", "key2Int" -> "2", "key3ComplexStr" -> "\"hello\"")
    val input = """{"key1": "string_value", "key2Int": "2", "key3ComplexStr": "\"hello\""}"""
    assert(TableConfig.parseJSONKeyValueMap(input) === expMap.asJava)
  }

  test("check TableConfig.editable is true") {
    TableConfig.validateProperties(
      Map(
        TableConfig.TOMBSTONE_RETENTION.getKey -> "interval 2 week",
        TableConfig.CHECKPOINT_INTERVAL.getKey -> "20",
        TableConfig.IN_COMMIT_TIMESTAMPS_ENABLED.getKey -> "true",
        TableConfig.IN_COMMIT_TIMESTAMP_ENABLEMENT_VERSION.getKey -> "1",
        TableConfig.IN_COMMIT_TIMESTAMP_ENABLEMENT_TIMESTAMP.getKey -> "1",
        TableConfig.COORDINATED_COMMITS_COORDINATOR_NAME.getKey -> "{in-memory}",
        TableConfig.COORDINATED_COMMITS_COORDINATOR_CONF.getKey -> "{\"1\": \"1\"}",
        TableConfig.COORDINATED_COMMITS_TABLE_CONF.getKey -> "{\"1\": \"1\"}",
        TableConfig.COLUMN_MAPPING_MODE.getKey -> "name",
        TableConfig.ICEBERG_COMPAT_V2_ENABLED.getKey -> "true").asJava)
  }

  test("check TableConfig.MAX_COLUMN_ID.editable is false") {
    val e = intercept[KernelException] {
      TableConfig.validateProperties(
        Map(
          TableConfig.TOMBSTONE_RETENTION.getKey -> "interval 2 week",
          TableConfig.CHECKPOINT_INTERVAL.getKey -> "20",
          TableConfig.IN_COMMIT_TIMESTAMPS_ENABLED.getKey -> "true",
          TableConfig.COLUMN_MAPPING_MAX_COLUMN_ID.getKey -> "10").asJava)
    }

    assert(e.isInstanceOf[KernelException])
    assert(e.getMessage ===
      s"The Delta table property " +
      s"'${TableConfig.COLUMN_MAPPING_MAX_COLUMN_ID.getKey}'" +
      s" is an internal property and cannot be updated.")
  }
}
