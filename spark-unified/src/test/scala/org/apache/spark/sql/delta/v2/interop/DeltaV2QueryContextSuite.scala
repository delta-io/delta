/*
 * Copyright (2026) The Delta Lake Project Authors.
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

package org.apache.spark.sql.delta.v2.interop

import java.util.Optional

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable, CatalogTableType}
import org.apache.spark.sql.types.StructType

class DeltaV2QueryContextSuite extends SparkFunSuite {

  test("empty query context has no catalog metadata") {
    assert(DeltaV2QueryContext.empty.catalogTableOpt.isEmpty)
  }

  test("Java catalog metadata is preserved by identity") {
    val catalogTable = CatalogTable(
      identifier = TableIdentifier("test_table"),
      tableType = CatalogTableType.EXTERNAL,
      storage = CatalogStorageFormat.empty,
      schema = StructType(Nil))

    val queryContext = DeltaV2QueryContext.fromJava(Optional.of(catalogTable))

    assert(queryContext.catalogTableOpt.exists(_ eq catalogTable))
    assert(DeltaV2QueryContext.fromJava(Optional.empty()).catalogTableOpt.isEmpty)
  }

  test("null context inputs are rejected") {
    intercept[NullPointerException] {
      DeltaV2QueryContext(null)
    }
    intercept[NullPointerException] {
      DeltaV2QueryContext.fromJava(null)
    }
  }
}
