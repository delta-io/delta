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

package io.delta.internal

import io.delta.spark.internal.v2.catalog.SparkTable
import org.apache.spark.sql.delta.streaming.V2StreamingSchemaReorder
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest

/**
 * Compile-time canary for [[V2StreamingSchemaReorder.SparkTableClassName]]. The rule scopes
 * itself to kernel-spark `SparkTable` via a runtime class-name string because sparkV1 cannot
 * compile-time-depend on sparkV2. spark-unified's test classpath has both modules, so this
 * suite can hold the typed reference and fail loudly if the FQCN drifts.
 */
class V2StreamingSchemaReorderSuite extends DeltaSQLCommandTest {

  test("SparkTableClassName matches the actual SparkTable FQCN") {
    assert(V2StreamingSchemaReorder.SparkTableClassName == classOf[SparkTable].getName)
  }
}
