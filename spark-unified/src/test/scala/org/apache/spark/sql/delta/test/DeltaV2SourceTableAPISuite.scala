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

package org.apache.spark.sql.delta.test

import org.apache.spark.sql.delta.DeltaSourceTableAPISuite

/** Runs the streaming source tests from [[DeltaSourceTableAPISuite]] through the V2 connector. */
class DeltaV2SourceTableAPISuite extends DeltaSourceTableAPISuite with V2ForceTest {

  override protected def assertNoV1Fallback: Boolean = true

  override protected def withV1Mode(f: => Unit): Unit = inV1Mode(f)

  override protected def shouldPassTests: Set[String] =
    DeltaV2SourceTableAPISuite.PassingTests

  override protected def shouldFailTests: Set[String] =
    DeltaV2SourceTableAPISuite.NonSourceTests
}

object DeltaV2SourceTableAPISuite {
  val PassingTests: Set[String] = Set(
    "table API",
    "table API with database"
  )

  // These inherited cases test writeStream.toTable. They do not cover the streaming source.
  val NonSourceTests: Set[String] = Set(
    "writeStream.table - create new external table",
    "writeStream.table - create new managed table",
    "writeStream.table - create new managed table with database",
    "writeStream.table - create table from existing output",
    "writeStream.table - fail writing into a view",
    "writeStream.table - fail due to different schema than existing Delta table",
    "writeStream.table - fail due to different partitioning on existing Delta table",
    "writeStream.table - fail writing into an external nonDelta table",
    "writeStream.table - fail writing into an external nonDelta path"
  )
}
