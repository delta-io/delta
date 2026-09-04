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

package org.apache.spark.sql.delta.coordinatedcommits

import org.apache.spark.sql.delta.test.V2ForceTest

/** Runs the catalog-managed streaming tests through the strict Delta V2 connector. */
class CatalogManagedStreamingV2Suite
  extends CatalogManagedStreamingSuiteBase
  with V2ForceTest {

  override protected def assertNoV1Fallback: Boolean = true

  override protected def withV1Mode(f: => Unit): Unit = inV1Mode(f)

  override protected def shouldPassTests: Set[String] =
    CatalogManagedStreamingV2Suite.PassingTests

  override protected def shouldFailTests: Set[String] =
    CatalogManagedStreamingV2Suite.FailingTests
}

object CatalogManagedStreamingV2Suite {
  val PassingTests: Set[String] = Set(
    "stream from delta source"
  )

  val FailingTests: Set[String] = Set(
    // Delta V2 supports catalog-managed reads but not catalog-managed streaming commits.
    "stream to delta sink",
    "stream from delta source to delta sink with shared commit coordinator"
  )
}
