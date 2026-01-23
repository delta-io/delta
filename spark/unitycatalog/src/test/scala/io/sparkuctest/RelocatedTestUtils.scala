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

package io.sparkuctest

import org.apache.spark.sql.delta.Relocated

/**
 * Test-only helpers for dealing with classes that are relocated across Spark versions.
 *
 * <p>Some streaming relation types are defined as Scala type aliases in [[Relocated]], which
 * makes them difficult to reference from Java tests directly. This utility centralizes the
 * instance checks so Java tests can call into a stable Scala API instead.
 */
object RelocatedTestUtils {
  /** Returns true if the plan is a V1 streaming relation across Spark versions. */
  def isStreamingRelation(plan: Any): Boolean = {
    plan.isInstanceOf[Relocated.StreamingRelation]
  }
}
