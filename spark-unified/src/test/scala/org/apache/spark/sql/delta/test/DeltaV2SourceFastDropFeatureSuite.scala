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

import org.apache.spark.sql.delta.DeltaSourceFastDropFeatureSuite

/**
 * Runs DeltaSourceFastDropFeatureSuite using the V2 connector (V2_ENABLE_MODE=STRICT).
 */
class DeltaV2SourceFastDropFeatureSuite
  extends DeltaSourceFastDropFeatureSuite with V2ForceTest {

  override protected def useDsv2: Boolean = true

  override protected def executeDml(sqlText: String): Unit = executeInV1Mode(sqlText)

  override protected def shouldPassTests: Set[String] = Set(
    "Latest protocol is checked for unsupported features",
    "Protocol is checked when using startingVersion - useStartingTS: false.",
    "Protocol is checked when using startingVersion - useStartingTS: true.",
    "Protocol check at startingVersion is skipped when config is disabled",
    "Protocol is checked when coming across an action with a protocol upgrade",
    "Protocol validations supress errors when snapshot cannot be reconstructed",
    "Restart from checkpoint reads forward into an unsupported feature commit"
  )

  override protected def shouldFailTests: Set[String] = Set(
    // Builds its first stream while testUnsupportedReaderWriter is still at the latest version.
    // DSv1 gates that test-only feature on UNSUPPORTED_TESTING_FEATURES_ENABLED (off here) and
    // accepts it, but Kernel-backed V2 doesn't know the feature and rejects unconditionally at
    // stream construction -- before the drop and restart this test means to exercise.
    "Protocol validations after restarting from a checkpoint"
  )
}
