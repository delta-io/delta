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
    // Artifact of how the two connectors decide a reader feature is unsupported, not a
    // restart/streaming defect and not a translation gap. Both connectors build and protocol-check
    // the latest snapshot when the stream is defined; they only differ in what counts as
    // unsupported. testUnsupportedReaderWriter is a test-only feature: the V1 path registers it as
    // supported and only treats it as unsupported when UNSUPPORTED_TESTING_FEATURES_ENABLED is on,
    // whereas the Kernel-backed V2 path does not know the feature at all and never reads that
    // config, so it rejects unconditionally. This test builds its first stream while the feature is
    // still at the latest version and that config is off, so DSv1 accepts it (feature "supported")
    // but V2 throws immediately at stream construction, before the drop and restart the test is
    // trying to exercise. The restart path itself is fine on V2 -- verified separately that when
    // the latest is clean and the unsupported feature sits in history after the checkpoint, both
    // connectors resume from the checkpoint and fail identically on read-forward with
    // DELTA_UNSUPPORTED_FEATURES_FOR_READ.
    "Protocol validations after restarting from a checkpoint"
  )
}
