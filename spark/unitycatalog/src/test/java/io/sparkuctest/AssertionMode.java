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

package io.sparkuctest;

/**
 * Verification modes for DSv2 streaming tests.
 *
 * <p>Controls how streaming results are checked against batch reads.
 */
public enum AssertionMode {
  /**
   * Single {@code Trigger.AvailableNow()} streaming read. Compares the final accumulated streaming
   * output against a batch {@code SELECT *} from the same table.
   */
  SNAPSHOT,

  /**
   * Continuous streaming query with {@code processAllAvailable()} between {@link
   * TableSetup#addIncrementalData} calls. Verifies at each step that accumulated streaming output
   * matches the current batch {@code SELECT *}. Only runs when {@link
   * TableSetup#incrementalRounds()} > 0.
   */
  INCREMENTAL
}
