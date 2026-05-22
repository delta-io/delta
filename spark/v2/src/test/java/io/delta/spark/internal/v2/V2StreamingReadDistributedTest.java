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

package io.delta.spark.internal.v2;

import org.junit.jupiter.api.*;

/**
 * Runs all tests from {@link V2StreamingReadTest} with the distributed initial snapshot path
 * enabled ({@code spark.databricks.delta.streaming.distributedInitialSnapshot=true}).
 *
 * <p>Every test that exercises the initial snapshot code path (i.e., reads a table from the
 * beginning without {@code startingVersion}) is automatically re-validated under the distributed
 * variant. Tests that skip the initial snapshot (e.g., those using {@code startingVersion}) still
 * run but the conf has no effect since that code path is not triggered.
 */
public class V2StreamingReadDistributedTest extends V2StreamingReadTest {

  private static final String DISTRIBUTED_INITIAL_SNAPSHOT_KEY =
      "spark.databricks.delta.streaming.distributedInitialSnapshot";

  @BeforeEach
  void enableDistributedInitialSnapshot() {
    spark.conf().set(DISTRIBUTED_INITIAL_SNAPSHOT_KEY, "true");
  }

  @AfterEach
  void resetDistributedInitialSnapshot() {
    spark.conf().unset(DISTRIBUTED_INITIAL_SNAPSHOT_KEY);
  }
}
