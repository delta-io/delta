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

package org.apache.spark.sql.delta.deletionvectors.mumbling;

import java.util.Random;

/** Data generation for {@link PFOREncoding} tests. */
class PFORRandomData {

  private PFORRandomData() {}

  /** Generates {@code count} values between 0 and {@code maxValue}. */
  static int[] uniform(Random random, int count, int maxValue) {
    int[] values = new int[count];
    for (int i = 0; i < count; i++) {
      values[i] = random.nextInt(maxValue + 1);
    }

    return values;
  }

  /** Generates values in [0, 3] with about {@code excPercent} exceptions in [0, 255]. */
  static int[] exceptions(Random random, int count, float excPercent) {
    int[] values = new int[count];
    for (int i = 0; i < count; i++) {
      values[i] = random.nextFloat() < excPercent ? random.nextInt(256) : random.nextInt(4);
    }

    return values;
  }
}
