/*
 * Copyright (2021) The Delta Lake Project Authors.
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

/** Shared constants and helpers for the Mumbling v1 wire format. */
final class MumblingFormat {
  static final int VERSION = 1;
  static final int HEADER_SIZE = 6;
  static final int DENSE_CONTAINER_BIT = 0b0010_0000;
  static final int DENSE_CONTAINER_SIZE = 32;
  static final int MAX_SPARSE_COUNT = DENSE_CONTAINER_SIZE - 1;
  static final int CONTAINER_SIZE = 256;
  static final int MAX_CONTAINERS = 8192;
  static final int MAX_POSITION_EXCLUSIVE = MAX_CONTAINERS * CONTAINER_SIZE;

  private MumblingFormat() {}

  static boolean isDense(int descriptor) {
    return (descriptor & DENSE_CONTAINER_BIT) != 0;
  }

  static boolean isSparse(int descriptor) {
    return descriptor < DENSE_CONTAINER_SIZE;
  }
}
