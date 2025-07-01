/*
 * Copyright (2025) The Delta Lake Project Authors.
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
package io.delta.kernel.internal.replay;

/** {@code PaginationContext} carries pagination-related information. */
public class PaginationContext {
  public final String startingLogFileName;
  public final long rowIdx;
  public final long sidecarIdx;
  public final long pageSize;

  // TODO: add cached hashsets related info
  public PaginationContext(
      String startingLogFileName, long rowIdx, long sidecarIdx, long pageSize) {
    this.startingLogFileName = startingLogFileName;
    this.rowIdx = rowIdx;
    this.sidecarIdx = sidecarIdx;
    this.pageSize = pageSize;
  }
}
