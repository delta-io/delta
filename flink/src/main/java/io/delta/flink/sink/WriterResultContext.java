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

package io.delta.flink.sink;

import java.io.Serializable;

public class WriterResultContext implements Serializable {

  private long lowWatermark;

  private long highWatermark;

  public WriterResultContext() {
    this(-1L, Long.MAX_VALUE);
  }

  public WriterResultContext(long hwm, long lwm) {
    this.highWatermark = hwm;
    this.lowWatermark = lwm;
  }

  public long getLowWatermark() {
    return lowWatermark;
  }

  public long getHighWatermark() {
    return highWatermark;
  }

  public WriterResultContext merge(WriterResultContext another) {
    this.lowWatermark = Math.min(lowWatermark, another.lowWatermark);
    this.highWatermark = Math.max(highWatermark, another.highWatermark);
    return this;
  }
}
