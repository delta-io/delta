/*
 * Copyright (2023) The Delta Lake Project Authors.
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
package io.delta.kernel.internal.util;

/** A clock whose time can be manually set and modified. */
public class ManualClock implements Clock {
  private long timeMillis;

  public ManualClock(long timeMillis) {
    this.timeMillis = timeMillis;
  }

  /** @param timeToSet new time (in milliseconds) that the clock should represent */
  public synchronized void setTime(long timeToSet) {
    this.timeMillis = timeToSet;
    this.notifyAll();
  }

  @Override
  public long getTimeMillis() {
    return timeMillis;
  }
}
