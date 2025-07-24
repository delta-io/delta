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

package io.delta.kernel.internal;

import io.delta.kernel.internal.replay.LogReplayUtils;
import java.util.Set;

// TODO: this class must be serializable, or must be able to transfer into sth serializable (like
// page token)
public class Tombstones {
  public Set<LogReplayUtils.UniqueFileActionTuple> removeFileSet;
  public Set<LogReplayUtils.UniqueFileActionTuple> alreadyReturnedSet;

  public Tombstones(
      Set<LogReplayUtils.UniqueFileActionTuple> removeFileSet,
      Set<LogReplayUtils.UniqueFileActionTuple> alreadyReturnedSet) {
    this.alreadyReturnedSet = alreadyReturnedSet;
    this.removeFileSet = removeFileSet;
  }
}
