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

package io.delta.flink.kernel;

import io.delta.kernel.Snapshot;
import io.delta.kernel.engine.Engine;
import java.io.IOException;

/** This is a placeholder class. It calls Snapshot.writeCheckpoint */
public class CheckpointWriter {

  private final Engine engine;
  private final Snapshot snapshot;

  public CheckpointWriter(Engine engine, Snapshot snapshot) {
    this.engine = engine;
    this.snapshot = snapshot;
  }

  public void write() throws IOException {
    snapshot.writeCheckpoint(engine);
  }
}
