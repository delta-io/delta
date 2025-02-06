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

import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.actions.Protocol;
import io.delta.kernel.internal.fs.Path;
import io.delta.kernel.internal.snapshot.LogSegment;

public class ExistingPAndMReplay extends LogReplay {

  private final Protocol protocol;
  private final Metadata metadata;

  public ExistingPAndMReplay(
      Engine engine, Path dataPath, LogSegment logSegment, Protocol protocol, Metadata metadata) {
    super(engine, dataPath, logSegment);
    this.protocol = protocol;
    this.metadata = metadata;
  }

  @Override
  public Protocol getProtocol() {
    return protocol;
  }

  @Override
  public Metadata getMetadata() {
    return metadata;
  }

  @Override
  public Optional<CRCInfo> getCurrentCrcInfo() {
    return Optional.empty();
  }
}
