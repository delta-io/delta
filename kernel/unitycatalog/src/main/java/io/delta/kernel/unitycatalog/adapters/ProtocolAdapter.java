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

package io.delta.kernel.unitycatalog.adapters;

import io.delta.kernel.internal.actions.Protocol;
import io.delta.storage.commit.actions.AbstractProtocol;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;

/**
 * Adapter from {@link io.delta.kernel.internal.actions.Protocol} to {@link
 * io.delta.storage.commit.actions.AbstractProtocol}.
 */
public class ProtocolAdapter implements AbstractProtocol {

  private final Protocol kernelProtocol;

  public ProtocolAdapter(Protocol kernelProtocol) {
    this.kernelProtocol = Objects.requireNonNull(kernelProtocol, "kernelProtocol is null");
  }

  @Override
  public int getMinReaderVersion() {
    return kernelProtocol.getMinReaderVersion();
  }

  @Override
  public int getMinWriterVersion() {
    return kernelProtocol.getMinWriterVersion();
  }

  @Override
  public Set<String> getReaderFeatures() {
    return Collections.unmodifiableSet(kernelProtocol.getReaderFeatures());
  }

  @Override
  public Set<String> getWriterFeatures() {
    return Collections.unmodifiableSet(kernelProtocol.getWriterFeatures());
  }
}
