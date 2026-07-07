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

import io.delta.kernel.internal.actions.DomainMetadata;
import io.delta.storage.commit.actions.AbstractDomainMetadata;
import java.util.Objects;

/**
 * Adapter from {@link io.delta.kernel.internal.actions.DomainMetadata} to {@link
 * io.delta.storage.commit.actions.AbstractDomainMetadata}.
 */
public class DomainMetadataAdapter implements AbstractDomainMetadata {

  private final DomainMetadata kernelDomainMetadata;

  public DomainMetadataAdapter(DomainMetadata kernelDomainMetadata) {
    this.kernelDomainMetadata =
        Objects.requireNonNull(kernelDomainMetadata, "kernelDomainMetadata is null");
  }

  @Override
  public String getDomain() {
    return kernelDomainMetadata.getDomain();
  }

  @Override
  public String getConfiguration() {
    return kernelDomainMetadata.getConfiguration();
  }

  @Override
  public boolean isRemoved() {
    return kernelDomainMetadata.isRemoved();
  }
}
