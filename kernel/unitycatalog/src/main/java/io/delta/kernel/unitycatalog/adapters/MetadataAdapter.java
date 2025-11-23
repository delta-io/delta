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

import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.util.VectorUtils;
import io.delta.storage.commit.actions.AbstractMetadata;
import java.util.*;

/**
 * Adapter from {@link io.delta.kernel.internal.actions.Metadata} to {@link
 * io.delta.storage.commit.actions.AbstractMetadata}.
 */
public class MetadataAdapter implements AbstractMetadata {

  private final Metadata kernelMetadata;

  public MetadataAdapter(Metadata kernelMetadata) {
    this.kernelMetadata = Objects.requireNonNull(kernelMetadata, "kernelMetadata is null");
  }

  @Override
  public String getId() {
    return kernelMetadata.getId();
  }

  @Override
  public String getName() {
    return kernelMetadata.getName().orElse(null);
  }

  @Override
  public String getDescription() {
    return kernelMetadata.getDescription().orElse(null);
  }

  @Override
  public String getProvider() {
    return kernelMetadata.getFormat().getProvider();
  }

  @Override
  public Map<String, String> getFormatOptions() {
    return Collections.unmodifiableMap(kernelMetadata.getFormat().getOptions());
  }

  @Override
  public String getSchemaString() {
    return kernelMetadata.getSchemaString();
  }

  @Override
  public List<String> getPartitionColumns() {
    return Collections.unmodifiableList(
        VectorUtils.toJavaList(kernelMetadata.getPartitionColumns()));
  }

  @Override
  public Map<String, String> getConfiguration() {
    return Collections.unmodifiableMap(kernelMetadata.getConfiguration());
  }

  @Override
  public Long getCreatedTime() {
    return kernelMetadata.getCreatedTime().orElse(null);
  }
}
