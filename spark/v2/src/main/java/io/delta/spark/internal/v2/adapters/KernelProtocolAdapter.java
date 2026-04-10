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
package io.delta.spark.internal.v2.adapters;

import io.delta.kernel.internal.actions.Protocol;
import java.util.Objects;
import org.apache.spark.sql.delta.v2.interop.AbstractProtocol;
import scala.Option;
import scala.collection.immutable.Set;
import scala.jdk.javaapi.CollectionConverters;

/**
 * Adapter from {@link io.delta.kernel.internal.actions.Protocol} to {@link
 * org.apache.spark.sql.delta.v2.interop.AbstractProtocol}.
 */
public class KernelProtocolAdapter implements AbstractProtocol {

  private final Protocol kernelProtocol;
  private volatile Option<Set<String>> cachedReaderFeatures;
  private volatile Option<Set<String>> cachedWriterFeatures;

  public KernelProtocolAdapter(Protocol kernelProtocol) {
    this.kernelProtocol = Objects.requireNonNull(kernelProtocol, "kernelProtocol is null");
  }

  @Override
  public int minReaderVersion() {
    return kernelProtocol.getMinReaderVersion();
  }

  @Override
  public int minWriterVersion() {
    return kernelProtocol.getMinWriterVersion();
  }

  @Override
  public Option<Set<String>> readerFeatures() {
    if (cachedReaderFeatures == null) {
      cachedReaderFeatures =
          kernelProtocol.supportsReaderFeatures()
              ? Option.apply(
                  CollectionConverters.asScala(kernelProtocol.getReaderFeatures()).toSet())
              : Option.empty();
    }
    return cachedReaderFeatures;
  }

  @Override
  public Option<Set<String>> writerFeatures() {
    if (cachedWriterFeatures == null) {
      cachedWriterFeatures =
          kernelProtocol.supportsWriterFeatures()
              ? Option.apply(
                  CollectionConverters.asScala(kernelProtocol.getWriterFeatures()).toSet())
              : Option.empty();
    }
    return cachedWriterFeatures;
  }
}
