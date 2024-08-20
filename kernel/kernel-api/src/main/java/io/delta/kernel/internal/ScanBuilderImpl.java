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

package io.delta.kernel.internal;

import io.delta.kernel.Scan;
import io.delta.kernel.ScanBuilder;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.expressions.Predicate;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.actions.Protocol;
import io.delta.kernel.internal.fs.Path;
import io.delta.kernel.internal.replay.LogReplay;
import io.delta.kernel.types.StructType;
import java.util.Optional;

/** Implementation of {@link ScanBuilder}. */
public class ScanBuilderImpl implements ScanBuilder {

  private final Path dataPath;
  private final Protocol protocol;
  private final Metadata metadata;
  private final StructType snapshotSchema;
  private final LogReplay logReplay;
  private final Engine engine;

  private StructType readSchema;
  private Optional<Predicate> predicate;

  public ScanBuilderImpl(
      Path dataPath,
      Protocol protocol,
      Metadata metadata,
      StructType snapshotSchema,
      LogReplay logReplay,
      Engine engine) {
    this.dataPath = dataPath;
    this.protocol = protocol;
    this.metadata = metadata;
    this.snapshotSchema = snapshotSchema;
    this.logReplay = logReplay;
    this.engine = engine;
    this.readSchema = snapshotSchema;
    this.predicate = Optional.empty();
  }

  @Override
  public ScanBuilder withFilter(Engine engine, Predicate predicate) {
    if (this.predicate.isPresent()) {
      throw new IllegalArgumentException("There already exists a filter in current builder");
    }
    this.predicate = Optional.of(predicate);
    return this;
  }

  @Override
  public ScanBuilder withReadSchema(Engine engine, StructType readSchema) {
    snapshotSchema.checkNestedFields(readSchema);
    this.readSchema = readSchema;
    return this;
  }

  @Override
  public Scan build() {
    return new ScanImpl(
        snapshotSchema, readSchema, protocol, metadata, logReplay, predicate, dataPath);
  }
}
