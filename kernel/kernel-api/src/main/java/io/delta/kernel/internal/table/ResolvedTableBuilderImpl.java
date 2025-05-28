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

package io.delta.kernel.internal.table;

import static io.delta.kernel.internal.util.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

import io.delta.kernel.ResolvedTableBuilder;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.actions.Protocol;
import io.delta.kernel.internal.files.ParsedLogData;
import io.delta.kernel.internal.files.ParsedLogData.ParsedLogType;
import io.delta.kernel.internal.util.Clock;
import io.delta.kernel.internal.util.Tuple2;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * An implementation of {@link ResolvedTableBuilder}.
 *
 * <p>Note: The primary responsibility of this class is to take input, validate that input, and then
 * pass the input to the {@link ResolvedTableFactory}, which is then responsible for actually
 * creating the {@link ResolvedTableInternal} instance.
 */
public class ResolvedTableBuilderImpl implements ResolvedTableBuilder {

  public static class Context {
    public final String unresolvedPath;
    public Optional<Long> versionOpt;
    public List<ParsedLogData> logDatas;
    public Optional<Tuple2<Protocol, Metadata>> protocolAndMetadataOpt;
    public Clock clock;

    public Context(String unresolvedPath) {
      this.unresolvedPath = requireNonNull(unresolvedPath, "unresolvedPath is null");
      this.versionOpt = Optional.empty();
      this.logDatas = Collections.emptyList();
      this.protocolAndMetadataOpt = Optional.empty();
      this.clock = System::currentTimeMillis;
    }
  }

  private final Context ctx;

  public ResolvedTableBuilderImpl(String unresolvedPath) {
    ctx = new Context(unresolvedPath);
  }

  /////////////////////////////////
  // Additional Internal Methods //
  /////////////////////////////////

  public ResolvedTableBuilderImpl withClock(Clock clock) {
    ctx.clock = requireNonNull(clock, "clock is null");
    return this;
  }

  /////////////////////////////////////////
  // Public ResolvedTableBuilder Methods //
  /////////////////////////////////////////

  @Override
  public ResolvedTableBuilderImpl atVersion(long version) {
    ctx.versionOpt = Optional.of(version);
    return this;
  }

  /** For now, only log datas of type {@link ParsedLogType#RATIFIED_STAGED_COMMIT}s are supported */
  @Override
  public ResolvedTableBuilderImpl withLogData(List<ParsedLogData> logDatas) {
    ctx.logDatas = requireNonNull(logDatas, "logDatas is null");
    return this;
  }

  @Override
  public ResolvedTableBuilder withProtocolAndMetadata(Protocol protocol, Metadata metadata) {
    ctx.protocolAndMetadataOpt = Optional.of(new Tuple2<>(protocol, metadata));
    return this;
  }

  @Override
  public ResolvedTableInternal build(Engine engine) {
    validateInputOnBuild();
    return new ResolvedTableFactory(engine, ctx).create(engine);
  }

  ////////////////////////////
  // Private Helper Methods //
  ////////////////////////////

  private void validateInputOnBuild() {
    checkArgument(ctx.versionOpt.orElse(0L) >= 0, "version must be >= 0");
    // TODO: logData only ratified staged commits
    // TODO: logData sorted and contiguous
  }
}
