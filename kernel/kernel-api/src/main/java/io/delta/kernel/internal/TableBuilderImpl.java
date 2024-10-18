/*
 * Copyright (2024) The Delta Lake Project Authors.
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

import static java.util.Objects.requireNonNull;

import io.delta.kernel.Table;
import io.delta.kernel.TableBuilder;
import io.delta.kernel.TableIdentifier;
import io.delta.kernel.config.ConfigurationProvider;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.util.Clock;
import java.util.Optional;

public class TableBuilderImpl implements TableBuilder {
  private final Engine engine;
  private final String path;

  private Optional<ConfigurationProvider> configProviderOpt = Optional.empty();
  private Optional<TableIdentifier> tableIdOpt = Optional.empty();

  /** Note: This is not for a public API. It's for internal testing use only. */
  private Optional<Clock> clockOpt = Optional.empty();

  public TableBuilderImpl(Engine engine, String path) {
    requireNonNull(engine, "Expected non-null value for 'engine'");
    requireNonNull(path, "Expected non-null value for 'path'");

    this.engine = engine;
    this.path = path;
  }

  @Override
  public TableBuilder withConfigurationProvider(ConfigurationProvider configProvider) {
    requireNonNull(configProvider, "Expected non-null value for 'configProvider'");
    this.configProviderOpt = Optional.of(configProvider);
    return this;
  }

  @Override
  public TableBuilder withTableId(TableIdentifier tableId) {
    requireNonNull(tableId, "Expected non-null value for 'tableId'");
    this.tableIdOpt = Optional.of(tableId);
    return null;
  }

  /** Note: This is not a public API. It's for internal testing use only. */
  public TableBuilder withClock(Clock clock) {
    requireNonNull(clock, "Expected non-null value for 'clock'");
    this.clockOpt = Optional.of(clock);
    return this;
  }

  @Override
  public Table build() {
    return TableImpl.create(
        engine,
        path,
        clockOpt.orElse(System::currentTimeMillis),
        configProviderOpt.orElse(new EmptyConfigurationProvider()),
        tableIdOpt);
  }
}
