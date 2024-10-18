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

package io.delta.kernel;

import io.delta.kernel.annotation.Evolving;
import io.delta.kernel.config.ConfigurationProvider;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.exceptions.TableNotFoundException;

/**
 * Builder for creating a {@link Table} instance.
 *
 * <p>Required parameters for building a {@link TableBuilder} instance include:
 *
 * <ul>
 *   <li>{@code Engine}: The {@link Engine} instance required to interact with Delta Kernel.
 *   <li>{@code Path}: The location of the table, resolved by the provided {@code Engine}.
 * </ul>
 *
 * @since 3.3.0
 */
@Evolving
public interface TableBuilder {

  /**
   * Set the {@link ConfigurationProvider} to use for the table.
   *
   * @param configProvider {@link ConfigurationProvider} to use for the table
   * @return this updated {@link TableBuilder} instance
   */
  TableBuilder withConfigurationProvider(ConfigurationProvider configProvider);

  /**
   * Set the {@link TableIdentifier} for the table.
   *
   * @param tableId {@link TableIdentifier} for the table
   * @return this updated {@link TableBuilder} instance
   */
  TableBuilder withTableId(TableIdentifier tableId);

  /**
   * Builds the table object for the Delta Lake table at the given path.
   *
   * <ul>
   *   <li>Behavior when the table location doesn't exist:
   *       <ul>
   *         <li>Reads will fail with a {@link TableNotFoundException}
   *         <li>Writes will create the location
   *       </ul>
   *   <li>Behavior when the table location exists (with contents or not) but is not a Delta table:
   *       <ul>
   *         <li>Reads will fail with a {@link TableNotFoundException}
   *         <li>Writes will create a Delta table at the given location. If there are any existing
   *             files in the location that are not already part of the Delta table, they will
   *             remain excluded from the Delta table.
   *       </ul>
   * </ul>
   *
   * @return the new {@link Table} instance
   */
  Table build();
}
