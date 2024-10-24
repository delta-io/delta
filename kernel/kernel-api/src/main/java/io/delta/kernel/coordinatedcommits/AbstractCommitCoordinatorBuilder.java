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

package io.delta.kernel.coordinatedcommits;

import io.delta.kernel.annotation.Evolving;
import io.delta.kernel.config.ConfigurationProvider;
import java.util.Map;

/**
 * A builder class to create a {@link CommitCoordinatorClient} (CCC).
 *
 * <p>Subclasses of this class must provide a no-argument constructor.
 *
 * <p>Note: These builder classed are optional. The {@link
 * io.delta.kernel.engine.Engine#getCommitCoordinatorClient} API does not prescribe how to create
 * the underlying CCC and does not require a builder.
 *
 * <p>The benefit of implementing a builder for your CCC is that your Engine may then invoke {@link
 * CommitCoordinatorUtils#buildCommitCoordinatorClient} to (1) instantiate your builder, and then
 * (2) build a new CCC via reflection.
 *
 * <p>In short, this builder provides the ability for users to specify at runtime which builder
 * implementation (e.g. x.y.z.FooCCBuilder) to use for a given commit coordinator name (e.g. "foo"),
 * without imposing any restrictions on how to construct CCCs (i.e. these builders are optional).
 */
@Evolving
public abstract class AbstractCommitCoordinatorBuilder {
  /** Subclasses of this class must provide a no-argument constructor. */
  public AbstractCommitCoordinatorBuilder() {}

  /** @return the commit coordinator name */
  public abstract String getName();

  /**
   * Build the {@link CommitCoordinatorClient}. This may be a new instance or a cached one.
   *
   * @param sessionConfig The session-level configuration that may represent environment-specific
   *     configurations such as {@code HadoopConf} or {@code SparkConf}. This sessionConfig can be
   *     used by builders to lookup per-session configuration values unique to that builder.
   * @param commitCoordinatorConf the parsed value of the Delta table property {@link
   *     io.delta.kernel.internal.TableConfig#COORDINATED_COMMITS_TABLE_CONF} and represents the
   *     configuration properties for describing the Delta table to the commit-coordinator.
   * @return the {@link CommitCoordinatorClient}
   */
  public abstract CommitCoordinatorClient build(
      ConfigurationProvider sessionConfig, Map<String, String> commitCoordinatorConf);
}
