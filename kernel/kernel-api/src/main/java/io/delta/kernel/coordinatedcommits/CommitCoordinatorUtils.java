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

import io.delta.kernel.config.ConfigurationProvider;
import io.delta.kernel.internal.DeltaErrors;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;
import java.util.NoSuchElementException;

/** Various public utility methods related to Coordinated Commits. */
public class CommitCoordinatorUtils {
  private CommitCoordinatorUtils() {}

  /**
   * Builds the underlying {@link CommitCoordinatorClient} associated with the given commit
   * coordinator name.
   *
   * <ul>
   *   <li>Determines the specific builder configuration lookup key.
   *   <li>Grabs the corresponding builder className for that key from the provided sessionConfig.
   *   <li>Instantiates a new instance of that {@link AbstractCommitCoordinatorBuilder}.
   *   <li>Invokes the builder's build method, passing along the sessionConfig and commit
   *       coordinator config map.
   * </ul>
   *
   * @param commitCoordinatorName the commit coordinator name
   * @param sessionConfig The session-level configuration that may represent environment-specific
   *     configurations such as {@code HadoopConf} or {@code SparkConf}. This sessionConfig is used
   *     to look up the right builder className to instantiate for the given commit coordinator
   *     name. It can also be used by builders to lookup per-session configuration values unique to
   *     that builder.
   * @param commitCoordinatorConf the parsed value of the Delta table property {@link
   *     io.delta.kernel.internal.TableConfig#COORDINATED_COMMITS_TABLE_CONF} and represents the
   *     configuration properties for describing the Delta table to the commit-coordinator.
   * @return the {@link CommitCoordinatorClient} corresponding to the given commit coordinator name
   */
  public static CommitCoordinatorClient buildCommitCoordinatorClient(
      String commitCoordinatorName,
      ConfigurationProvider sessionConfig,
      Map<String, String> commitCoordinatorConf) {
    final String builderConfKey = getCommitCoordinatorBuilderConfKey(commitCoordinatorName);

    final String builderClassName;
    try {
      builderClassName = sessionConfig.getNonNull(builderConfKey);
    } catch (NoSuchElementException | IllegalStateException ex) {
      throw DeltaErrors.unknownCommitCoordinator(commitCoordinatorName, builderConfKey);
    }

    try {
      return Class.forName(builderClassName)
          .asSubclass(AbstractCommitCoordinatorBuilder.class)
          .getConstructor()
          .newInstance()
          .build(sessionConfig, commitCoordinatorConf);
    } catch (ClassNotFoundException
        | NoSuchMethodException
        | InstantiationException
        | IllegalAccessException
        | InvocationTargetException e) {
      throw DeltaErrors.couldNotInstantiateCommitCoordinatorClient(
          commitCoordinatorName, builderClassName, e);
    }
  }

  /** Returns the builder configuration key for the given commit coordinator name */
  public static String getCommitCoordinatorBuilderConfKey(String ccName) {
    return String.format("io.delta.kernel.commitCoordinatorBuilder.%s.impl", ccName);
  }
}
