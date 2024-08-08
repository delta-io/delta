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
package io.delta.kernel.defaults.internal.coordinatedcommits;

import static io.delta.kernel.defaults.internal.DefaultEngineErrors.canNotInstantiateCommitCoordinatorBuilder;

import io.delta.kernel.defaults.engine.DefaultCommitCoordinatorClientHandler;
import io.delta.storage.commit.CommitCoordinatorClient;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Factory to get the correct {@link CommitCoordinatorClient} for a table which is used by {@link
 * DefaultCommitCoordinatorClientHandler} to get the commit coordinator client using the {@code
 * getCommitCoordinatorClient} method.
 */
public class CommitCoordinatorProvider {
  private static final Logger logger = LoggerFactory.getLogger(CommitCoordinatorProvider.class);

  /**
   * Returns a {@link CommitCoordinatorClient} for the given `hadoopConf`, `name` and
   * `commitCoordinatorConf`. Caller can set `io.delta.kernel.commitCoordinatorBuilder.{@code
   * name}.impl` to specify the {@link CommitCoordinatorBuilder} implementation to use for {@code
   * name}.
   *
   * @param hadoopConf {@link Configuration} to use for creating the {@link
   *     CommitCoordinatorBuilder}.
   * @param name Name of the commit-coordinator.
   * @param commitCoordinatorConf Configuration for building the commit coordinator used by the
   *     {@link CommitCoordinatorBuilder}.
   * @return {@link CommitCoordinatorClient} instance.
   */
  public static CommitCoordinatorClient getCommitCoordinatorClient(
      Configuration hadoopConf, String name, Map<String, String> commitCoordinatorConf) {
    // Check if the CommitCoordinatorBuilder implementation is set in the configuration.
    String classNameFromConfig = hadoopConf.get(getCommitCoordinatorNameConfKey(name));
    if (classNameFromConfig != null) {
      CommitCoordinatorBuilder builder =
          createCommitCoordinatorBuilder(classNameFromConfig, hadoopConf, "from config");
      return builder.build(commitCoordinatorConf);
    } else {
      throw new IllegalArgumentException("Unknown commit-coordinator: " + name);
    }
  }

  /**
   * Configuration key for setting the CommitCoordinatorBuilder implementation for a name. ex:
   * `io.delta.kernel.commitCoordinatorBuilder.in-memory.impl` ->
   * `io.delta.storage.InMemoryCommitCoordinatorBuilder`
   */
  static String getCommitCoordinatorNameConfKey(String name) {
    return "io.delta.kernel.commitCoordinatorBuilder." + name + ".impl";
  }

  /** Utility method to get the CommitCoordinatorBuilder class from the class name. */
  private static Class<? extends CommitCoordinatorBuilder> getCommitCoordinatorBuilderClass(
      String commitCoordinatorBuilderClassName) throws ClassNotFoundException {
    return Class.forName(commitCoordinatorBuilderClassName)
        .asSubclass(CommitCoordinatorBuilder.class);
  }

  private static CommitCoordinatorBuilder createCommitCoordinatorBuilder(
      String className, Configuration hadoopConf, String context) {
    try {
      return getCommitCoordinatorBuilderClass(className)
          .getConstructor(Configuration.class)
          .newInstance(hadoopConf);
    } catch (Exception e) {
      String msgTemplate = "Failed to instantiate CommitCoordinatorBuilder class ({}): {}";
      logger.error(msgTemplate, context, className, e);
      throw canNotInstantiateCommitCoordinatorBuilder(className, context, e);
    }
  }
}
