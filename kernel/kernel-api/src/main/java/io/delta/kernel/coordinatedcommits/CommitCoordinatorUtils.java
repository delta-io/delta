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

import static io.delta.kernel.internal.TableConfig.COORDINATED_COMMITS_COORDINATOR_NAME;

import io.delta.kernel.config.ConfigurationProvider;
import io.delta.kernel.data.Row;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.engine.coordinatedcommits.UpdatedActions;
import io.delta.kernel.internal.DeltaErrors;
import io.delta.kernel.internal.TableConfig;
import io.delta.kernel.internal.fs.Path;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.FileStatus;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.UUID;

/** Various public utility methods related to Coordinated Commits. */
public class CommitCoordinatorUtils {
  /** The subdirectory in which to store the unbackfilled commit files. */
  private static final String COMMIT_SUBDIR = "_commits";

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

  /** Returns true if the commit is a coordinated commits to filesystem conversion. */
  public static boolean isCoordinatedCommitsToFSConversion(
      Long commitVersion, UpdatedActions updatedActions) {
    final boolean oldMetadataHasCoordinatedCommits =
        updatedActions
            .getOldMetadata()
            .getConfiguration()
            .containsKey(COORDINATED_COMMITS_COORDINATOR_NAME);

    final boolean newMetadataHasCoordinatedCommits =
        updatedActions
            .getNewMetadata()
            .getConfiguration()
            .containsKey(COORDINATED_COMMITS_COORDINATOR_NAME);

    return oldMetadataHasCoordinatedCommits
        && !newMetadataHasCoordinatedCommits
        && commitVersion > 0;
  }

  /** Returns path to the directory which holds the unbackfilled commits */
  public static String getCommitDirPath(String logPath) {
    return new Path(logPath, COMMIT_SUBDIR).toString();
  }

  /**
   * Returns the un-backfilled uuid formatted delta (json format) path for a given version.
   *
   * @param logPath The root path of the delta log.
   * @param version The version of the delta file.
   * @return The path to the un-backfilled delta file: logPath/_commits/version.uuid.json
   */
  public static String getUnbackfilledDeltaFilePath(String logPath, long version) {
    final String basePath = getCommitDirPath(logPath);
    final String uuid = UUID.randomUUID().toString();
    return new Path(basePath, String.format("%020d.%s.json", version, uuid)).toString();
  }

  /**
   * Returns the path to the backfilled delta file for the given commit version. The path is of the
   * form `tablePath/_delta_log/00000000000000000001.json`.
   */
  public static String getBackfilledDeltaFilePath(String logPath, long version) {
    return new Path(logPath, String.format("%020d.json", version)).toString();
  }

  /** Write a UUID-based commit file for the specified version to the table at logPath. */
  public static FileStatus writeUnbackfilledCommitFile(
      Engine engine, String logPath, long commitVersion, CloseableIterator<Row> actions)
      throws IOException {
    final String unbackfilledDeltaFilePath = getUnbackfilledDeltaFilePath(logPath, commitVersion);

    // Do not use Put-If-Absent for unbackfilled commit files since we assume that UUID-based
    // commit files are globally unique, and so we will never have concurrent writers attempting
    // to write the same commit file.

    engine
        .getJsonHandler()
        .writeJsonFileAtomically(unbackfilledDeltaFilePath, actions, true /* overwrite */);

    return engine.getFileSystemClient().getFileStatus(unbackfilledDeltaFilePath);
  }
}
