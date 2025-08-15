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

package io.delta.kernel.internal.util;

import static io.delta.kernel.internal.tablefeatures.TableFeatures.CHECKPOINT_V2_RW_FEATURE;

import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.actions.Protocol;
import io.delta.kernel.internal.fs.Path;
import io.delta.kernel.internal.tablefeatures.TableFeatures;
import java.io.IOException;
import java.util.Optional;

/** Utility class for creating Delta directories based on commit version and protocol features. */
public class DirectoryCreationUtils {
  private DirectoryCreationUtils() {}

  /** Creates all required Delta directories based on commit version and protocol features. */
  public static void createAllDeltaDirectoriesAsNeeded(
      Engine engine,
      Path logPath,
      long commitAsVersion,
      Optional<Protocol> readProtocol,
      Protocol writeProtocol)
      throws IOException {
    createDeltaLogDirectoryIfNeeded(engine, logPath, commitAsVersion);
    createStagedCommitDirectoryIfNeeded(engine, logPath, readProtocol, writeProtocol);
    createSidecarDirectoryIfNeeded(engine, logPath, readProtocol, writeProtocol);
  }

  /** Creates delta log directory (_delta_log) if this is the initial commit (version 0). */
  private static void createDeltaLogDirectoryIfNeeded(
      Engine engine, Path logPath, long commitAsVersion) throws IOException {
    if (commitAsVersion == 0) {
      createDirectoryOrThrow(engine, logPath.toString());
    }
  }

  /**
   * Creates staged commit directory (_delta_log/_staged_commits) when enabling catalog managed
   * feature.
   */
  private static void createStagedCommitDirectoryIfNeeded(
      Engine engine, Path logPath, Optional<Protocol> readProtocol, Protocol writeProtocol)
      throws IOException {
    final boolean readVersionSupportsCatalogManaged =
        readProtocol.map(TableFeatures::isCatalogManagedSupported).orElse(false);
    final boolean writeVersionSupportsCatalogManaged =
        TableFeatures.isCatalogManagedSupported(writeProtocol);

    if (!readVersionSupportsCatalogManaged && writeVersionSupportsCatalogManaged) {
      createDirectoryOrThrow(engine, FileNames.stagedCommitDirectory(logPath));
    }
  }

  /** Creates sidecar directory (_delta_log/_sidecar) when enabling v2 checkpoint feature. */
  private static void createSidecarDirectoryIfNeeded(
      Engine engine, Path logPath, Optional<Protocol> readProtocol, Protocol writeProtocol)
      throws IOException {
    final boolean readVersionSupportsV2Checkpoints =
        readProtocol.map(p -> p.supportsFeature(CHECKPOINT_V2_RW_FEATURE)).orElse(false);
    final boolean writeVersionSupportsV2Checkpoints =
        writeProtocol.supportsFeature(CHECKPOINT_V2_RW_FEATURE);

    if (!readVersionSupportsV2Checkpoints && writeVersionSupportsV2Checkpoints) {
      createDirectoryOrThrow(engine, FileNames.sidecarDirectory(logPath));
    }
  }

  /** Creates directory using engine filesystem client, throws on failure. */
  private static void createDirectoryOrThrow(Engine engine, String directoryPath)
      throws IOException {
    try {
      if (!engine.getFileSystemClient().mkdirs(directoryPath)) {
        throw new RuntimeException("Failed to create directory: " + directoryPath);
      }
    } catch (Exception e) {
      throw new IOException("Creating directories for path " + directoryPath, e);
    }
  }
}
