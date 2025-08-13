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

package io.delta.unity;

import static io.delta.kernel.internal.util.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

import io.delta.kernel.commit.CommitFailedException;
import io.delta.kernel.commit.CommitMetadata;
import io.delta.kernel.commit.CommitResponse;
import io.delta.kernel.commit.Committer;
import io.delta.kernel.data.Row;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.storage.commit.uccommitcoordinator.UCClient;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An implementation of {@link Committer} that handles commits to Delta tables managed by Unity
 * Catalog. That is, these Delta tables must have the catalogManaged table feature supported.
 */
public class UCCatalogManagedCommitter implements Committer {
  private static final Logger logger = LoggerFactory.getLogger(UCCatalogManagedCommitter.class);

  private final UCClient ucClient;
  private final String ucTableId;
  private final Path tablePath;

  /**
   * Creates a new UCCatalogManagedCommitter for the specified Unity Catalog-managed Delta table.
   *
   * @param ucClient the Unity Catalog client to use for commit operations
   * @param ucTableId the unique Unity Catalog table identifier
   * @param tablePath the path to the Delta table in the underlying storage system
   */
  public UCCatalogManagedCommitter(UCClient ucClient, String ucTableId, String tablePath) {
    this.ucClient = requireNonNull(ucClient, "ucClient is null");
    this.ucTableId = requireNonNull(ucTableId, "ucTableId is null");
    this.tablePath = new Path(requireNonNull(tablePath, "tablePath is null"));
  }

  @Override
  public CommitResponse commit(
      Engine engine, CloseableIterator<Row> finalizedActions, CommitMetadata commitMetadata)
      throws CommitFailedException {
    requireNonNull(engine, "engine is null");
    requireNonNull(finalizedActions, "finalizedActions is null");
    requireNonNull(commitMetadata, "commitMetadata is null");
    validateLogPathBelongsToThisUcTable(commitMetadata);

    final CommitMetadata.CommitType commitType = commitMetadata.getCommitType();

    if (commitType != CommitMetadata.CommitType.CATALOG_WRITE) {
      // TODO: Support CATALOG_CREATE in the future, too
      throw new UnsupportedOperationException("Unsupported commit type: " + commitType);
    }

    // TODO: lastKnownBackfilledVersion? Take that in as a hint.

    if (commitMetadata.getNewProtocolOpt().isPresent()) {
      // TODO: support this
      throw new UnsupportedOperationException("Protocol change is not yet implemented");
    }
    if (commitMetadata.getNewMetadataOpt().isPresent()) {
      // TODO: support this
      throw new UnsupportedOperationException("Metadata change is not yet implemented");
    }

    // TODO: support this
    throw new UnsupportedOperationException("Commit logic is not yet implemented");
  }

  ////////////////////
  // Helper methods //
  ////////////////////

  private String normalize(Path path) {
    return path.toUri().normalize().toString();
  }

  private void validateLogPathBelongsToThisUcTable(CommitMetadata cm) {
    final String expectedDeltaLogPathNormalized = normalize(new Path(tablePath, "_delta_log"));
    final String providedDeltaLogPathNormalized = normalize(new Path(cm.getDeltaLogDirPath()));
    checkArgument(
        expectedDeltaLogPathNormalized.equals(providedDeltaLogPathNormalized),
        "Delta log path '%s' does not match expected '%s'",
        expectedDeltaLogPathNormalized,
        providedDeltaLogPathNormalized);
  }
}
