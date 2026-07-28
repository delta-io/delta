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

package io.delta.kernel.commit;

import io.delta.kernel.annotation.Experimental;
import io.delta.kernel.engine.Engine;
import java.util.Collections;
import java.util.Map;

/**
 * {@link Committer} sub-interface for catalog-managed tables. Provides catalog-specific operations
 * not applicable to filesystem-managed tables.
 */
@Experimental
public interface CatalogCommitter extends Committer {

  /**
   * Returns required catalog table properties that must be set in the Delta metadata.
   *
   * <p>These properties are automatically injected during CREATE and REPLACE operations and cannot
   * be changed or removed by users. Any attempt to set these properties to different values or
   * remove them will result in a validation error.
   *
   * @return a map of required catalog properties
   */
  default Map<String, String> getRequiredTableProperties() {
    return Collections.emptyMap();
  }

  /**
   * Publishes catalog commits to the Delta log. Applicable only to catalog-managed tables.
   *
   * <p>Publishing is the act of copying ratified catalog commits to the Delta log as published
   * Delta files (e.g., {@code _delta_log/00000000000000000001.json}).
   *
   * <p>The benefits of publishing include:
   *
   * <ul>
   *   <li>Reduces the number of commits the catalog needs to store internally and serve to readers
   *   <li>Enables table maintenance operations that must operate on published versions only, such
   *       as checkpointing and log compaction
   * </ul>
   *
   * <p>Requirements:
   *
   * <ul>
   *   <li>This method must ensure that all catalog commits are published to the Delta log up to and
   *       including the snapshot version specified in {@code publishMetadata}
   *   <li>Commits must be published in order: version V-1 must be published before version V
   * </ul>
   *
   * <p>Catalog-specific semantics: Each catalog implementation may specify its own rules and
   * semantics for publishing, including whether it expects to be notified immediately upon
   * publishing success, whether published deltas must appear with PUT-if-absent semantics in the
   * Delta log, and whether publishing happens in the client-side or server-side catalog-component.
   *
   * @param engine the {@link Engine} instance used for publishing commits
   * @param publishMetadata the {@link PublishMetadata} containing the snapshot version up to which
   *     all catalog commits must be published, the log path, and list of catalog commits
   * @throws PublishFailedException if the publish operation fails
   */
  void publish(Engine engine, PublishMetadata publishMetadata) throws PublishFailedException;
}
