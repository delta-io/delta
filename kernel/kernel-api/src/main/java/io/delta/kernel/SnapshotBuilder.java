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

package io.delta.kernel;

import io.delta.kernel.annotation.Experimental;
import io.delta.kernel.commit.Committer;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.actions.Protocol;
import io.delta.kernel.internal.files.ParsedLogData;
import io.delta.kernel.internal.files.ParsedLogData.ParsedLogType;
import java.util.List;

/**
 * Builder for constructing a {@link Snapshot} instance.
 *
 * <p>This builder allows table managers (filesystems, catalogs) to provide any information they may
 * know about a Delta table and get back a {@link Snapshot}. When {@link #build(Engine)} is invoked,
 * Kernel will automatically fill any missing information needed to construct the {@link Snapshot}
 * by reading from the filesystem as needed.
 *
 * <p>If no version is specified, the builder will resolve to the latest version. Depending on the
 * {@link ParsedLogData} provided, Kernel can avoid expensive filesystem operations to improve
 * performance.
 */
@Experimental
public interface SnapshotBuilder {
  /**
   * Configures the builder to resolve the table at a specific version.
   *
   * @param version the version number to resolve to
   * @return a new builder instance configured for the specified version
   */
  SnapshotBuilder atVersion(long version);

  // TODO: atTimestamp

  /**
   * Provides a custom committer to use at transaction commit time.
   *
   * <p>Catalog implementations that wish to support the catalogManaged Delta table feature should
   * provide to engines their own catalog-specific Committer implementation which may, for example,
   * send a commit RPC to the catalog service to finalize the commit.
   *
   * <p>If no committer is provided, a default committer will be created that only supports writing
   * into filesystem-managed Delta tables.
   *
   * @param committer the committer to use
   * @return a new builder instance with the provided committer
   * @see io.delta.kernel.transaction.TransactionV2
   * @see Committer
   */
  SnapshotBuilder withCommitter(Committer committer);

  /**
   * Provides parsed log data to optimize table resolution.
   *
   * <p>When log data is provided, Kernel can avoid reading from the filesystem for information that
   * is already available in the parsed data, improving performance. Currently, only log data of
   * type {@link ParsedLogType#RATIFIED_STAGED_COMMIT} is supported.
   *
   * @param logData the parsed log data to use for optimization
   * @return a new builder instance with the provided log data
   */
  SnapshotBuilder withLogData(List<ParsedLogData> logData);

  /**
   * Provides protocol and metadata information to optimize table resolution.
   *
   * <p>When protocol and metadata are provided, Kernel can avoid reading this information from the
   * filesystem, improving performance.
   *
   * @param protocol the protocol information
   * @param metadata the metadata information
   * @return a new builder instance with the provided protocol and metadata
   */
  // TODO: [delta-io/delta#4820] Public Protocol API
  // TODO: [delta-io/delta#4821] Public Metadata API
  SnapshotBuilder withProtocolAndMetadata(Protocol protocol, Metadata metadata);

  /**
   * Constructs the {@link Snapshot} using the provided engine.
   *
   * <p>This method will read any missing information from the filesystem using the provided engine
   * to complete the snapshot resolution process.
   *
   * @param engine the engine to use for filesystem operations
   * @return the resolved snapshot instance
   */
  Snapshot build(Engine engine);
}
