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

package io.delta.kernel.unitycatalog;

import io.delta.kernel.internal.actions.CommitInfo;
import io.delta.kernel.internal.actions.DomainMetadata;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.actions.Protocol;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * Immutable result of parsing a single delta file. Contains the Protocol, Metadata, CommitInfo, and
 * DomainMetadata actions extracted from the JSON delta file.
 */
public class ParsedDeltaFileContents {

  private final Optional<Protocol> protocol;
  private final Optional<Metadata> metadata;
  private final Optional<CommitInfo> commitInfo;
  private final List<DomainMetadata> domainMetadatas;

  public ParsedDeltaFileContents(
      Optional<Protocol> protocol,
      Optional<Metadata> metadata,
      Optional<CommitInfo> commitInfo,
      List<DomainMetadata> domainMetadatas) {
    this.protocol = protocol;
    this.metadata = metadata;
    this.commitInfo = commitInfo;
    this.domainMetadatas = Collections.unmodifiableList(domainMetadatas);
  }

  /** Returns the Protocol action from the delta file, if present. */
  public Optional<Protocol> getProtocol() {
    return protocol;
  }

  /** Returns the Metadata action from the delta file, if present. */
  public Optional<Metadata> getMetadata() {
    return metadata;
  }

  /** Returns the CommitInfo action from the delta file, if present. */
  public Optional<CommitInfo> getCommitInfo() {
    return commitInfo;
  }

  /**
   * Returns all DomainMetadata actions from the delta file. A delta file can contain multiple
   * domain metadata actions (one per domain, e.g. clustering, row tracking).
   */
  public List<DomainMetadata> getDomainMetadatas() {
    return domainMetadatas;
  }
}
