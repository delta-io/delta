/*
 * Copyright (2026) The Delta Lake Project Authors.
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

package io.delta.storage.commit.uccommitcoordinator;

import io.delta.storage.commit.Commit;
import io.delta.storage.commit.actions.AbstractMetadata;
import io.delta.storage.commit.actions.AbstractProtocol;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Storage-level envelope returned by {@link UCDeltaClient#loadTable},
 * {@link UCDeltaClient#createTable}, and {@link UCDeltaClient#commit}.
 *
 * <p>Holds only Delta storage types ({@link AbstractMetadata}, {@link AbstractProtocol},
 * {@link Commit}) so that non-Spark consumers (Kernel, Flink) can read it without pulling
 * in the UC SDK. Callers that need the DRC-native columnar representation of the schema
 * cast {@code metadata} to {@code DRCMetadataAdapter}.
 *
 * <p>{@code etag} is {@link Optional} in v1 (design clarifications §3 -- etag optional).
 * An absent etag still returns a valid response; callers that would otherwise emit
 * {@code assert-etag} on the next commit must skip that requirement.
 *
 * <p>Immutable. {@code unbackfilledCommits} is wrapped with
 * {@link Collections#unmodifiableList(List)}.
 */
public final class UCLoadTableResponse {

  private final String tableUuid;
  private final AbstractMetadata metadata;
  private final AbstractProtocol protocol;
  private final List<Commit> unbackfilledCommits;
  private final long latestTableVersion;
  private final Optional<String> etag;

  public UCLoadTableResponse(
      String tableUuid,
      AbstractMetadata metadata,
      AbstractProtocol protocol,
      List<Commit> unbackfilledCommits,
      long latestTableVersion,
      Optional<String> etag) {
    this.tableUuid = Objects.requireNonNull(tableUuid, "tableUuid");
    this.metadata = Objects.requireNonNull(metadata, "metadata");
    this.protocol = Objects.requireNonNull(protocol, "protocol");
    this.unbackfilledCommits = Collections.unmodifiableList(
        Objects.requireNonNull(unbackfilledCommits, "unbackfilledCommits"));
    this.latestTableVersion = latestTableVersion;
    this.etag = Objects.requireNonNull(etag, "etag");
  }

  public String getTableUuid() { return tableUuid; }
  public AbstractMetadata getMetadata() { return metadata; }
  public AbstractProtocol getProtocol() { return protocol; }
  public List<Commit> getUnbackfilledCommits() { return unbackfilledCommits; }
  public long getLatestTableVersion() { return latestTableVersion; }
  public Optional<String> getEtag() { return etag; }

  @Override
  public String toString() {
    return "UCLoadTableResponse{tableUuid='" + tableUuid
        + "', latestTableVersion=" + latestTableVersion
        + ", etagPresent=" + etag.isPresent()
        + ", unbackfilledCommitCount=" + unbackfilledCommits.size() + '}';
  }
}
