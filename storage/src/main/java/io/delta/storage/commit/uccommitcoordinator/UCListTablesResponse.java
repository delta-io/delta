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

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Paginated response for {@link UCDeltaClient#listTables}. {@link #getNextPageToken()}
 * is empty when the server has no more pages.
 */
public final class UCListTablesResponse {

  private final List<UCTableIdentifier> identifiers;
  private final Optional<String> nextPageToken;

  public UCListTablesResponse(
      List<UCTableIdentifier> identifiers, Optional<String> nextPageToken) {
    this.identifiers = Collections.unmodifiableList(
        Objects.requireNonNull(identifiers, "identifiers"));
    this.nextPageToken = Objects.requireNonNull(nextPageToken, "nextPageToken");
  }

  public List<UCTableIdentifier> getIdentifiers() { return identifiers; }
  public Optional<String> getNextPageToken() { return nextPageToken; }

  @Override
  public String toString() {
    return "UCListTablesResponse{count=" + identifiers.size()
        + ", hasNextPage=" + nextPageToken.isPresent() + '}';
  }
}
