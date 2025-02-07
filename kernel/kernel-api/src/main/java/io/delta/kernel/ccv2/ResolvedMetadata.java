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

package io.delta.kernel.ccv2;

import io.delta.kernel.TransactionCommitResult;
import io.delta.kernel.data.Row;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.actions.Protocol;
import io.delta.kernel.internal.snapshot.LogSegment;
import io.delta.kernel.utils.CloseableIterable;
import io.delta.kernel.utils.CloseableIterator;
import java.util.Optional;

public interface ResolvedMetadata {

  /////////////////////////////////////////////////
  // APIs to provide input information to Kernel //
  /////////////////////////////////////////////////

  /** @return The fully qualified path of the table. */
  String getPath();

  /** @return The resolved version of the table. */
  long getVersion();

  /**
   * @return A potentially partial {@link LogSegment}. If any deltas are present, then they
   *     represent a contiguous suffix of commits up until the given version.
   */
  Optional<LogSegment> getLogSegment();

  /**
   * @return The protocol of the table at the given version. If present, then {@link #getMetadata()}
   *     must also be present.
   */
  Optional<Protocol> getProtocol();

  /**
   * @return The metadata of the table at the given version. If present, then {@link #getProtocol()}
   *     must also be present.
   */
  Optional<Metadata> getMetadata();

  /** @return The schema of the table, at the given version, as a JSON-serialized string. */
  Optional<String> getSchemaString();

  /////////////////////////////////////////////////////////////
  // APIs for Kernel to interact with the invoking connector //
  /////////////////////////////////////////////////////////////

  TransactionCommitResult commit(
      long commitAsVersion,
      CloseableIterator<Row> actions,
      Optional<Protocol> newProtocol,
      Optional<Metadata> newMetadata);
}
