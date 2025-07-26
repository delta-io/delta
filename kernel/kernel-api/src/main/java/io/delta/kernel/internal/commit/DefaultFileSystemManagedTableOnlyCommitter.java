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

package io.delta.kernel.internal.commit;

import io.delta.kernel.commit.CommitFailedException;
import io.delta.kernel.commit.CommitMetadata;
import io.delta.kernel.commit.CommitResponse;
import io.delta.kernel.commit.Committer;
import io.delta.kernel.data.Row;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.DeltaErrorsInternal;
import io.delta.kernel.internal.actions.Protocol;
import io.delta.kernel.internal.tablefeatures.TableFeatures;
import io.delta.kernel.utils.CloseableIterator;

public class DefaultFileSystemManagedTableOnlyCommitter implements Committer {
  public static final DefaultFileSystemManagedTableOnlyCommitter INSTANCE =
      new DefaultFileSystemManagedTableOnlyCommitter();

  private DefaultFileSystemManagedTableOnlyCommitter() {}

  @Override
  public CommitResponse commit(
      Engine engine, CloseableIterator<Row> finalizedActions, CommitMetadata commitMetadata)
      throws CommitFailedException {
    commitMetadata.getReadProtocolOpt().ifPresent(this::validateProtocol);
    commitMetadata.getNewProtocolOpt().ifPresent(this::validateProtocol);
    throw new UnsupportedOperationException("Default Committer not yet implemented");
  }

  private void validateProtocol(Protocol protocol) {
    if (TableFeatures.isCatalogManagedSupported(protocol)) {
      throw DeltaErrorsInternal.defaultCommitterDoesNotSupportCatalogManagedTables();
    }
  }
}
