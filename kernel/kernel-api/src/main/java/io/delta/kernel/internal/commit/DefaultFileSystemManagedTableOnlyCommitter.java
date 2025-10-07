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

import static io.delta.kernel.internal.DeltaErrors.wrapEngineExceptionThrowsIO;

import io.delta.kernel.commit.CommitFailedException;
import io.delta.kernel.commit.CommitMetadata;
import io.delta.kernel.commit.CommitResponse;
import io.delta.kernel.commit.Committer;
import io.delta.kernel.data.Row;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.DeltaErrorsInternal;
import io.delta.kernel.internal.actions.Protocol;
import io.delta.kernel.internal.files.ParsedPublishedDeltaData;
import io.delta.kernel.internal.tablefeatures.TableFeatures;
import io.delta.kernel.internal.util.FileNames;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.FileStatus;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultFileSystemManagedTableOnlyCommitter implements Committer {

  private static final Logger logger =
      LoggerFactory.getLogger(DefaultFileSystemManagedTableOnlyCommitter.class);

  public static final DefaultFileSystemManagedTableOnlyCommitter INSTANCE =
      new DefaultFileSystemManagedTableOnlyCommitter();

  private DefaultFileSystemManagedTableOnlyCommitter() {}

  @Override
  public CommitResponse commit(
      Engine engine, CloseableIterator<Row> finalizedActions, CommitMetadata commitMetadata)
      throws CommitFailedException {
    commitMetadata.getReadProtocolOpt().ifPresent(this::validateProtocol);
    commitMetadata.getNewProtocolOpt().ifPresent(this::validateProtocol);

    final String jsonCommitFile =
        FileNames.deltaFile(commitMetadata.getDeltaLogDirPath(), commitMetadata.getVersion());

    logger.info("Attempting to commit {}", jsonCommitFile);

    try {
      wrapEngineExceptionThrowsIO(
          () -> {
            engine
                .getJsonHandler()
                .writeJsonFileAtomically(jsonCommitFile, finalizedActions, false /* overwrite */);
            return null;
          },
          String.format("Write file actions to JSON log file `%s`", jsonCommitFile));
    } catch (FileAlreadyExistsException e) {
      throw new CommitFailedException(
          true /* retryable */,
          true /* conflict */,
          "Concurrent write detected for version " + commitMetadata.getVersion(),
          e);
    } catch (IOException e) {
      throw new CommitFailedException(
          true /* retryable */,
          false /* conflict */,
          "Failed to write commit file due to I/O error: " + e.getMessage(),
          e);
    }

    // TODO: [delta-io/delta#5021] Use FileSystemClient::getFileStatus API instead
    return new CommitResponse(
        ParsedPublishedDeltaData.forFileStatus(FileStatus.of(jsonCommitFile)));
  }

  private void validateProtocol(Protocol protocol) {
    if (TableFeatures.isCatalogManagedSupported(protocol)) {
      throw DeltaErrorsInternal.defaultCommitterDoesNotSupportCatalogManagedTables();
    }
  }
}
