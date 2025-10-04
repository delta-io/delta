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

import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.TableConfig;
import io.delta.kernel.internal.actions.Protocol;
import io.delta.kernel.internal.files.ParsedDeltaData;
import io.delta.kernel.internal.tablefeatures.TableFeatures;
import io.delta.kernel.internal.util.FileNames;
import io.delta.kernel.internal.util.Utils;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.FileStatus;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.util.HashMap;
import java.util.Map;

public class CatalogCommitterUtils {
  private CatalogCommitterUtils() {}

  /**
   * Table property key to enable the catalogManaged table feature. This is a signal to Kernel to
   * add this table feature to Kernel's protocol. This property won't be written to the delta
   * metadata.
   */
  public static final String CATALOG_MANAGED_ENABLEMENT_KEY =
      TableFeatures.SET_TABLE_FEATURE_SUPPORTED_PREFIX
          + TableFeatures.CATALOG_MANAGED_R_W_FEATURE_PREVIEW.featureName();

  /** Property key that specifies which version last updated the catalog entry. */
  public static final String METASTORE_LAST_UPDATE_VERSION = "delta.lastUpdateVersion";

  /**
   * Property key that specifies the timestamp (in milliseconds since the Unix epoch) of the last
   * commit that updated the catalog entry.
   */
  public static final String METASTORE_LAST_COMMIT_TIMESTAMP = "delta.lastCommitTimestamp";

  /**
   * Extract protocol-related properties from the given protocol.
   *
   * <p>For a Protocol(3, 7) with reader features ["columnMapping", "deletionVectors"] and writer
   * features ["appendOnly", "columnMapping"], this would return properties like:
   *
   * <ul>
   *   <li>delta.minReaderVersion: 3
   *   <li>delta.minWriterVersion: 7
   *   <li>delta.feature.columnMapping: supported
   *   <li>delta.feature.deletionVectors: supported
   *   <li>delta.feature.appendOnly: supported
   * </ul>
   */
  public static Map<String, String> extractProtocolProperties(Protocol protocol) {
    final Map<String, String> properties = new HashMap<>();

    properties.put(
        TableConfig.MIN_PROTOCOL_READER_VERSION_KEY,
        String.valueOf(protocol.getMinReaderVersion()));
    properties.put(
        TableConfig.MIN_PROTOCOL_WRITER_VERSION_KEY,
        String.valueOf(protocol.getMinWriterVersion()));

    if (protocol.supportsReaderFeatures() || protocol.supportsWriterFeatures()) {
      for (String featureName : protocol.getReaderAndWriterFeatures()) {
        properties.put(
            TableFeatures.SET_TABLE_FEATURE_SUPPORTED_PREFIX + featureName,
            TableFeatures.SET_TABLE_FEATURE_SUPPORTED_VALUE);
      }
    }

    return properties;
  }

  /**
   * Returns the published Delta file path for a catalog commit.
   *
   * @param logPath the path to the Delta log directory
   * @param catalogCommit the catalog commit containing the version
   * @return the path where the catalog commit should be published (e.g.,
   *     _delta_log/00000000000000000001.json)
   */
  public static String getPublishedDeltaFilePath(String logPath, ParsedDeltaData catalogCommit) {
    return FileNames.deltaFile(logPath, catalogCommit.getVersion());
  }

  /**
   * Publishes a catalog commit to its final location in the Delta log.
   *
   * <p>This method copies a staged catalog commit to its published location using atomic
   * PUT-if-absent semantics. If the file already exists at the destination, the operation completes
   * successfully (idempotent).
   *
   * @param engine the engine to use for reading and writing files
   * @param logPath the path to the Delta log directory
   * @param catalogCommit the catalog commit to publish
   * @throws UnsupportedOperationException if the catalog commit contains inline data
   * @throws IOException if an I/O error occurs during publishing
   */
  public static void publishCatalogCommit(
      Engine engine, String logPath, ParsedDeltaData catalogCommit) throws IOException {
    if (!catalogCommit.isFile()) {
      throw new UnsupportedOperationException("Publishing inline catalog commits is not supported");
    }

    final FileStatus stagedFile = catalogCommit.getFileStatus();
    final String publishedPath = getPublishedDeltaFilePath(logPath, catalogCommit);

    try (CloseableIterator<FileStatus> fileIter = Utils.singletonCloseableIterator(stagedFile);
        CloseableIterator<ColumnarBatch> batchIter =
            engine
                .getJsonHandler()
                .readJsonFiles(fileIter, null /* physicalSchema */, null /* predicate */);
        CloseableIterator<Row> rows = batchIter.flatMap(ColumnarBatch::getRows)) {

      // Write to the published location with PUT-if-absent semantics
      engine.getJsonHandler().writeJsonFileAtomically(publishedPath, rows, false /* overwrite */);
    } catch (FileAlreadyExistsException e) {
      // File already exists at destination - this is okay (idempotent operation)
      // Just return successfully
    }
  }
}
