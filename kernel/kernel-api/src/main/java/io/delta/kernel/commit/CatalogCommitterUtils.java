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

import io.delta.kernel.internal.TableConfig;
import io.delta.kernel.internal.actions.Protocol;
import io.delta.kernel.internal.tablefeatures.TableFeatures;
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
}
