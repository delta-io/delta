/*
 * Copyright (2024) The Delta Lake Project Authors.
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

package io.delta.kernel.internal.util;

import io.delta.kernel.data.Row;
import io.delta.kernel.internal.DeltaErrors;
import io.delta.kernel.internal.actions.DomainMetadata;
import io.delta.kernel.internal.actions.Protocol;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DomainMetadataUtils {

  /**
   * Extracts a map of domain metadata from actions. In one transaction, there can be only one
   * DomainMetadata action per domain name. It throws a KernelException if duplicate domain metadata
   * actions is found.
   *
   * @param actions an iterable of Row objects representing actions
   * @param schema the schema of the row
   * @param allowDuplicate whether to allow duplicate domain metadata actions
   * @return a map where the keys are domain names and the values are DomainMetadata objects
   */
  public static Map<String, DomainMetadata> extractDomainMetadataMap(
      CloseableIterable<Row> actions, StructType schema, boolean allowDuplicate) {
    Map<String, DomainMetadata> domainMetadataMap = new HashMap<>();

    final int domainMetadataOrdinal = schema.indexOf("domainMetadata");
    if (domainMetadataOrdinal == -1) {
      throw new IllegalArgumentException("Schema does not contain 'domainMetadata' field");
    }

    // Use try-with-resources to ensure that the CloseableIterable is closed after the loop
    try (CloseableIterable<Row> closeableDataActions = actions) {
      for (Row action : closeableDataActions) {
        // Only process DomainMetadata actions
        if (action.isNullAt(domainMetadataOrdinal)) continue;

        // Extract DomainMetadata action
        DomainMetadata domainMetadata =
            DomainMetadata.fromRow(action.getStruct(domainMetadataOrdinal));

        // Check if domain already seen
        String domain = domainMetadata.getDomain();
        if (!allowDuplicate && domainMetadataMap.containsKey(domain)) {
          throw DeltaErrors.duplicateDomainMetadataAction(
              domainMetadataMap.get(domain).toString(), domainMetadata.toString());
        }
        domainMetadataMap.put(domain, domainMetadata);
      }
    } catch (IOException ioe) {
      throw new UncheckedIOException(ioe);
    }

    return domainMetadataMap;
  }

  /**
   * Checks if the table protocol supports the "domainMetadata" writer feature.
   *
   * @param protocol the protocol to check
   * @return true if the "domainMetadata" feature is supported, false otherwise
   */
  public static boolean isDomainMetadataSupported(Protocol protocol) {
    List<String> writerFeatures = protocol.getWriterFeatures();
    if (writerFeatures == null) {
      return false;
    }
    return writerFeatures.contains(DomainMetadata.FEATURE_NAME)
        && protocol.getMinWriterVersion() >= DomainMetadata.MIN_WRITER_VERSION_REQUIRED;
  }
}
