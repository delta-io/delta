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

import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.internal.DeltaErrors;
import io.delta.kernel.internal.actions.DomainMetadata;
import io.delta.kernel.internal.actions.Protocol;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DomainMetadataUtils {

  private DomainMetadataUtils() {
    // Empty private constructor to prevent instantiation
  }

  /**
   * Extracts a map of domain metadata from actions. There should be only one {@link DomainMetadata}
   * per domain name. It throws a KernelException if duplicate domain metadata is found.
   *
   * @param domainMetadataActionVector A {@link ColumnVector} containing the domain metadata rows
   * @return A map where the keys are domain names and the values are {@link DomainMetadata} objects
   */
  public static Map<String, DomainMetadata> extractDomainMetadataMap(
      ColumnVector domainMetadataActionVector) {
    // A map from domain name to DomainMetadata action
    Map<String, DomainMetadata> domainMetadataMap = new HashMap<>();

    final int vectorSize = domainMetadataActionVector.getSize();
    for (int rowId = 0; rowId < vectorSize; rowId++) {
      if (domainMetadataActionVector.isNullAt(rowId)) {
        // Skip non-domainMetadata actions
        continue;
      }

      // Extract DomainMetadata action
      DomainMetadata domainMetadata =
          DomainMetadata.fromColumnVector(domainMetadataActionVector, rowId);
      assert (domainMetadata != null);

      // Check if domain already seen
      String domain = domainMetadata.getDomain();
      if (domainMetadataMap.containsKey(domain)) {
        throw DeltaErrors.duplicateDomainMetadataAction(
            domain, domainMetadataMap.get(domain).toString(), domainMetadata.toString());
      }
      domainMetadataMap.put(domain, domainMetadata);
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
