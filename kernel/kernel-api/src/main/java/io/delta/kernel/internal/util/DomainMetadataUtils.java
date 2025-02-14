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
import io.delta.kernel.internal.tablefeatures.TableFeatures;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DomainMetadataUtils {

  private DomainMetadataUtils() {
    // Empty private constructor to prevent instantiation
  }

  /**
   * Populate the map of domain metadata from actions. When encountering duplicate domain metadata
   * actions for the same domain, this method preserves the first seen entry and skips subsequent
   * entries. This behavior is especially useful for log replay as we want to ensure that earlier
   * domain metadata entries take precedence over later ones.
   *
   * @param domainMetadataActionVector A {@link ColumnVector} containing the domain metadata rows
   * @param domainMetadataMap The existing map to be populated with domain metadata entries, where
   *     the key is the domain name and the value is the domain metadata
   */
  public static void populateDomainMetadataMap(
      ColumnVector domainMetadataActionVector, Map<String, DomainMetadata> domainMetadataMap) {
    final int vectorSize = domainMetadataActionVector.getSize();
    for (int rowId = 0; rowId < vectorSize; rowId++) {
      DomainMetadata dm = DomainMetadata.fromColumnVector(domainMetadataActionVector, rowId);
      if (dm != null && !domainMetadataMap.containsKey(dm.getDomain())) {
        // We only add the domain metadata if its domain name not already present in the map
        domainMetadataMap.put(dm.getDomain(), dm);
      }
    }
  }

  /**
   * Validates the list of domain metadata actions before committing them. It ensures that
   *
   * <ol>
   *   <li>domain metadata actions are only present when supported by the table protocol
   *   <li>there are no duplicate domain metadata actions for the same domain in the provided
   *       actions.
   * </ol>
   *
   * @param domainMetadataActions The list of domain metadata actions to validate
   * @param protocol The protocol to check for domain metadata support
   */
  public static void validateDomainMetadatas(
      List<DomainMetadata> domainMetadataActions, Protocol protocol) {
    if (domainMetadataActions.isEmpty()) return;

    // The list of domain metadata is non-empty, so the protocol must support domain metadata
    if (!TableFeatures.isDomainMetadataEnabled(protocol)) {
      throw DeltaErrors.domainMetadataUnsupported();
    }

    Map<String, DomainMetadata> domainMetadataMap = new HashMap<>();
    for (DomainMetadata domainMetadata : domainMetadataActions) {
      String domain = domainMetadata.getDomain();
      if (domainMetadataMap.containsKey(domain)) {
        String message =
            String.format(
                "Multiple actions detected for domain '%s' in single transaction: '%s' and '%s'. "
                    + "Only one action per domain is allowed.",
                domain, domainMetadataMap.get(domain).toString(), domainMetadata.toString());
        throw new IllegalArgumentException(message);
      }
      domainMetadataMap.put(domain, domainMetadata);
    }
  }
}
