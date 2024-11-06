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
import io.delta.kernel.utils.CloseableIterator;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * This iterator wraps around a data action {@link CloseableIterator} and performs additional checks
 * for {@link DomainMetadata} actions.
 *
 * <ul>
 *   <li>Ensures domain metadata actions are only present when supported by the table protocol.
 *   <li>Validates that there are no duplicate domain metadata actions for the same domain in the
 *       provided actions.
 *   <li>Throws an exception if the above requirements are not met.
 * </ul>
 */
public class ValidateDomainMetadataIterator implements CloseableIterator<Row> {
  private final boolean domainMetadataSupported;
  private final CloseableIterator<Row> dataActionsIter;
  private final int domainMetadataOrdinal;
  private final Map<String, DomainMetadata> domainMetadataMap = new HashMap<>();

  public ValidateDomainMetadataIterator(
      Protocol protocol, CloseableIterator<Row> dataActionsIter, StructType schema) {
    this.domainMetadataSupported = DomainMetadataUtils.isDomainMetadataSupported(protocol);
    this.dataActionsIter = dataActionsIter;
    this.domainMetadataOrdinal = schema.indexOf("domainMetadata");
    if (domainMetadataOrdinal == -1) {
      throw new IllegalArgumentException("Schema does not contain 'domainMetadata' field");
    }
  }

  @Override
  public boolean hasNext() {
    return dataActionsIter.hasNext();
  }

  @Override
  public Row next() {
    Row action = dataActionsIter.next();
    if (!action.isNullAt(domainMetadataOrdinal)) {
      // This is a DomainMetadata action
      if (!domainMetadataSupported) {
        throw DeltaErrors.domainMetadataUnsupported();
      }

      // Extract DomainMetadata action
      DomainMetadata domainMetadata =
          DomainMetadata.fromRow(action.getStruct(domainMetadataOrdinal));

      // Check if domain already seen
      String domain = domainMetadata.getDomain();
      if (domainMetadataMap.containsKey(domain)) {
        throw DeltaErrors.duplicateDomainMetadataAction(
            domain, domainMetadataMap.get(domain).toString(), domainMetadata.toString());
      }
      domainMetadataMap.put(domain, domainMetadata);
    }
    return action;
  }

  @Override
  public void close() throws IOException {
    dataActionsIter.close();
  }
}
