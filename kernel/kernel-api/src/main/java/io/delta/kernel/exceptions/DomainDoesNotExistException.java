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
package io.delta.kernel.exceptions;

import io.delta.kernel.annotation.Evolving;

/** Thrown when attempting to remove a domain metadata that does not exist in the read snapshot. */
@Evolving
public class DomainDoesNotExistException extends KernelException {
  public DomainDoesNotExistException(String tablePath, String domain, long snapshotVersion) {
    super(
        String.format(
            "%s: Cannot remove domain metadata with identifier %s because it does not exist in the "
                + "read snapshot at version %s",
            tablePath, domain, snapshotVersion));
  }
}
