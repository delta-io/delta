/*
 * Copyright (2026) The Delta Lake Project Authors.
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

/**
 * Thrown when the presence of a maximum catalog version does not match the snapshot's table
 * protocol. Catalog-managed snapshots require a maximum catalog version; filesystem-managed
 * snapshots must not have one.
 *
 * <p>This mismatch can occur when a table changes between catalog-managed and filesystem-managed
 * modes. It can also indicate incorrect snapshot builder inputs, so it does not by itself establish
 * that a transition occurred or that retrying will succeed.
 *
 * @since 4.5.0
 */
@Evolving
public class MaxCatalogVersionException extends KernelException {

  public MaxCatalogVersionException(String message) {
    super(message);
  }
}
