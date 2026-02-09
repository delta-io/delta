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
package io.delta.spark.internal.v2.catalog;

/**
 * Finalizes catalog-visible state after a successful Kernel commit.
 *
 * <p>Examples include registering a table in the session catalog, or performing catalog-specific
 * completion actions for catalog-managed tables. Implementations should treat {@link
 * #finalizeAfterCommit()} as best-effort: failures may leave the table committed on storage without
 * a catalog entry.
 */
public interface CatalogFinalizer {

  /** Run after a successful Kernel commit. */
  void finalizeAfterCommit();

  /**
   * Best-effort cleanup/diagnostics when commit/finalize fails.
   *
   * @param cause the failure that triggered abort
   */
  void abort(Throwable cause);
}
