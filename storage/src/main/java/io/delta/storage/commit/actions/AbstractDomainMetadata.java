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

package io.delta.storage.commit.actions;

/**
 * Interface for Delta domain-metadata actions. Each instance represents a per-domain
 * configuration entry (or a tombstone, when {@link #isRemoved()} is {@code true}). The
 * payload is opaque JSON; the consumer interprets it per the well-known {@link #getDomain()}
 * (e.g. {@code "delta.clustering"}, {@code "delta.rowTracking"}).
 */
public interface AbstractDomainMetadata {

  /** Domain identifier (e.g. {@code "delta.clustering"}). */
  String getDomain();

  /** Domain-specific configuration, serialized as JSON. */
  String getConfiguration();

  /** {@code true} when this entry is a tombstone removing a previously-set domain. */
  boolean isRemoved();
}
