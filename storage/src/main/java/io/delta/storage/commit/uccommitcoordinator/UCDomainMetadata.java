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

package io.delta.storage.commit.uccommitcoordinator;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;

/**
 * Storage-level domain metadata entry passed through the {@link UCDeltaClient} surface.
 * Mirrors Delta's {@code DomainMetadata} action ({@code name} + {@code configuration} map)
 * but lives in storage so that non-Spark consumers can use the interface without pulling in
 * Spark.
 *
 * <p>Promotion to a first-class {@code AbstractDomainMetadata} interface in
 * {@code io.delta.storage.commit.actions} is a follow-up; for DRC v1 a concrete DTO is
 * enough.
 */
public final class UCDomainMetadata {

  private final String name;
  private final Map<String, String> configuration;
  private final boolean removed;

  public UCDomainMetadata(String name, Map<String, String> configuration, boolean removed) {
    this.name = Objects.requireNonNull(name, "name");
    if (name.isEmpty()) {
      throw new IllegalArgumentException("domain name must not be empty");
    }
    this.configuration = Collections.unmodifiableMap(
        Objects.requireNonNull(configuration, "configuration"));
    this.removed = removed;
  }

  public String getName() { return name; }
  public Map<String, String> getConfiguration() { return configuration; }
  public boolean isRemoved() { return removed; }

  @Override
  public String toString() {
    return "UCDomainMetadata{name='" + name + "', keys=" + configuration.keySet()
        + ", removed=" + removed + '}';
  }
}
