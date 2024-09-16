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
package io.delta.kernel.types;

import io.delta.kernel.annotation.Evolving;

import java.util.Objects;
import java.util.Optional;

import static io.delta.kernel.internal.util.Preconditions.checkArgument;

/**
 * Identifies collation for string type.
 * <a href="https://github.com/delta-io/delta/blob/master/protocol_rfcs/collated-string-type.md#collation-identifiers">
 *   Collation identifiers</a>
 *
 * @since 3.3.0
 */
@Evolving
public class CollationIdentifier {

  private final String provider;
  private final String name;
  private final Optional<String> version;

  public CollationIdentifier(String provider, String collationName) {
    Objects.requireNonNull(provider, "Collation provider cannot be null.");
    Objects.requireNonNull(collationName, "Collation name cannot be null.");

    this.provider = provider.toUpperCase();
    this.name = collationName.toUpperCase();
    this.version = Optional.empty();
  }

  public CollationIdentifier(String provider, String collationName, Optional<String> version) {
    Objects.requireNonNull(provider, "Collation provider cannot be null.");
    Objects.requireNonNull(collationName, "Collation name cannot be null.");
    Objects.requireNonNull(version, "Provider version cannot be null.");

    this.provider = provider.toUpperCase();
    this.name = collationName.toUpperCase();
    this.version = version.map(String::toUpperCase);
  }

  /**
   *
   * @return collation provider.
   */
  public String getProvider() {
    return provider;
  }

  /**
   *
   * @return collation name.
   */
  public String getName() {
    return name;
  }

  /**
   *
   * @return provider version.
   */
  public Optional<String> getVersion() {
    return version;
  }

  /**
   *
   * @param identifier collation identifier in string form of <br>{@code PROVIDER.COLLATION_NAME[.PROVIDER_VERSION]}.
   * @return appropriate collation identifier object
   */
  public static CollationIdentifier fromString(String identifier) {
    long numDots = identifier.chars().filter(ch -> ch == '.').count();
    checkArgument(numDots > 0, String.format("Invalid collation identifier: %s", identifier));
    if (numDots == 1) {
      String[] parts = identifier.split("\\.");
      return new CollationIdentifier(parts[0], parts[1]);
    } else {
      String[] parts = identifier.split("\\.", 3);
      return new CollationIdentifier(parts[0], parts[1], Optional.of(parts[2]));
    }
  }

  /**
   * Collation identifiers are identical when the provider, name, and version are the same.
   */
  @Override
  public boolean equals(Object o) {
    if (!(o instanceof CollationIdentifier)) {
      return false;
    }

    CollationIdentifier other = (CollationIdentifier) o;
    return this.provider.equals(other.provider)
            && this.name.equals(other.name)
            && this.version.equals(other.version);
  }

  /**
   *
   * @return collation identifier in form of {@code PROVIDER.COLLATION_NAME}.
   */
  public String toStringWithoutVersion() {
    return String.format("%s.%s", provider, name);
  }

  /**
   *
   * @return collation identifier in form of {@code PROVIDER.COLLATION_NAME[.PROVIDER_VERSION]}
   */
  @Override
  public String toString() {
    if (version.isPresent()) {
      return String.format("%s.%s.%s", provider, name, version.get());
    } else {
      return String.format("%s.%s", provider, name);
    }
  }
}
