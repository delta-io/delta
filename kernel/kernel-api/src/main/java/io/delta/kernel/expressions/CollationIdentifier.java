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
package io.delta.kernel.expressions;

import java.util.Optional;

public class CollationIdentifier {
  public static final String PROVIDER_SPARK = "SPARK";
  public static final String PROVIDER_ICU = "ICU";

  public static final String ICU_COLLATOR_VERSION = "75.1";

  public static final String DEFAULT_COLLATION_NAME = "UTF8_BINARY";

  public static final CollationIdentifier DEFAULT_COLLATION_IDENTIFIER =
          new CollationIdentifier(PROVIDER_SPARK, DEFAULT_COLLATION_NAME);

  private final String provider;
  private final String name;
  private final Optional<String> version;

  public CollationIdentifier(String provider, String collationName) {
    this.provider = provider.toUpperCase();
    this.name = collationName.toUpperCase();
    this.version = Optional.empty();
  }

  public CollationIdentifier(String provider, String collationName, Optional<String> version) {
    this.provider = provider.toUpperCase();
    this.name = collationName.toUpperCase();
    if (version.isPresent()) {
      this.version = Optional.of(version.get().toUpperCase());
    } else {
      this.version = Optional.empty();
    }
  }

  public String toStringWithoutVersion() {
    return String.format("%s.%s", provider, name);
  }

  public String getProvider() {
    return provider;
  }

  public String getName() {
    return name;
  }

  // Returns Optional.empty()
  public Optional<String> getVersion() {
    return version;
  }

  public static CollationIdentifier fromString(String identifier) {
    long numDots = identifier.chars().filter(ch -> ch == '.').count();
    if (numDots == 0) {
      throw new IllegalArgumentException(
              String.format("Invalid collation identifier: %s", identifier));
    } else if (numDots == 1) {
      String[] parts = identifier.split("\\.");
      return new CollationIdentifier(parts[0], parts[1]);
    } else {
      String[] parts = identifier.split("\\.", 3);
      return new CollationIdentifier(parts[0], parts[1], Optional.of(parts[2]));
    }
  }

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

  @Override
  public String toString() {
    if (version.isPresent()) {
      return String.format("%s.%s.%s", provider, name, version.get());
    } else {
      return String.format("%s.%s", provider, name);
    }
  }
}
