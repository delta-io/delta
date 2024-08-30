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

  public static final String DEFAULT_COLLATION_NAME = "UTF8_BINARY";

  public static final CollationIdentifier DEFAULT_COLLATION_IDENTIFIER =
      new CollationIdentifier(PROVIDER_SPARK, DEFAULT_COLLATION_NAME, Optional.empty());

  private final String provider;
  private final String name;
  private final Optional<String> version;

  public CollationIdentifier(String provider, String name, Optional<String> version) {
    this.provider = provider;
    this.name = name;
    this.version = version;
  }

  public String toStringWithoutVersion() {
    return String.format("%s.%s", provider, name);
  }

  public static CollationIdentifier fromString(String identifier) {
    String[] parts = identifier.split("\\.");
    if (parts.length == 1) {
      throw new IllegalArgumentException(
          String.format("Invalid collation identifier: %s", identifier));
    } else if (parts.length == 2) {
      return new CollationIdentifier(parts[0], parts[1], Optional.empty());
    } else {
      return new CollationIdentifier(parts[0], parts[1], Optional.of(parts[2]));
    }
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof CollationIdentifier)) {
      return false;
    }

    CollationIdentifier that = (CollationIdentifier) o;
    return provider.equals(that.provider) && name.equals(that.name) && version.equals(that.version);
  }
}
