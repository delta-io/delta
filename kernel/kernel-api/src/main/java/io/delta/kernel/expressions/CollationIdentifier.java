package io.delta.kernel.expressions;

import java.util.Optional;

public class CollationIdentifier {
  public static final String PROVIDER_KERNEL = "KERNEL";
  public static final String PROVIDER_ICU = "ICU";

  public static final String DEFAULT_COLLATION_NAME = "UTF8_BINARY";

  public static final CollationIdentifier DEFAULT_COLLATION_IDENTIFIER =
          new CollationIdentifier(PROVIDER_KERNEL, DEFAULT_COLLATION_NAME, Optional.empty());

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
      throw new IllegalArgumentException(String.format("Invalid collation identifier: %s", identifier));
    } else if (parts.length == 2) {
      return new CollationIdentifier(parts[0], parts[1], Optional.empty());
    } else {
      return new CollationIdentifier(parts[0], parts[1], Optional.of(parts[2]));
    }
  }
}
