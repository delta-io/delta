package io.delta.kernel.expressions;

import java.util.Optional;

public class Collation {
  public static final String PROVIDER_KERNEL = "kernel";
  public static final String PROVIDER_ICU = "icu";

  public static final String DEFAULT_COLLATION_NAME = "UTF8_BINARY";

  public static final Collation DEFAULT_COLLATION =
          new Collation(PROVIDER_KERNEL, DEFAULT_COLLATION_NAME, Optional.of("1.0"));

  private final String provider;
  private final String name;
  private final Optional<String> version;

  public Collation(String provider, String name, Optional<String> version) {
    this.provider = provider;
    this.name = name;
    this.version = version;
  }
}
