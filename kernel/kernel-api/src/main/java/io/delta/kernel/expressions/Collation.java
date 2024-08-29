package io.delta.kernel.expressions;

import java.util.Comparator;
import java.util.Optional;

public interface Collation {
  public static final String PROVIDER_KERNEL = "kernel";
  public static final String PROVIDER_ICU = "icu";

  public static final String DEFAULT_COLLATION_NAME = "UTF8_BINARY";

  public static final Collation DEFAULT_COLLATION =
          new CollationICU(DEFAULT_COLLATION_NAME, Optional.of("1.0"));
}
