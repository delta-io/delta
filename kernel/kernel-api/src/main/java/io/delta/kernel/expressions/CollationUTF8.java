package io.delta.kernel.expressions;

import java.util.Comparator;
import java.util.Optional;

public class CollationUTF8 {

  private final String provider;
  private final String name;
  private final Optional<String> version;
  private final Comparator<String> comparator;

  public CollationUTF8(String name, Optional<String> version) {
    this.provider = Collation.PROVIDER_KERNEL;
    this.name = name;
    this.version = version;

    if (name == null) {
      // TODO: throw exception?
    } else if (name.equals(Collation.DEFAULT_COLLATION_NAME)) {
      comparator = STRING_COMPARATOR
    }
  }
}
