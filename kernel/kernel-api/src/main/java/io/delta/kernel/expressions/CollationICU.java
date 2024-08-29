package io.delta.kernel.expressions;

import java.text.Collator;
import java.util.Comparator;
import java.util.Locale;
import java.util.Optional;

public class CollationICU implements Collation {

  private final String provider;
  private final String name;
  private final Optional<String> version;
  private final Comparator<String> comparator;

  public CollationICU(String name, Optional<String> version) {
    this.provider = Collation.PROVIDER_ICU;
    this.name = name;
    this.version = version;

    Locale locale = new Locale(name);
    Collator collator = Collator.getInstance(locale);
    comparator = collator::compare;
  }

  public Comparator<String> getComparator() {
    return comparator;
  }
}
