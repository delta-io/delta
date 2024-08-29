package io.delta.kernel.expressions;

import java.nio.charset.StandardCharsets;
import java.util.Comparator;
import java.util.Optional;

public class CollationUTF8 {

  private static final Comparator<String> BINARY_COMPARE =
      (leftOp, rightOp) -> {
        byte[] leftBytes = leftOp.getBytes(StandardCharsets.UTF_8);
        byte[] rightBytes = rightOp.getBytes(StandardCharsets.UTF_8);
        int i = 0;
        while (i < leftBytes.length && i < rightBytes.length) {
          if (leftBytes[i] != rightBytes[i]) {
            return Byte.toUnsignedInt(leftBytes[i]) - Byte.toUnsignedInt(rightBytes[i]);
          }
          i++;
        }
        return Integer.compare(leftBytes.length, rightBytes.length);
      };

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
      comparator = null;
    } else if (name.equals(Collation.DEFAULT_COLLATION_NAME)) {
      comparator = BINARY_COMPARE;
    } else {
      // TODO
      comparator = null;
    }
  }

  public Comparator<String> getComparator() {
    return comparator;
  }
}
