/*
 * Copyright (2023) The Delta Lake Project Authors.
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

/**
 * The data type representing {@code string} type values.
 *
 * @since 3.0.0
 */
@Evolving
public class StringType extends BasePrimitiveType {
  public static final StringType STRING =
      new StringType(CollationIdentifier.fromString("SPARK.UTF8_BINARY"));

  private final CollationIdentifier collationIdentifier;

  /**
   *
   * @param collationIdentifier An identifier representing the collation to be used for string comparison and sorting.
   *                            This determines how strings will be ordered and compared in query operations.
   */
  public StringType(CollationIdentifier collationIdentifier) {
    super("string");
    this.collationIdentifier = collationIdentifier;
  }

  /**
   *
   * @param collationName name of collation in which this StringType will be observed.
   *                      In form of {@code PROVIDER.COLLATION_NAME[.VERSION]}
   */
  public StringType(String collationName) {
    super("string");
    this.collationIdentifier = CollationIdentifier.fromString(collationName);
  }

  /**
   *
   * @return StringType's collation identifier
   */
  public CollationIdentifier getCollationIdentifier() {
    return collationIdentifier;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof StringType)) {
      return false;
    }

    StringType that = (StringType) o;
    return collationIdentifier.equals(that.collationIdentifier);
  }
}
