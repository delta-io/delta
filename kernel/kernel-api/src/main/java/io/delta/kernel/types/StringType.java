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
  public static final StringType STRING = new StringType(CollationIdentifier.SPARK_UTF8_BINARY);

  private final CollationIdentifier collationIdentifier;

  /**
   * @param collationIdentifier An identifier representing the collation to be used for string
   *     comparison and sorting. This determines how strings will be ordered and compared in query
   *     operations.
   */
  public StringType(CollationIdentifier collationIdentifier) {
    super("string");
    this.collationIdentifier = collationIdentifier;
  }

  /**
   * @param collationName name of collation in which this StringType will be observed. In form of
   *     {@code PROVIDER.COLLATION_NAME[.VERSION]}
   */
  public StringType(String collationName) {
    super("string");
    this.collationIdentifier = CollationIdentifier.fromString(collationName);
  }

  /** @return StringType's collation identifier */
  public CollationIdentifier getCollationIdentifier() {
    return collationIdentifier;
  }

  /**
   * Are the data types same? The metadata, collations or column names could be different.
   *
   * @param dataType
   * @return
   */
  public boolean equivalent(DataType dataType) {
    return dataType instanceof StringType;
  }

  /**
   * Checks whether the given {@code dataType} is compatible with this type when writing data.
   * Collation differences are ignored.
   *
   * <p>This method is intended to be used during the write path to validate that an input type
   * matches the expected schema before data is written.
   *
   * <p>It should not be used in other cases, such as the read path.
   *
   * @param dataType the input data type being written
   * @return {@code true} if the input type is compatible with this type.
   */
  @Override
  public boolean isWriteCompatible(DataType dataType) {
    return dataType instanceof StringType;
  }

  /** @return true if this StringType uses the default Spark UTF8_BINARY collation. */
  public boolean isUTF8BinaryCollated() {
    return collationIdentifier.isSparkUTF8BinaryCollation();
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof StringType)) {
      return false;
    }

    StringType that = (StringType) o;
    return collationIdentifier.equals(that.collationIdentifier);
  }

  /**
   * Override is needed because {@code toString()} may be used for schema serialization and similar
   * contexts, so collation information must be included.
   *
   * @return string representation of the StringType.
   */
  @Override
  public String toString() {
    if (isUTF8BinaryCollated()) {
      return super.toString();
    } else {
      return String.format("string collate %s", collationIdentifier.getName());
    }
  }
}
