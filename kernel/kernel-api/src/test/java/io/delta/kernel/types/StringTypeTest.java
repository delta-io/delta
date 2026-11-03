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
package io.delta.kernel.types;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class StringTypeTest {

  private static Stream<Arguments> equalsCases() {
    return Stream.of(
        Arguments.of(StringType.STRING, StringType.STRING, true),
        Arguments.of(StringType.STRING, new StringType("sPark.UTF8_bINary"), true),
        Arguments.of(StringType.STRING, new StringType("SPARK.UTF8_LCASE"), false),
        Arguments.of(new StringType("ICU.UNICODE"), new StringType("SPARK.UTF8_LCASE"), false),
        Arguments.of(new StringType("ICU.UNICODE"), new StringType("ICU.UNICODE_CI"), false),
        Arguments.of(new StringType("ICU.UNICODE_CI"), new StringType("icU.uniCODe_Ci"), true));
  }

  @ParameterizedTest(name = "{0}.equals({1}) => {2}")
  @MethodSource("equalsCases")
  void equalsHonorsCollationIdentity(StringType left, StringType right, boolean expected) {
    assertEquals(expected, left.equals(right));
    // Collation identity comparison must be symmetric.
    assertEquals(expected, right.equals(left));
  }

  @Test
  void equalsIsNullOrForeignTypeSafeAndReflexive() {
    StringType custom = new StringType("ICU.UNICODE_CI");

    assertFalse(custom.equals(null));
    assertFalse(custom.equals(new Object()));
    assertFalse(custom.equals(IntegerType.INTEGER));

    assertTrue(custom.equals(custom));
    assertTrue(StringType.STRING.equals(StringType.STRING));
  }

  @Test
  void equalInstancesHaveEqualHashCodes() {
    assertEquals(StringType.STRING.hashCode(), new StringType("sPark.UTF8_bINary").hashCode());
  }

  private static Stream<Arguments> utf8BinaryCollationCases() {
    return Stream.of(
        Arguments.of(StringType.STRING, true),
        Arguments.of(new StringType("sPark.UTF8_bINary"), true),
        Arguments.of(new StringType("SPARK.UTF8_LCASE"), false),
        Arguments.of(new StringType("ICU.UNICODE.72.2"), false),
        Arguments.of(new StringType("ICU.UNICODE_CI"), false));
  }

  @ParameterizedTest
  @MethodSource("utf8BinaryCollationCases")
  void isUTF8BinaryCollated(StringType stringType, boolean expected) {
    assertEquals(expected, stringType.isUTF8BinaryCollated());
  }

  private static Stream<Arguments> toStringCases() {
    return Stream.of(
        Arguments.of(StringType.STRING, "string"),
        Arguments.of(new StringType("sPark.UTF8_bINary"), "string"),
        Arguments.of(new StringType("SPARK.UTF8_LCASE"), "string collate UTF8_LCASE"),
        Arguments.of(new StringType("ICU.uNICoDE.72.2"), "string collate UNICODE"),
        Arguments.of(new StringType("ICU.UNICODE_CI"), "string collate UNICODE_CI"));
  }

  @ParameterizedTest
  @MethodSource("toStringCases")
  void stringRepresentation(StringType stringType, String expected) {
    assertEquals(expected, stringType.toString());
  }
}
