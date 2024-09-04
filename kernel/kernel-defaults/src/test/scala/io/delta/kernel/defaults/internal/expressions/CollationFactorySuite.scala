package io.delta.kernel.defaults.internal.expressions

import io.delta.kernel.defaults.internal.expressions.CollationFactory.fetchCollation
import io.delta.kernel.expressions.CollationIdentifier
import org.scalatest.funsuite.AnyFunSuite

import java.util.function.BiFunction

class CollationFactorySuite extends AnyFunSuite {
   test("basic ICU collator checks") {
    // scalastyle:off nonascii
    Seq(
      CollationTestCase("UNICODE_CI", "a", "A", true),
      CollationTestCase("UNICODE_CI", "a", "å", false),
      CollationTestCase("UNICODE_CI", "a", "Å", false),
      CollationTestCase("UNICODE_AI", "a", "A", false),
      CollationTestCase("UNICODE_AI", "a", "å", true),
      CollationTestCase("UNICODE_AI", "a", "Å", false),
      CollationTestCase("UNICODE_CI_AI", "a", "A", true),
      CollationTestCase("UNICODE_CI_AI", "a", "å", true),
      CollationTestCase("UNICODE_CI_AI", "a", "Å", true)
    ).foreach {
      testCase =>
        assert(testCase.equalsFunction(testCase.s1, testCase.s2) == testCase.expectedResult)
    }

    Seq(
      CollationTestCase("en", "a", "A", -1),
      CollationTestCase("en_CI", "a", "A", 0),
      CollationTestCase("en_AI", "a", "å", 0),
      CollationTestCase("sv", "Kypper", "Köpfe", -1),
      CollationTestCase("de", "Kypper", "Köpfe", 1)
    ).foreach {
      testCase =>
        assert(
          Integer.signum(testCase.compare(testCase.s1, testCase.s2)) == testCase.expectedResult)
    }
    // scalastyle:on nonascii
  }

  test("collation aware compare") {
    Seq(
      CollationTestCase("UTF8_BINARY", "aaa", "aaa", 0),
      CollationTestCase("UTF8_BINARY", "aaa", "AAA", 1),
      CollationTestCase("UTF8_BINARY", "aaa", "bbb", -1),
      CollationTestCase("UTF8_BINARY", "aaa", "BBB", 1),
      CollationTestCase("UNICODE", "aaa", "aaa", 0),
      CollationTestCase("UNICODE", "aaa", "AAA", -1),
      CollationTestCase("UNICODE", "aaa", "bbb", -1),
      CollationTestCase("UNICODE", "aaa", "BBB", -1),
      CollationTestCase("UNICODE_CI", "aaa", "aaa", 0),
      CollationTestCase("UNICODE_CI", "aaa", "AAA", 0),
      CollationTestCase("UNICODE_CI", "aaa", "bbb", -1)
    ).foreach {
      testCase =>
        assert(
          Integer.signum(testCase.compare(testCase.s1, testCase.s2)) == testCase.expectedResult)
    }
  }

  case class CollationTestCase[R](collationName: String,
                                  s1: String,
                                  s2: String,
                                  expectedResult: R) {
    val provider =
      if (collationName.startsWith("UTF8")) {
        CollationIdentifier.PROVIDER_SPARK
      } else {
        CollationIdentifier.PROVIDER_ICU
      }
    val fullCollationName = String.format("%s.%s", provider, collationName)
    val collationIdentifier = CollationIdentifier.fromString(fullCollationName)
    val collation = fetchCollation(collationIdentifier)
    val equalsFunction: BiFunction[String, String, java.lang.Boolean] = collation.getEqualsFunction
    val compare: (String, String) => Int = collation.getComparator.compare
  }
}
