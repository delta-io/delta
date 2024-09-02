package io.delta.kernel.expressions

import org.scalatest.funsuite.AnyFunSuite
import io.delta.kernel.expressions.CollationIdentifier._

import java.util.Optional

class CollationIdentifierSuite extends AnyFunSuite {
  test("check fromString with valid string") {
    Seq(
      (
        s"$PROVIDER_SPARK.$DEFAULT_COLLATION_NAME",
        DEFAULT_COLLATION_IDENTIFIER
      ),
      (
        s"$PROVIDER_ICU.sr_Cyrl_SRB",
        new CollationIdentifier(PROVIDER_ICU, "sr_Cyrl_SRB", Optional.empty())
      ),
      (
        s"$PROVIDER_ICU.sr_Cyrl_SRB.75",
        new CollationIdentifier(PROVIDER_ICU, "sr_Cyrl_SRB", Optional.of("75"))
      )
    ).foreach {
      case(stringIdentifier, collationIdentifier) =>
        assert(CollationIdentifier.fromString(stringIdentifier).equals(collationIdentifier))
    }
  }

  test("check fromString with invalid string") {
    Seq(
      PROVIDER_SPARK,
      s"${PROVIDER_SPARK}_sr_Cyrl_SRB"
    ).foreach {
      stringIdentifier =>
        val e = intercept[IllegalArgumentException] {
          val collationIdentifier = CollationIdentifier.fromString(stringIdentifier)
        }
        assert(e.getMessage == String.format("Invalid collation identifier: %s", stringIdentifier))
    }
  }

  test("check toStringWithoutVersion") {
    Seq(
      (
        DEFAULT_COLLATION_IDENTIFIER,
        s"$PROVIDER_SPARK.$DEFAULT_COLLATION_NAME"
      ),
      (
        new CollationIdentifier(PROVIDER_ICU, "sr_Cyrl_SRB", Optional.empty()),
        s"$PROVIDER_ICU.sr_Cyrl_SRB"
      ),
      (
        new CollationIdentifier(PROVIDER_ICU, "sr_Cyrl_SRB", Optional.of("75")),
        s"$PROVIDER_ICU.sr_Cyrl_SRB"
      )
    ).foreach {
      case(collationIdentifier, toStringWithoutVersion) =>
        assert(collationIdentifier.toStringWithoutVersion == toStringWithoutVersion)
    }
  }

  test("check toString") {
    Seq(
      (
        DEFAULT_COLLATION_IDENTIFIER,
        s"$PROVIDER_SPARK.$DEFAULT_COLLATION_NAME"
      ),
      (
        new CollationIdentifier(PROVIDER_ICU, "sr_Cyrl_SRB", Optional.empty()),
        s"$PROVIDER_ICU.sr_Cyrl_SRB"
      ),
      (
        new CollationIdentifier(PROVIDER_ICU, "sr_Cyrl_SRB", Optional.of("75")),
        s"$PROVIDER_ICU.sr_Cyrl_SRB.75"
      )
    ).foreach {
      case(collationIdentifier, toString) =>
        assert(collationIdentifier.toString == toString)
    }
  }
}
