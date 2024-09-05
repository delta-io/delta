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
        s"$PROVIDER_ICU.sr_Cyrl_SRB.75.1",
        new CollationIdentifier(PROVIDER_ICU, "sr_Cyrl_SRB", Optional.of("75.1"))
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
        s"$PROVIDER_ICU.SR_CYRL_SRB"
      ),
      (
        new CollationIdentifier(PROVIDER_ICU, "sr_Cyrl_SRB", Optional.of("75.1")),
        s"$PROVIDER_ICU.SR_CYRL_SRB"
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
        s"$PROVIDER_ICU.SR_CYRL_SRB"
      ),
      (
        new CollationIdentifier(PROVIDER_ICU, "sr_Cyrl_SRB", Optional.of("75.1")),
        s"$PROVIDER_ICU.SR_CYRL_SRB.75.1"
      )
    ).foreach {
      case(collationIdentifier, toString) =>
        assert(collationIdentifier.toString == toString)
    }
  }
}
