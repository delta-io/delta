/*
 * Copyright (2025) The Delta Lake Project Authors.
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

import io.delta.kernel.types.CollationIdentifier
import org.scalatest.funsuite.AnyFunSuite

import java.util.Locale

class CollatedPredicateSuite extends AnyFunSuite {
  test("check invalid operation") {
    Seq(
      "anD",
      "oR",
      "ELEMENT_AT",
      "SUBstring"
    ).foreach {
      operationName =>
        val e = intercept[IllegalArgumentException] {
          new CollatedPredicate(operationName, new Column("c1"), new Column("c2"),
            CollationIdentifier.fromString("SPARK.UTF8_LCASE"))
        }
        assert(e.getMessage.contains(s"Collation is not supported for operator" +
          s" ${operationName.toUpperCase(Locale.ENGLISH)}."))
    }
  }

  test("check toString") {
    Seq(
      (
        new CollatedPredicate("<", new Column("c1"), new Column("c2"),
          CollationIdentifier.fromString("SPARK.UTF8_LCASE")),
        "(column(`c1`) < column(`c2`) COLLATE SPARK.UTF8_LCASE)"
      ),
      (
        new CollatedPredicate(">=", Literal.ofString("a"), new Column("c1"),
          CollationIdentifier.fromString("ICU.sr_Cyrl_SRB.75.1")),
        "(a >= column(`c1`) COLLATE ICU.SR_CYRL_SRB.75.1)"
      ),
      (
        new CollatedPredicate("stARtS_wiTh", new Column("c1"), Literal.ofString("a"),
          CollationIdentifier.fromString("ICU.en_US")),
        "(column(`c1`) STARTS_WITH a COLLATE ICU.EN_US)"
      )
    ).foreach {
      case (collatedPredicate, expectedToString) =>
        assert(collatedPredicate.toString == expectedToString)
    }
  }
}
