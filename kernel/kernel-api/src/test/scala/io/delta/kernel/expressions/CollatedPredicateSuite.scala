package io.delta.kernel.expressions

import io.delta.kernel.types.CollationIdentifier
import org.scalatest.funsuite.AnyFunSuite

class CollatedPredicateSuite extends AnyFunSuite {

  test("check toString") {
    Seq(
      (
        new CollatedPredicate("<", new Column("c1"), new Column("c2"),
          CollationIdentifier.fromString("SPARK.UTF8_LCASE")),
        "(column(`c1`) < column(`c2`) COLLATE SPARK.UTF8_LCASE)",
      ),
      (
        new CollatedPredicate(">=", Literal.ofString("a"), new Column("c1"),
          CollationIdentifier.fromString("ICU.sr_Cyrl_SRB.75.1")),
        "(a >= column(`c1`) COLLATE ICU.SR_CYRL_SRB.75.1)",
      ),
      (
        new CollatedPredicate("stARtS_wiTh", new Column("c1"), Literal.ofString("a"),
          CollationIdentifier.fromString("ICU.en_US")),
        "(column(`c1`) STARTS_WITH a COLLATE ICU.EN_US)",
      )
    ).foreach {
      case (collatedPredicate, expectedToString) =>
        assert(collatedPredicate.toString == expectedToString)
    }
  }
}
