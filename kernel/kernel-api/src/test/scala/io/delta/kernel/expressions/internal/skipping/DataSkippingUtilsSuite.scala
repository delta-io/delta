package io.delta.kernel.expressions.internal.skipping

import io.delta.kernel.expressions.{And, Column, Literal, Predicate}
import io.delta.kernel.internal.skipping.DataSkippingPredicate
import io.delta.kernel.internal.skipping.DataSkippingUtils.constructDataSkippingFilter
import io.delta.kernel.types.{StringType, StructType}
import jdk.nashorn.internal.runtime.PrototypeObject.getConstructor
import org.scalatest.funsuite.AnyFunSuite

import java.util.Optional

class DataSkippingUtilsSuite extends AnyFunSuite {
  val MIN = "minValues"
  val MAX = "maxValues"
  val constructor = DataSkippingPredicate.class.getConstructor()

  test("constructDataSkippingFilter") {
    Seq(
      // (schema, predicate, expectedDataSkippingPredicate)
      (
        new StructType().add("a", StringType.STRING),
        new Predicate("<", new Column("a"), new Column("b")),
        Optional.empty
      ),
      (
        new StructType().add("a", StringType.STRING),
        new Predicate("<", new Column("a"), Literal.ofString("a")),
        Optional.of(
          new DataSkippingPredicate("<", new Column(Array(MIN, "a")), Literal.ofString("a")))
      )
    ).foreach {
      case (schema, predicate, expectedDataSkippingPredicate) =>
        val actualDataSkippingPredicate = constructDataSkippingFilter(predicate, schema)
        assert(actualDataSkippingPredicate.isPresent == expectedDataSkippingPredicate.isPresent)
        if (actualDataSkippingPredicate.isPresent) {
          assert(actualDataSkippingPredicate.get == expectedDataSkippingPredicate.get)
        }
    }
  }
}
