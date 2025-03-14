package io.delta.kernel.expressions.internal.skipping

import io.delta.kernel.expressions.{And, Column, Expression, Literal, Predicate}
import io.delta.kernel.internal.skipping.{CollatedDataSkippingPredicate, DataSkippingPredicate}
import io.delta.kernel.internal.skipping.DataSkippingUtils.constructDataSkippingFilter
import io.delta.kernel.types.{CollationIdentifier, StringType, StructType}
import org.scalatest.funsuite.AnyFunSuite

import java.util
import java.util.Optional
import scala.collection.JavaConverters.{mapAsJavaMapConverter, seqAsJavaListConverter, setAsJavaSetConverter}

class DataSkippingUtilsSuite extends AnyFunSuite {
  val MIN = "minValues"
  val MAX = "maxValues"
  val dataSkippingPredicateConstructor =
    classOf[DataSkippingPredicate].getDeclaredConstructor(
      classOf[String],
      classOf[util.List[Expression]],
      classOf[util.Set[Column]],
      classOf[util.Map[CollationIdentifier, util.Set[Column]]])
  val collatedDataSkippingPredicateConstructor =
    classOf[CollatedDataSkippingPredicate].getDeclaredConstructor(
      classOf[String],
      classOf[util.List[Expression]],
      classOf[CollationIdentifier],
      classOf[util.Set[Column]],
      classOf[util.Map[CollationIdentifier, util.Set[Column]]])
  dataSkippingPredicateConstructor.setAccessible(true)
  collatedDataSkippingPredicateConstructor.setAccessible(true)

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
          dataSkippingPredicateConstructor.newInstance(
            "<",
            List[Expression](new Column(Array(MIN, "a")), Literal.ofString("a")).asJava,
            Set[Column](new Column("a")).asJava,
            Map.empty[CollationIdentifier, java.util.Set[Column]].asJava))
      )
    ).foreach {
      case (schema, predicate, expectedDataSkippingPredicate) =>
        val actualDataSkippingPredicate = constructDataSkippingFilter(predicate, schema)
        assert(actualDataSkippingPredicate.isPresent == expectedDataSkippingPredicate.isPresent)
        if (actualDataSkippingPredicate.isPresent) {
          assert(actualDataSkippingPredicate.get.toString
            == expectedDataSkippingPredicate.get.toString)
        }
    }
  }
}
