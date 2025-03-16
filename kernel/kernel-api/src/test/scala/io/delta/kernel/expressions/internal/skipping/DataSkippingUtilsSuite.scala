package io.delta.kernel.expressions.internal.skipping

import io.delta.kernel.expressions.{And, CollatedPredicate, Column, Expression, Literal, Or, Predicate}
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
  val NULL_COUNT = "nullCount"
  val NUM_RECORDS = "numRecords";
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
        new StructType()
          .add("a", StringType.STRING),
        comparator("<", column("a"), column("b")),
        Optional.empty
      ),
      (
        new StructType()
          .add("a", StringType.STRING),
        comparator("<", column("a"), Literal.ofString("b")),
        Optional.of(
          dataSkippingPredicate(
            "<",
            List(column(MIN, "a"), Literal.ofString("b")),
            Set(column(MIN, "a")),
            Map.empty[CollationIdentifier, Set[Column]]))
      ),
      (
        new StructType()
          .add("a", StringType.STRING),
        comparator("<", Literal.ofString("b"), column("a")),
        Optional.of(
          dataSkippingPredicate(
            ">",
            List(column(MAX, "a"), Literal.ofString("b")),
            Set(column(MAX, "a")),
            Map.empty[CollationIdentifier, Set[Column]]))
      ),
      (
        new StructType()
          .add("a", StringType.STRING),
        comparator("<=", column("a"), Literal.ofString("b")),
        Optional.of(
          dataSkippingPredicate(
            "<=",
            List(column(MIN, "a"), Literal.ofString("b")),
            Set(column(MIN, "a")),
            Map.empty[CollationIdentifier, Set[Column]]))
      ),
      (
        new StructType()
          .add("a", StringType.STRING),
        comparator(">", column("a"), Literal.ofString("b")),
        Optional.of(
          dataSkippingPredicate(
            ">",
            List(column(MAX, "a"), Literal.ofString("b")),
            Set(column(MAX, "a")),
            Map.empty[CollationIdentifier, Set[Column]]))
      ),
      (
        new StructType()
          .add("a", StringType.STRING),
        comparator(">=", column("a"), Literal.ofString("b")),
        Optional.of(
          dataSkippingPredicate(
            ">=",
            List(column(MAX, "a"), Literal.ofString("b")),
            Set(column(MAX, "a")),
            Map.empty[CollationIdentifier, Set[Column]]))
      ),
      (
        new StructType()
          .add("a", StringType.STRING),
        comparator("=", column("a"), Literal.ofString("b")),
        Optional.of(
          dataSkippingPredicate(
            "AND",
            List(dataSkippingPredicate(
              "<=",
              List(column(MIN, "a"), Literal.ofString("b")),
              Set(column(MIN, "a")),
              Map.empty[CollationIdentifier, Set[Column]]
            ),
            dataSkippingPredicate(
              ">=",
              List(column(MAX, "a"), Literal.ofString("b")),
              Set(column(MAX, "a")),
              Map.empty[CollationIdentifier, Set[Column]]
            )),
            Set(column(MIN, "a"), column(MAX, "a")),
            Map.empty[CollationIdentifier, Set[Column]]))
      ),
      (
        new StructType()
          .add("a", StringType.STRING),
        comparator("IS NOT DISTINCT FROM", Literal.ofString("b"), column("a")),
        Optional.of(
          dataSkippingPredicate(
            "AND",
            List(dataSkippingPredicate(
              "<",
              List(column(NULL_COUNT, "a"), column(NUM_RECORDS)),
              Set(column(NULL_COUNT, "a"), column(NUM_RECORDS)),
              Map.empty[CollationIdentifier, Set[Column]]),
            dataSkippingPredicate(
              "AND",
              List(dataSkippingPredicate(
                "<=",
                List(column(MIN, "a"), Literal.ofString("b")),
                Set(column(MIN, "a")),
                Map.empty[CollationIdentifier, Set[Column]]
              ),
              dataSkippingPredicate(
                ">=",
                List(column(MAX, "a"), Literal.ofString("b")),
                Set(column(MAX, "a")),
                Map.empty[CollationIdentifier, Set[Column]]
              )),
              Set(column(MIN, "a"), column(MAX, "a")),
              Map.empty[CollationIdentifier, Set[Column]])),
            Set(column(NULL_COUNT, "a"), column(NUM_RECORDS), column(MIN, "a"), column(MAX, "a")),
            Map.empty[CollationIdentifier, Set[Column]]))
      ),
      (
        new StructType()
          .add("a", StringType.STRING)
          .add("b", StringType.STRING),
        and(
          comparator("<", column("a"), column("b")),
          comparator(">", column("a"), Literal.ofString("b"))
        ),
        Optional.of(
          dataSkippingPredicate(
            ">",
            List(column(MAX, "a"), Literal.ofString("b")),
            Set(column(MAX, "a")),
            Map.empty[CollationIdentifier, Set[Column]]))
      ),
      (
        new StructType()
          .add("a", StringType.STRING)
          .add("b", StringType.STRING),
        or(
          comparator("<", column("a"), column("b")),
          comparator(">", column("a"), Literal.ofString("b"))
        ),
        Optional.empty
      ),
      (
        new StructType()
          .add("a", StringType.STRING),
        new Predicate("IS_NULL", List[Expression](column("a")).asJava),
        Optional.of(
          dataSkippingPredicate(">", List(column(NULL_COUNT, "a"), Literal.ofInt(0)),
            Set(column(NULL_COUNT, "a")),
            Map.empty[CollationIdentifier, Set[Column]]))
      ),
      (
        new StructType()
          .add("a", StringType.STRING),
        new Predicate("IS_NOT_NULL", List[Expression](column("a")).asJava),
        Optional.of(
          dataSkippingPredicate("<", List(column(NULL_COUNT, "a"), column(NUM_RECORDS)),
            Set(column(NULL_COUNT, "a"), column(NUM_RECORDS)),
            Map.empty[CollationIdentifier, Set[Column]]))
      ),
      (
        new StructType()
          .add("a", StringType.STRING)
          .add("b", StringType.STRING),
        and(
          comparator("<", column("a"), Literal.ofString("c")),
          comparator(">", Literal.ofString("c"), column("b"))),
        Optional.of(
          dataSkippingPredicate(
            "AND",
            List(dataSkippingPredicate(
              "<",
              List(column(MIN, "a"), Literal.ofString("c")),
              Set(column(MIN, "a")),
              Map.empty[CollationIdentifier, Set[Column]]
            ),
            dataSkippingPredicate(
              "<",
              List(column(MIN, "b"), Literal.ofString("c")),
              Set(column(MIN, "b")),
              Map.empty[CollationIdentifier, Set[Column]]
            )),
            Set(column(MIN, "a"), column(MIN, "b")),
            Map.empty[CollationIdentifier, Set[Column]]))
      ),
      (
        new StructType()
          .add("a", StringType.STRING)
          .add("b", StringType.STRING),
        or(
          comparator("<", column("a"), Literal.ofString("c")),
          comparator(">", Literal.ofString("c"), column("b"))),
        Optional.of(
          dataSkippingPredicate(
            "OR",
            List(dataSkippingPredicate(
              "<",
              List(column(MIN, "a"), Literal.ofString("c")),
              Set(column(MIN, "a")),
              Map.empty[CollationIdentifier, Set[Column]]
            ),
              dataSkippingPredicate(
                "<",
                List(column(MIN, "b"), Literal.ofString("c")),
                Set(column(MIN, "b")),
                Map.empty[CollationIdentifier, Set[Column]]
              )),
            Set(column(MIN, "a"), column(MIN, "b")),
            Map.empty[CollationIdentifier, Set[Column]]))
      ),
      (
        new StructType()
          .add("a1", new StructType()
            .add("a2", StringType.STRING)),
        comparator("<", column("a1"), Literal.ofString("b")),
        Optional.empty
      )
    ).foreach {
      case (schema, predicate, expectedDataSkippingPredicate) =>
        val actualDataSkippingPredicate = constructDataSkippingFilter(predicate, schema)
        assert(actualDataSkippingPredicate.isPresent == expectedDataSkippingPredicate.isPresent)
        if (actualDataSkippingPredicate.isPresent) {
          assert(actualDataSkippingPredicate.get.toString
            == expectedDataSkippingPredicate.get.toString)
          assert(actualDataSkippingPredicate.get.getReferencedCols
            == expectedDataSkippingPredicate.get.getReferencedCols)
          assert(actualDataSkippingPredicate.get.getReferencedCollatedCols
            == expectedDataSkippingPredicate.get.getReferencedCollatedCols)
        }
    }
  }

  def and(left: Predicate, right: Predicate): And = {
    new And(left, right)
  }

  def or(left: Predicate, right: Predicate): Or = {
    new Or(left, right)
  }

  def column(name: String*): Column = {
    new Column(name.toArray)
  }

  def comparator(
                            symbol: String, left: Expression,
                            right: Expression,
                            collationIdentifier: Option[CollationIdentifier] = None): Predicate = {
    if (collationIdentifier.isDefined && collationIdentifier.get != null) {
      new CollatedPredicate(symbol, left, right, collationIdentifier.get)
    } else {
      new Predicate(symbol, left, right)
    }
  }

  def dataSkippingPredicate(
                             predicateName: String,
                             children: List[Expression],
                             columns: Set[Column],
                             collationMap: Map[CollationIdentifier, Set[Column]])
  : DataSkippingPredicate = {
    dataSkippingPredicateConstructor.newInstance(
      predicateName, children.asJava, columns.asJava, collationMap.mapValues(_.asJava).asJava)
  }
}
