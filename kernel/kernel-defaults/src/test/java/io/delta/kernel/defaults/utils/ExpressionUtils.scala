package io.delta.kernel.defaults.utils

import java.util.Optional

import io.delta.kernel.{types => kerneltypes}
import io.delta.kernel.expressions.{And => KernelAnd, Column => KernelColumn, Expression => KernelExpression, Literal => KernelLiteral, Or => KernelOr, Predicate => KernelPredicate}

import org.apache.spark.sql.{types => sparktypes}
import org.apache.spark.sql.catalyst.expressions.{Add => SparkAdd, And => SparkAnd, Attribute => SparkNamedReference, BinaryExpression => SparkBinaryExpression, Divide => SparkDivide, EqualTo => SparkEqualTo, Expression => SparkExpression, IsNull => SparkIsNull, Literal => SparkLiteral, Multiply => SparkMultiply, Or => SparkOr, Predicate => SparkPredicate, Remainder => SparkRemainder, Subtract => SparkSubtract}
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.unsafe.types.UTF8String

//noinspection ScalaStyle
object ExpressionUtils {
  private val logger = org.slf4j.LoggerFactory.getLogger(this.getClass)

  ///////////////////////////
  // Kernel --> Spark DSv1 //
  ///////////////////////////

  /**
   * Parses a SQL expression string and converts it to a Delta Kernel predicate.
   *
   * @param sqlExpr SQL expression string (e.g., "id > 1000", "name = 'test'")
   * @return Delta Kernel predicate
   * @throws IllegalArgumentException if the SQL cannot be parsed or converted
   */
  def parseSqlPredicate(sqlExpr: String): Optional[KernelExpression] = {
    // Parse SQL using Spark's CatalystSqlParser
    val parser = new CatalystSqlParser()
    val sparkExpr = parser.parseExpression(sqlExpr)
    println("Spark expression: " + sparkExpr)

    // Convert Spark expression to Kernel predicate
    return convertStoKExprJava(sparkExpr)
  }

  //////////////////////
  // Spark --> Kernel //
  //////////////////////

  // TODO: perhaps this should NOT return an Option?

  def convertStoKPredicate(sparkPredicate: SparkExpression): Option[KernelPredicate] = {
    convertStoKExpr(sparkPredicate) match {
      case Some(pred: KernelPredicate) => Some(pred)
      case Some(expr) =>
        throw new IllegalArgumentException(
          s"Expected a KernelPredicate but got ${expr.getClass.getSimpleName}")
      case _ => None
    }
  }

  def convertStoKExprJava(sparkExpression: SparkExpression): Optional[KernelExpression] = {
    convertStoKExpr(sparkExpression) match {
      case Some(expr) => Optional.of(expr)
      case None => Optional.empty()
    }
  }

  private val BINARY_OPERATORS = Set("<", "<=", ">", ">=", "=", "AND", "OR")

  private def convertStoKExpr(sparkExpression: SparkExpression): Option[KernelExpression] = {
    sparkExpression match {
      case expr: SparkAnd =>
        for {
          left <- convertStoKPredicate(expr.left)
          right <- convertStoKPredicate(expr.right)
        } yield new KernelAnd(left, right)

      case expr: SparkOr =>
        for {
          left <- convertStoKPredicate(expr.left)
          right <- convertStoKPredicate(expr.right)
        } yield new KernelOr(left, right)

      case expr: SparkBinaryExpression =>
        val name = expr match {
          case _: SparkAdd => "+"
          case _: SparkSubtract => "-"
          case _: SparkMultiply => "*"
          case _: SparkDivide => "/"
          case _: SparkRemainder => "%"
          case _: SparkEqualTo => "="
          case _ =>
            throw new IllegalArgumentException(
              s"Unsupported binary expression: ${expr.getClass.getSimpleName}")
        }
        for {
          left <- convertStoKExpr(expr.left)
          right <- convertStoKExpr(expr.right)
        } yield new KernelPredicate(name, left, right)

      case c: SparkNamedReference =>
        Some(new KernelColumn(c.name))

      case l: SparkLiteral if l.dataType.isInstanceOf[sparktypes.BooleanType] =>
        Some(KernelLiteral.ofBoolean(l.value.asInstanceOf[Boolean]))

      case l: SparkLiteral if l.dataType.isInstanceOf[sparktypes.IntegerType] =>
        Some(KernelLiteral.ofInt(l.value.asInstanceOf[Int]))

      case l: SparkLiteral if l.dataType.isInstanceOf[sparktypes.LongType] =>
        Some(KernelLiteral.ofLong(l.value.asInstanceOf[Long]))

      case l: SparkLiteral if l.dataType.isInstanceOf[sparktypes.StringType] =>
        Some(KernelLiteral.ofString(l.value.toString))

      case l: SparkIsNull =>
        for {
          child <- convertStoKExpr(l.child)
        } yield new KernelPredicate("IS NULL", child)

      case l: SparkEqualTo =>
        for {
          left <- convertStoKExpr(l.left)
          right <- convertStoKExpr(l.right)
        } yield new KernelPredicate("=", left, right)
      case x =>
        println("Unsupported Spark expression: " + x + " with type: " + x.getClass)
        None
    }
  }

}
