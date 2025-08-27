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
  def parseSqlPredicate(sqlExpr: String): KernelPredicate = {
    // Parse SQL using Spark's CatalystSqlParser
    val parser = new CatalystSqlParser()
    val sparkExpr = parser.parseExpression(sqlExpr)
    println("Spark expression: " + sparkExpr)

    // Convert Spark expression to Kernel predicate
    convertStoKPredicate(sparkExpr)
  }

  //////////////////////
  // Spark --> Kernel //
  //////////////////////

  // TODO: perhaps this should NOT return an Option?

  def convertStoKPredicate(sparkPredicate: SparkExpression): KernelPredicate = {
    convertStoKExpr(sparkPredicate) match {
      case pred: KernelPredicate => pred
      case expr =>
        throw new IllegalArgumentException(
          s"Expected a KernelPredicate but got ${expr.getClass.getSimpleName}")
    }
  }

  def convertStoKExprJava(sparkExpression: SparkExpression): KernelExpression = {
    convertStoKExpr(sparkExpression)
  }

  private val BINARY_OPERATORS = Set("<", "<=", ">", ">=", "=", "AND", "OR")

  private def convertStoKExpr(sparkExpression: SparkExpression): KernelExpression = {
    sparkExpression match {
      case expr: SparkAnd =>
        val left = convertStoKPredicate(expr.left)
        val right = convertStoKPredicate(expr.right)
        new KernelAnd(left, right)

      case expr: SparkOr =>
        val left = convertStoKPredicate(expr.left)
        val right = convertStoKPredicate(expr.right)
        new KernelOr(left, right)

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
        val left = convertStoKExpr(expr.left)
        val right = convertStoKExpr(expr.right)
        new KernelPredicate(name, left, right)

      case c: SparkNamedReference =>
        new KernelColumn(c.name)

      case l: SparkLiteral if l.dataType.isInstanceOf[sparktypes.BooleanType] =>
        KernelLiteral.ofBoolean(l.value.asInstanceOf[Boolean])

      case l: SparkLiteral if l.dataType.isInstanceOf[sparktypes.IntegerType] =>
        KernelLiteral.ofInt(l.value.asInstanceOf[Int])

      case l: SparkLiteral if l.dataType.isInstanceOf[sparktypes.LongType] =>
        KernelLiteral.ofLong(l.value.asInstanceOf[Long])

      case l: SparkLiteral if l.dataType.isInstanceOf[sparktypes.StringType] =>
        KernelLiteral.ofString(l.value.toString)

      case l: SparkIsNull =>
        val child = convertStoKExpr(l.child)
        new KernelPredicate("IS NULL", child)

      case l: SparkEqualTo =>
        val left = convertStoKExpr(l.left)
        val right = convertStoKExpr(l.right)
        new KernelPredicate("=", left, right)
      case x =>
        throw new RuntimeException(
          "Unsupported Spark expression: " + x + " with type: " + x.getClass)
    }
  }

}
