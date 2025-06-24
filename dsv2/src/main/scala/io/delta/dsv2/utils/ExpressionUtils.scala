package io.delta.dsv2.utils

import io.delta.kernel.expressions.{And => KernelAnd, Column => KernelColumn, Expression => KernelExpression, Literal => KernelLiteral, Or => KernelOr, Predicate => KernelPredicate}
import io.delta.kernel.{types => kerneltypes}
import org.apache.spark.sql.connector.expressions.{Expressions, Expression => SparkExpression, Literal => SparkLiteral, NamedReference => SparkNamedReference}
import org.apache.spark.sql.connector.expressions.filter.{And => SparkAnd, Or => SparkOr, Predicate => SparkPredicate}
import org.apache.spark.sql.sources.{EqualTo => SparkDSv1EqualTo, Filter => SparkDSv1Filter, GreaterThan => SparkDSv1GreaterThan, GreaterThanOrEqual => SparkDSv1GreaterThanOrEqual, LessThan => SparkDSv1LessThan, LessThanOrEqual => SparkDSv1LessThanOrEqual, And => SparkDSv1And, Or => SparkDSv1Or, Not => SparkDSv1Not}
import org.apache.spark.sql.{types => sparktypes}

object ExpressionUtils {
  private val logger = org.slf4j.LoggerFactory.getLogger(this.getClass)

  ///////////////////////////
  // Kernel --> Spark DSv1 //
  ///////////////////////////

  def convertKtoSFilter(kernelPredicate: KernelPredicate): SparkDSv1Filter = {
    logger.info(s"Converting KernelPredicate to Spark DSv1 Filter: $kernelPredicate")

    val binaryOpMap: Map[String, (String, Any) => SparkDSv1Filter] = Map(
      "=" -> SparkDSv1EqualTo,
      ">" -> SparkDSv1GreaterThan,
      ">=" -> SparkDSv1GreaterThanOrEqual,
      "<" -> SparkDSv1LessThan,
      "<=" -> SparkDSv1LessThanOrEqual)

    val result = kernelPredicate.getName match {
      case op if binaryOpMap.contains(op) =>
        val left = kernelPredicate.getChildren.get(0)
        val right = kernelPredicate.getChildren.get(1)

        (convertKtoSExpr(left), convertKtoSExpr(right)) match {
          case (Some(col: SparkNamedReference), Some(literal: SparkLiteral[_])) =>
            binaryOpMap(op)(col.fieldNames.mkString("."), literal.value)
          case (Some(literal: SparkLiteral[_]), Some(col: SparkNamedReference)) =>
            binaryOpMap(op)(col.fieldNames.mkString("."), literal.value)
          case _ =>
            throw new IllegalArgumentException(
              s"Unsupported predicate structure: $kernelPredicate")
        }

      case "AND" =>
        val left = kernelPredicate.getChildren.get(0)
        val right = kernelPredicate.getChildren.get(1)

        SparkDSv1And(
          convertKtoSFilter(left.asInstanceOf[KernelPredicate]),
          convertKtoSFilter(right.asInstanceOf[KernelPredicate]))

      case "OR" =>
        val left = kernelPredicate.getChildren.get(0)
        val right = kernelPredicate.getChildren.get(1)

        SparkDSv1Or(
          convertKtoSFilter(left.asInstanceOf[KernelPredicate]),
          convertKtoSFilter(right.asInstanceOf[KernelPredicate]))

      case "NOT" =>
        val child = kernelPredicate.getChildren.get(0)

        SparkDSv1Not(convertKtoSFilter(child.asInstanceOf[KernelPredicate]))

      case _ =>
        throw new IllegalArgumentException(
          s"Unsupported predicate type: ${kernelPredicate.getName}")
    }

    logger.info(s"Converted KernelPredicate to Spark DSv1 Filter: $kernelPredicate -> $result")

    result
  }

  ///////////////////////////
  // Kernel --> Spark DSv2 //
  ///////////////////////////

  def convertKtoSPredicate(kernelPredicate: KernelPredicate): Option[SparkPredicate] = {
    val result = convertKtoSExpr(kernelPredicate) match {
      case Some(pred: SparkPredicate) => Some(pred)
      case Some(expr) =>
        throw new IllegalArgumentException(
          s"Expected a SparkPredicate but got ${expr.getClass.getSimpleName}")
      case _ => None
    }

    logger.info(s"convertKtoSPredicate input=$kernelPredicate, result=$result")

    result
  }

  private def convertKtoSExpr(kernelExpression: KernelExpression): Option[SparkExpression] = {
    val result = kernelExpression match {
      case expr: KernelAnd =>
        for {
          left <- convertKtoSPredicate(expr.getLeft)
          right <- convertKtoSPredicate(expr.getRight)
        } yield new SparkAnd(left, right)

      case expr: KernelOr =>
        for {
          left <- convertKtoSPredicate(expr.getLeft)
          right <- convertKtoSPredicate(expr.getRight)
        } yield new SparkOr(left, right)

      case expr: KernelPredicate if KernelPredicate.BINARY_OPERATORS.contains(expr.getName) =>
        for {
          left <- convertKtoSExpr(expr.getChildren.get(0))
          right <- convertKtoSExpr(expr.getChildren.get(1))
        } yield new SparkPredicate(expr.getName, Array(left, right))

      case expr: KernelPredicate if KernelPredicate.UNARY_OPERATORS.contains(expr.getName) =>
        for {
          child <- convertKtoSExpr(expr.getChildren.get(0))
        } yield new SparkPredicate(expr.getName, Array(child))

      case expr: KernelColumn => Some(Expressions.column(expr.getNames.mkString(".")))

      case literal: KernelLiteral =>
        literal.getDataType match {
          case _: kerneltypes.BooleanType =>
            Some(Expressions.literal(literal.getValue.asInstanceOf[Boolean]))
          case _: kerneltypes.IntegerType =>
            Some(Expressions.literal(literal.getValue.asInstanceOf[Integer]))
          case _: kerneltypes.LongType =>
            Some(Expressions.literal(literal.getValue.asInstanceOf[Long]))
          case _: kerneltypes.StringType =>
            Some(Expressions.literal(literal.getValue.asInstanceOf[String]))
          case _ =>
            throw new UnsupportedOperationException(
              s"Unsupported KernelLiteral type: ${literal.getDataType}")
        }

      case x =>
        logger.warn(s"Unsupported KernelExpression: $x")
        throw new UnsupportedOperationException(s"Unsupported KernelExpression: $x")
    }

    logger.info(s"convertKtoSExpr: input=$kernelExpression, result=$result")

    result
  }

  //////////////////////
  // Spark --> Kernel //
  //////////////////////

  // TODO: perhaps this should NOT return an Option?

  def convertStoKPredicate(sparkPredicate: SparkPredicate): Option[KernelPredicate] = {
    convertStoKExpr(sparkPredicate) match {
      case Some(pred: KernelPredicate) => Some(pred)
      case Some(expr) =>
        throw new IllegalArgumentException(
          s"Expected a KernelPredicate but got ${expr.getClass.getSimpleName}")
      case _ => None
    }
  }

  private def convertStoKExpr(sparkExpression: SparkExpression): Option[KernelExpression] = {
    sparkExpression match {
      case expr: SparkAnd =>
        for {
          left <- convertStoKPredicate(expr.left())
          right <- convertStoKPredicate(expr.right())
        } yield new KernelAnd(left, right)

      case expr: SparkOr =>
        for {
          left <- convertStoKPredicate(expr.left())
          right <- convertStoKPredicate(expr.right())
        } yield new KernelOr(left, right)

      case expr: SparkPredicate if KernelPredicate.BINARY_OPERATORS.contains(expr.name()) =>
        for {
          left <- convertStoKExpr(expr.children()(0))
          right <- convertStoKExpr(expr.children()(1))
        } yield new KernelPredicate(expr.name(), left, right)

      case expr: SparkPredicate if KernelPredicate.UNARY_OPERATORS.contains(expr.name()) =>
        for {
          child <- convertStoKExpr(expr.children()(0))
        } yield new KernelPredicate(expr.name(), child)

      case c: SparkNamedReference =>
        Some(new KernelColumn(c.fieldNames))

      case l: SparkLiteral[Boolean] if l.dataType.isInstanceOf[sparktypes.BooleanType] =>
        Some(KernelLiteral.ofBoolean(l.value))

      case l: SparkLiteral[Int] if l.dataType.isInstanceOf[sparktypes.IntegerType] =>
        Some(KernelLiteral.ofInt(l.value))

      case l: SparkLiteral[Long] if l.dataType.isInstanceOf[sparktypes.LongType] =>
        Some(KernelLiteral.ofLong(l.value))

      case l: SparkLiteral[String] if l.dataType.isInstanceOf[sparktypes.StringType] =>
        Some(KernelLiteral.ofString(l.value))

      case _ => None
    }
  }

}