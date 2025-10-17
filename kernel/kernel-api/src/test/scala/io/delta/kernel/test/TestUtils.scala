package io.delta.kernel.test

import io.delta.kernel.expressions.{Column, Literal}

import java.util.Optional

/** Utility functions for tests. */
trait TestUtils {
  def col(name: String): Column = new Column(name)

  def nestedCol(name: String): Column = {
    new Column(name.split("\\."))
  }

  def literal(value: Any): Literal = {
    value match {
      case v: String => Literal.ofString(v)
      case v: Int => Literal.ofInt(v)
      case v: Long => Literal.ofLong(v)
      case v: Float => Literal.ofFloat(v)
      case v: Double => Literal.ofDouble(v)
      case v: Boolean => Literal.ofBoolean(v)
      case _ => throw new IllegalArgumentException(s"Unsupported literal type: ${value}")
    }
  }

  protected def optionToJava[T](option: Option[T]): Optional[T] = {
    option match {
      case Some(value) => Optional.of(value)
      case None => Optional.empty()
    }
  }

  protected def optionalToScala[T](optional: Optional[T]): Option[T] = {
    if (optional.isPresent) Some(optional.get()) else None
  }
}
