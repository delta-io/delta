package io.delta.kernel.defaults.ccv2.setup

import java.util.Optional

object JavaScalaUtils {
  implicit class ScalaOptionOps[T](val scalaOption: Option[T]) {
    def asJava(implicit ev: Null <:< T): Optional[T] = Optional.ofNullable(scalaOption.orNull)
  }

  implicit class JavaOptionalOps[T](val javaOptional: Optional[T]) {
    def asScala: Option[T] = if (javaOptional.isPresent) Some(javaOptional.get()) else None
  }
}
