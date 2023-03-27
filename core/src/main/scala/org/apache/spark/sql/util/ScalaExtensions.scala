/*
 * Copyright (2021) The Delta Lake Project Authors.
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

package org.apache.spark.sql.util

/** Extension utility classes for built-in Scala functionality. */
object ScalaExtensions {

  implicit class OptionExt[T](opt: Option[T]) {
    /**
     * Execute `f` on the content of `opt`, if `opt.isDefined`.
     *
     * This is basically a rename of `opt.foreach`, but with better readability.
     */
    def ifDefined(f: T => Unit): Unit = opt.foreach(f)
  }

  implicit class OptionExtCompanion(opt: Option.type) {
    /**
     * When a given condition is true, evaluates the a argument and returns Some(a).
     * When the condition is false, a is not evaluated and None is returned.
     */
    def when[A](cond: Boolean)(a: => A): Option[A] = if (cond) Some(a) else None

    /** Sum up all the `options`, substituting `default` for each `None`. */
    def sum[N : Numeric](default: N)(options: Option[N]*): N =
      options.map(_.getOrElse(default)).sum
  }
}
