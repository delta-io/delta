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

package org.apache.spark.sql.delta

/** Marker trait for a commit tag used by delta. */
sealed trait DeltaCommitTag {

  /** Key to be used in the commit tags `Map[String, String]`. */
  def key: String

  /**
   * Combine tags coming from multiple sub-jobs into a single tag according to the tags'
   * semantics.
   */
  def merge(left: String, right: String): String
}

object DeltaCommitTag {

  trait TypedCommitTag[ValueT] extends DeltaCommitTag {

    /**
     * Combine tags coming from multiple sub-jobs into a single tag according to the tags'
     * semantics.
     */
    def mergeTyped(left: ValueT, right: ValueT): ValueT

    override def merge(left: String, right: String): String =
      valueToString(mergeTyped(valueFromString(left), valueFromString(right)))

    /**
     * Combine tags coming from multiple sub-jobs into a single tag according to the tags'
     * semantics.
     *
     * This variant is used when adding a new typed value to a potentially existing value from a
     * `Map[<tagtype>, String]`.
     */
    def mergeWithNewTypedValue(existingOpt: Option[String], newValue: ValueT): String = {
      existingOpt match {
        case Some(existing) => valueToString(mergeTyped(valueFromString(existing), newValue))
        case None => valueToString(newValue)
      }
    }

    /** Deserialize a value for this tag from String. */
    def valueFromString(s: String): ValueT

    /** Serialize a value for this tag to String. */
    def valueToString(value: ValueT): String = value.toString

    def withValue(value: ValueT): TypedCommitTagPair[ValueT] = TypedCommitTagPair(this, value)
  }

  final case class TypedCommitTagPair[ValueT](tag: TypedCommitTag[ValueT], value: ValueT) {
    /** Produce a tuple for inserting into `Map[DeltaCommitTag, String]` instances. */
    def stringValue: (DeltaCommitTag, String) = tag -> tag.valueToString(value)

    /** Produce a tuple for inserting into `Map[String, String]` instances. */
    def stringPair: (String, String) = tag.key -> tag.valueToString(value)
  }

  /** Any [[DeltaCommitTag]] where `ValueT` is `Boolean`. */
  trait BooleanCommitTag extends TypedCommitTag[Boolean] {
    override def valueFromString(value: String): Boolean = value.toBoolean
  }

  /**
   * Tag to indicate whether the operation preserved row tracking. If not set, it is assumed that
   * the operation did not preserve row tracking.
   */
  case object PreservedRowTrackingTag extends BooleanCommitTag {
    override val key = "delta.rowTracking.preserved"

    override def mergeTyped(left: Boolean, right: Boolean): Boolean = left && right
  }
}
