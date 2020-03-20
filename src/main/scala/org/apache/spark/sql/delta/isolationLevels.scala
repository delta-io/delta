/*
 * Copyright (2020) The Delta Lake Project Authors.
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

/**
 * Trait that defines the level consistency guarantee is going to be provided by
 * `OptimisticTransaction.commit()`. [[Serializable]] is the most
 * strict level and [[SnapshotIsolation]] is the least strict one.
 *
 * @see [[IsolationLevel.allLevelsInDescOrder]] for all the levels in the descending order
 *       of strictness and [[IsolationLevel.DEFAULT]] for the default table isolation level.
 */
sealed trait IsolationLevel {
  override def toString: String = this.getClass.getSimpleName.stripSuffix("$")
}

/**
 * This isolation level will ensure serializability between all read and write operations.
 * Specifically, for write operations, this mode will ensure that the result of
 * the table will be perfectly consistent with the visible history of operations, that is,
 * as if all the operations were executed sequentially one by one.
 */
case object Serializable extends IsolationLevel

/**
 * This isolation level will ensure snapshot isolation consistency guarantee between write
 * operations only. In other words, if only the write operations are considered, then
 * there exists a serializable sequence between them that would produce the same result
 * as seen in the table. However, if both read and write operations are considered, then
 * there may not exist a serializable sequence that would explain all the observed reads.
 *
 * This provides a lower consistency guarantee than [[Serializable]] but a higher
 * availability than that. For example, unlike [[Serializable]], this level allows an UPDATE
 * operation to be committed even if there was a concurrent INSERT operation that has already
 * added data that should have been read by the UPDATE. It will be as if the UPDATE was executed
 * before the INSERT even if the former was committed after the latter. As a side effect,
 * the visible history of operations may not be consistent with the
 * result expected if these operations were executed sequentially one by one.
 */
case object WriteSerializable extends IsolationLevel

/**
 * This isolation level will ensure that all reads will see a consistent
 * snapshot of the table and any transactional write will successfully commit only
 * if the values updated by the transaction have not been changed externally since
 * the snapshot was read by the transaction.
 *
 * This provides a lower consistency guarantee than [[WriteSerializable]] but a higher
 * availability than that. For example, unlike [[WriteSerializable]], this level allows two
 * concurrent UPDATE operations reading the same data to be committed successfully as long as
 * they don't modify the same data.
 *
 * Note that for operations that do not modify data in the table, Snapshot isolation is same
 * as Serializablity. Hence such operations can be safely committed with Snapshot isolation level.
 */
case object SnapshotIsolation extends IsolationLevel


object IsolationLevel {

  val DEFAULT = WriteSerializable

  /** All possible isolation levels in descending order of guarantees provided */
  val allLevelsInDescOrder: Seq[IsolationLevel] = Seq(
    Serializable,
    WriteSerializable,
    SnapshotIsolation)

  /** All the valid isolation levels that can be specified as the table isolation level */
  val validTableIsolationLevels = Set[IsolationLevel](Serializable, WriteSerializable)

  def fromString(s: String): IsolationLevel = {
    allLevelsInDescOrder.find(_.toString.equalsIgnoreCase(s)).getOrElse {
      throw new IllegalArgumentException(s"invalid isolation level '$s'")
    }
  }
}
