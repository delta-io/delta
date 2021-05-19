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

package io.delta.exceptions

/** The basic class for all Delta commit conflict exceptions. */
abstract class DeltaConcurrentModificationException(message: String)
  extends org.apache.spark.sql.delta.DeltaConcurrentModificationException(message)

/**
 * Thrown when a concurrent transaction has written data after the current transaction read the
 * table.
 */
class ConcurrentWriteException(message: String)
  extends org.apache.spark.sql.delta.ConcurrentWriteException(message)

/**
 * Thrown when the metadata of the Delta table has changed between the time of read
 * and the time of commit.
 */
class MetadataChangedException(message: String)
  extends org.apache.spark.sql.delta.MetadataChangedException(message)

/**
 * Thrown when the protocol version has changed between the time of read
 * and the time of commit.
 */
class ProtocolChangedException(message: String)
  extends org.apache.spark.sql.delta.ProtocolChangedException(message)

/** Thrown when files are added that would have been read by the current transaction. */
class ConcurrentAppendException(message: String)
  extends org.apache.spark.sql.delta.ConcurrentAppendException(message)

/** Thrown when the current transaction reads data that was deleted by a concurrent transaction. */
class ConcurrentDeleteReadException(message: String)
  extends org.apache.spark.sql.delta.ConcurrentDeleteReadException(message)

/**
 * Thrown when the current transaction deletes data that was deleted by a concurrent transaction.
 */
class ConcurrentDeleteDeleteException(message: String)
  extends org.apache.spark.sql.delta.ConcurrentDeleteDeleteException(message)

/** Thrown when concurrent transaction both attempt to update the same idempotent transaction. */
class ConcurrentTransactionException(message: String)
  extends org.apache.spark.sql.delta.ConcurrentTransactionException(message)
