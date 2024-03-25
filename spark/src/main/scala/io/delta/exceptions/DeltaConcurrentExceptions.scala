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

package io.delta.exceptions

import org.apache.spark.sql.delta.{DeltaThrowable, DeltaThrowableHelper}

import org.apache.spark.annotation.Evolving

/**
 * :: Evolving ::
 *
 * The basic class for all Delta commit conflict exceptions.
 *
 * @since 1.0.0
 */
@Evolving
abstract class DeltaConcurrentModificationException(message: String)
  extends org.apache.spark.sql.delta.DeltaConcurrentModificationException(message)

/**
 * :: Evolving ::
 *
 * Thrown when a concurrent transaction has written data after the current transaction read the
 * table.
 *
 * @since 1.0.0
 */
@Evolving
class ConcurrentWriteException(message: String)
  extends org.apache.spark.sql.delta.ConcurrentWriteException(message)
    with DeltaThrowable {
  def this(messageParameters: Array[String]) = {
    this(DeltaThrowableHelper.getMessage("DELTA_CONCURRENT_WRITE", messageParameters))
  }
  override def getErrorClass: String = "DELTA_CONCURRENT_WRITE"
  override def getMessage: String = message
}

/**
 * :: Evolving ::
 *
 * Thrown when the metadata of the Delta table has changed between the time of read
 * and the time of commit.
 *
 * @since 1.0.0
 */
@Evolving
class MetadataChangedException(message: String)
  extends org.apache.spark.sql.delta.MetadataChangedException(message)
    with DeltaThrowable {
  def this(messageParameters: Array[String]) = {
    this(DeltaThrowableHelper.getMessage("DELTA_METADATA_CHANGED", messageParameters))
  }
  override def getErrorClass: String = "DELTA_METADATA_CHANGED"
  override def getMessage: String = message
}

/**
 * :: Evolving ::
 *
 * Thrown when the protocol version has changed between the time of read
 * and the time of commit.
 *
 * @since 1.0.0
 */
@Evolving
class ProtocolChangedException(message: String)
  extends org.apache.spark.sql.delta.ProtocolChangedException(message)
    with DeltaThrowable {
  def this(messageParameters: Array[String]) = {
    this(DeltaThrowableHelper.getMessage("DELTA_PROTOCOL_CHANGED", messageParameters))
  }
  override def getErrorClass: String = "DELTA_PROTOCOL_CHANGED"
  override def getMessage: String = message
}

/**
 * :: Evolving ::
 *
 * Thrown when files are added that would have been read by the current transaction.
 *
 * @since 1.0.0
 */
@Evolving
class ConcurrentAppendException(message: String)
  extends org.apache.spark.sql.delta.ConcurrentAppendException(message)
    with DeltaThrowable {
  def this(messageParameters: Array[String]) = {
    this(DeltaThrowableHelper.getMessage("DELTA_CONCURRENT_APPEND", messageParameters))
  }
  override def getErrorClass: String = "DELTA_CONCURRENT_APPEND"
  override def getMessage: String = message
}

/**
 * :: Evolving ::
 *
 * Thrown when the current transaction reads data that was deleted by a concurrent transaction.
 *
 * @since 1.0.0
 */
@Evolving
class ConcurrentDeleteReadException(message: String)
  extends org.apache.spark.sql.delta.ConcurrentDeleteReadException(message)
    with DeltaThrowable {
  def this(messageParameters: Array[String]) = {
    this(DeltaThrowableHelper.getMessage("DELTA_CONCURRENT_DELETE_READ", messageParameters))
  }
  override def getErrorClass: String = "DELTA_CONCURRENT_DELETE_READ"
  override def getMessage: String = message
}

/**
 * :: Evolving ::
 *
 * Thrown when the current transaction deletes data that was deleted by a concurrent transaction.
 *
 * @since 1.0.0
 */
@Evolving
class ConcurrentDeleteDeleteException(message: String)
  extends org.apache.spark.sql.delta.ConcurrentDeleteDeleteException(message)
    with DeltaThrowable {
  def this(messageParameters: Array[String]) = {
    this(DeltaThrowableHelper.getMessage("DELTA_CONCURRENT_DELETE_DELETE", messageParameters))
  }
  override def getErrorClass: String = "DELTA_CONCURRENT_DELETE_DELETE"
  override def getMessage: String = message
}

/**
 * :: Evolving ::
 *
 * Thrown when concurrent transaction both attempt to update the same idempotent transaction.
 *
 * @since 1.0.0
 */
@Evolving
class ConcurrentTransactionException(message: String)
  extends org.apache.spark.sql.delta.ConcurrentTransactionException(message)
    with DeltaThrowable {
  def this(messageParameters: Array[String]) = {
    this(DeltaThrowableHelper.getMessage("DELTA_CONCURRENT_TRANSACTION", messageParameters))
  }
  override def getErrorClass: String = "DELTA_CONCURRENT_TRANSACTION"
  override def getMessage: String = message
}
