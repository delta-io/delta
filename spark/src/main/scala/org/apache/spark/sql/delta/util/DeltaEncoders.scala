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

package org.apache.spark.sql.delta.util

import scala.reflect.runtime.universe.TypeTag

import org.apache.spark.sql.delta.{DeltaHistory, DeltaHistoryManager, SerializableFileStatus, SnapshotState}
import org.apache.spark.sql.delta.actions.{AddFile, Metadata, Protocol, RemoveFile, SingleAction}
import org.apache.spark.sql.delta.commands.convert.ConvertTargetFile
import org.apache.spark.sql.delta.sources.IndexedFile

import org.apache.spark.sql.Encoder
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder

private[delta] class DeltaEncoder[T: TypeTag] {
  private lazy val _encoder = ExpressionEncoder[T]()

  def get: Encoder[T] = {
    _encoder.copy()
  }
}

/**
 * Define a few `Encoder`s to reuse in Delta in order to avoid touching Scala reflection after
 * warming up. This will be mixed into `org.apache.spark.sql.delta.implicits`. Use
 * `import org.apache.spark.sql.delta.implicits._` to use these `Encoder`s.
 */
private[delta] trait DeltaEncoders {
  private lazy val _IntEncoder = new DeltaEncoder[Int]
  implicit def intEncoder: Encoder[Int] = _IntEncoder.get

  private lazy val _longEncoder = new DeltaEncoder[Long]
  implicit def longEncoder: Encoder[Long] = _longEncoder.get

  private lazy val _stringEncoder = new DeltaEncoder[String]
  implicit def stringEncoder: Encoder[String] = _stringEncoder.get

  private lazy val _longLongEncoder = new DeltaEncoder[(Long, Long)]
  implicit def longLongEncoder: Encoder[(Long, Long)] = _longLongEncoder.get

  private lazy val _stringLongEncoder = new DeltaEncoder[(String, Long)]
  implicit def stringLongEncoder: Encoder[(String, Long)] = _stringLongEncoder.get

  private lazy val _stringStringEncoder = new DeltaEncoder[(String, String)]
  implicit def stringStringEncoder: Encoder[(String, String)] = _stringStringEncoder.get

  private lazy val _javaLongEncoder = new DeltaEncoder[java.lang.Long]
  implicit def javaLongEncoder: Encoder[java.lang.Long] = _javaLongEncoder.get

  private lazy val _singleActionEncoder = new DeltaEncoder[SingleAction]
  implicit def singleActionEncoder: Encoder[SingleAction] = _singleActionEncoder.get

  private lazy val _addFileEncoder = new DeltaEncoder[AddFile]
  implicit def addFileEncoder: Encoder[AddFile] = _addFileEncoder.get

  private lazy val _removeFileEncoder = new DeltaEncoder[RemoveFile]
  implicit def removeFileEncoder: Encoder[RemoveFile] = _removeFileEncoder.get

  private lazy val _pmfEncoder = new DeltaEncoder[(Protocol, Metadata, String)]
  implicit def pmfEncoder: Encoder[(Protocol, Metadata, String)] = _pmfEncoder.get

  private lazy val _serializableFileStatusEncoder = new DeltaEncoder[SerializableFileStatus]
  implicit def serializableFileStatusEncoder: Encoder[SerializableFileStatus] =
    _serializableFileStatusEncoder.get

  private lazy val _indexedFileEncoder = new DeltaEncoder[IndexedFile]
  implicit def indexedFileEncoder: Encoder[IndexedFile] = _indexedFileEncoder.get

  private lazy val _addFileWithIndexEncoder = new DeltaEncoder[(AddFile, Long)]
  implicit def addFileWithIndexEncoder: Encoder[(AddFile, Long)] = _addFileWithIndexEncoder.get

  private lazy val _addFileWithSourcePathEncoder = new DeltaEncoder[(AddFile, String)]
  implicit def addFileWithSourcePathEncoder: Encoder[(AddFile, String)] =
    _addFileWithSourcePathEncoder.get

  private lazy val _deltaHistoryEncoder = new DeltaEncoder[DeltaHistory]
  implicit def deltaHistoryEncoder: Encoder[DeltaHistory] = _deltaHistoryEncoder.get

  private lazy val _historyCommitEncoder = new DeltaEncoder[DeltaHistoryManager.Commit]
  implicit def historyCommitEncoder: Encoder[DeltaHistoryManager.Commit] = _historyCommitEncoder.get

  private lazy val _snapshotStateEncoder = new DeltaEncoder[SnapshotState]
  implicit def snapshotStateEncoder: Encoder[SnapshotState] = _snapshotStateEncoder.get

  private lazy val _convertTargetFileEncoder = new DeltaEncoder[ConvertTargetFile]
  implicit def convertTargetFileEncoder: Encoder[ConvertTargetFile] =
    _convertTargetFileEncoder.get
}
