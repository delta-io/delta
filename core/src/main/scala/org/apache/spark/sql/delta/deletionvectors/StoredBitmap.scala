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

package org.apache.spark.sql.delta.deletionvectors

import org.apache.spark.sql.delta.actions.DeletionVectorDescriptor
import org.apache.spark.sql.delta.storage.dv.DeletionVectorStore
import org.apache.spark.sql.delta.util.JsonUtils
import org.apache.hadoop.fs.Path


/**
 * Bitmap for a Deletion Vector, implemented as a thin wrapper around a Deletion Vector
 * Descriptor. The bitmap can be empty, inline or on-disk. In case of on-disk deletion
 * vectors, `tableDataPath` must be set to the data path of the Delta table, which is where
 * deletion vectors are stored.
 */
case class StoredBitmap(
    dvDescriptor: DeletionVectorDescriptor,
    tableDataPath: Option[Path] = None) {
  require(tableDataPath.isDefined || !dvDescriptor.isOnDisk,
    "Table path is required for on-disk deletion vectors")

  /**
   * Load this bitmap into memory.
   *
   * Use `dvStore` if this variant is in cloud storage, otherwise just deserialize.
   */
  def load(dvStore: DeletionVectorStore): RoaringBitmapArray = {
    if (isEmpty) {
      new RoaringBitmapArray()
    } else if (isInline) {
      RoaringBitmapArray.readFrom(dvDescriptor.inlineData)
    } else {
      assert(isOnDisk)
      dvStore.read(onDiskPath.get, dvDescriptor.offset.getOrElse(0), dvDescriptor.sizeInBytes)
    }
  }

  /**
   * The serialized size of the stored bitmap in bytes.
   *
   * Can be used for planning memory management without a round-trip to cloud storage.
   */
  def size: Int = dvDescriptor.sizeInBytes

  /**
   * Number of entries in the bitmap.
   */
  def cardinality: Long = dvDescriptor.cardinality

  /** Returns a unique identifier for this bitmap (Deletion Vector serialized as a JSON object. */
  def getUniqueId(): String = JsonUtils.toJson(dvDescriptor)

  private def isEmpty: Boolean = dvDescriptor.isEmpty

  private def isInline: Boolean = dvDescriptor.isInline

  private def isOnDisk: Boolean = dvDescriptor.isOnDisk

  /** The absolute path for on-disk deletion vectors. */
  private lazy val onDiskPath: Option[Path] = tableDataPath.map(dvDescriptor.absolutePath(_))
}

object StoredBitmap {
  /** The stored bitmap of an empty deletion vector. */
  final val EMPTY = new StoredBitmap(DeletionVectorDescriptor.EMPTY, None)


  /** Factory for inline deletion vectors. */
  def inline(dvDescriptor: DeletionVectorDescriptor): StoredBitmap = {
    require(dvDescriptor.isInline)
    new StoredBitmap(dvDescriptor, None)
  }

  /** Factory for deletion vectors. */
  def create(dvDescriptor: DeletionVectorDescriptor, tablePath: Path): StoredBitmap = {
    if (dvDescriptor.isOnDisk) {
      new StoredBitmap(dvDescriptor, Some(tablePath))
    } else {
      new StoredBitmap(dvDescriptor, None)
    }
  }
}
