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

package io.delta.sharing.spark.model

import java.net.URLEncoder

import org.apache.spark.sql.delta.actions.{
  AddCDCFile,
  AddFile,
  DeletionVectorDescriptor,
  FileAction,
  Metadata,
  Protocol,
  RemoveFile,
  SingleAction
}
import org.apache.spark.sql.delta.storage.dv.DeletionVectorStore
import org.apache.spark.sql.delta.util.JsonUtils
import com.fasterxml.jackson.annotation._
import com.fasterxml.jackson.annotation.JsonInclude.Include
import io.delta.sharing.client.DeltaSharingFileSystem

import org.apache.spark.sql.types.{DataType, StructType}

// Represents a single action in the response of a Delta Sharing rpc.
sealed trait DeltaSharingAction {
  def wrap: DeltaSharingSingleAction
  def json: String = JsonUtils.toJson(wrap)
}

/** A serialization helper to create a common action envelope, for delta sharing actions in the
 * response of a rpc.
 */
case class DeltaSharingSingleAction(
    protocol: DeltaSharingProtocol = null,
    metaData: DeltaSharingMetadata = null,
    file: DeltaSharingFileAction = null) {
  def unwrap: DeltaSharingAction = {
    if (file != null) {
      file
    } else if (metaData != null) {
      metaData
    } else if (protocol != null) {
      protocol
    } else {
      null
    }
  }
}

/**
 * The delta sharing protocol from the response of a rpc. It only wraps a delta protocol now, but
 * can be extended with additional delta sharing fields if needed later.
 */
case class DeltaSharingProtocol(deltaProtocol: Protocol) extends DeltaSharingAction {

  override def wrap: DeltaSharingSingleAction = DeltaSharingSingleAction(protocol = this)
}

/**
 * The delta sharing metadata from the response of a rpc.
 * It wraps a delta metadata, and adds three delta sharing fields:
 *     - version: the version of the metadata, used to generate faked delta log file on the client
 *                side.
 *     - size: the estimated size of the table at the version, used to estimate query size.
 *     - numFiles: the number of files of the table at the version, used to estimate query size.
 */
case class DeltaSharingMetadata(
    version: java.lang.Long = null,
    size: java.lang.Long = null,
    numFiles: java.lang.Long = null,
    deltaMetadata: Metadata)
    extends DeltaSharingAction {

  /** Returns the schema as a [[StructType]] */
  @JsonIgnore
  lazy val schema: StructType = deltaMetadata.schema

  /** Returns the partitionSchema as a [[StructType]] */
  @JsonIgnore
  lazy val partitionSchema: StructType = deltaMetadata.partitionSchema

  override def wrap: DeltaSharingSingleAction = DeltaSharingSingleAction(metaData = this)
}

/**
 * DeltaResponseFileAction used in delta sharing protocol. It wraps a delta single action,
 *   and adds 4 delta sharing related fields: id/version/timestamp/expirationTimestamp.
 *       - id: used to uniquely identify a file, and in idToUrl mapping for executor to get
 *             presigned url.
 *       - version/timestamp: the version and timestamp of the commit, used to generate faked delta
 *                            log file on the client side.
 *       - expirationTimestamp: indicate when the presigned url is going to expire and need a
 *                              refresh.
 *   The server is responsible to redact sensitive fields such as "tags" before returning.
 */
case class DeltaSharingFileAction(
    id: String,
    version: java.lang.Long = null,
    timestamp: java.lang.Long = null,
    expirationTimestamp: java.lang.Long = null,
    deletionVectorFileId: String = null,
    deltaSingleAction: SingleAction)
    extends DeltaSharingAction {

  lazy val path: String = {
    deltaSingleAction.unwrap match {
      case file: FileAction => file.path
      case action =>
        throw new IllegalStateException(
          s"unexpected action in delta sharing " +
          s"response: ${action.json}"
        )
    }
  }

  lazy val size: Long = {
    deltaSingleAction.unwrap match {
      case add: AddFile => add.size
      case cdc: AddCDCFile => cdc.size
      case remove: RemoveFile =>
        remove.size.getOrElse {
          throw new IllegalStateException(
            "size is missing for the remove file returned from server" +
            s", which is required by delta sharing client, response:${remove.json}."
          )
        }
      case action =>
        throw new IllegalStateException(
          s"unexpected action in delta sharing " +
          s"response: ${action.json}"
        )
    }
  }

  def getDeletionVectorOpt: Option[DeletionVectorDescriptor] = {
    deltaSingleAction.unwrap match {
      case file: FileAction => Option.apply(file.deletionVector)
      case _ => None
    }
  }

  def getDeletionVectorDeltaSharingPath(tablePath: String): String = {
    getDeletionVectorOpt.map { deletionVector =>
      // Adding offset to dvFileSize so it can load all needed bytes in memory,
      // starting from the beginning of the file instead of the `offset`.
      // There could be other DVs beyond this length in the file, but not needed by this DV.
      val dvFileSize = DeletionVectorStore.getTotalSizeOfDVFieldsInFile(
        deletionVector.sizeInBytes
      ) + deletionVector.offset.getOrElse(0)
      // This path is going to be put in the delta log file and processed by delta code, where
      // absolutePath() is applied to the path in all places, such as TahoeFileIndex and
      // DeletionVectorDescriptor, and in absolutePath, URI will apply a decode of the path.
      // Additional encoding on the tablePath and table id to allow the path still able to be
      // processed by DeltaSharingFileSystem after URI decodes it.
      DeltaSharingFileSystem
        .DeltaSharingPath(
          URLEncoder.encode(tablePath, "UTF-8"),
          URLEncoder.encode(deletionVectorFileId, "UTF-8"),
          dvFileSize
        )
        .toPath
        .toString
    }.orNull
  }

  /**
   * A helper function to get the delta sharing path for this file action to put in delta log,
   * in the format below:
   * ```
   * delta-sharing:///<url encoded table path>/<url encoded file id>/<size>
   * ```
   *
   * This is to make a unique and unchanged path for each file action, which will be mapped to
   * pre-signed url by DeltaSharingFileSystem.open(). size is needed to know how much bytes to read
   * from the FSDataInputStream.
   */
  def getDeltaSharingPath(tablePath: String): String = {
    // This path is going to be put in the delta log file and processed by delta code, where
    // absolutePath() is applied to the path in all places, such as TahoeFileIndex and
    // DeletionVectorDescriptor, and in absolutePath, URI will apply a decode of the path.
    // Additional encoding on the tablePath and table id to allow the path still able to be
    // processed by DeltaSharingFileSystem after URI decodes it.
    DeltaSharingFileSystem
      .DeltaSharingPath(
        URLEncoder.encode(tablePath, "UTF-8"),
        URLEncoder.encode(id, "UTF-8"),
        size
      )
      .toPath
      .toString
  }

  override def wrap: DeltaSharingSingleAction = DeltaSharingSingleAction(file = this)
}
