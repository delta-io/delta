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

package org.apache.spark.sql.delta.zorder

import java.util.UUID

import org.apache.spark.sql.delta.{DeltaErrors, DeltaLog, SnapshotIsolation}
import org.apache.spark.sql.delta.actions.AddFile
import org.apache.spark.sql.delta.actions.AddFile.Tags.{ZCUBE_ID, ZCUBE_ZORDER_BY, ZCUBE_ZORDER_CURVE}
import org.apache.spark.sql.delta.commands.DeltaCommand
import org.apache.spark.sql.delta.util.JsonUtils
import org.apache.spark.sql.delta.zorder.ZCubeInfo.ZCubeID

import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal}


/**
 * [[ZCube]] identifying information.
 * @param zCubeID unique identifier of a OPTIMIZE ZORDER BY command run
 * @param zOrderBy list of ZORDER BY columns for the run
 * @note This piece of info could be put in table Metadata or taken from CommitInfo
 *       but is currently inlined here for simplicity.
 */
case class ZCubeInfo(zCubeID: ZCubeID, zOrderBy: Seq[String]) {
  require(zOrderBy.nonEmpty)
}

object ZCubeInfo extends DeltaCommand {
  type ZCubeID = String // Could be UUID, but there's no implicit encoding for that.

  /**
   * Preferred way of creating a ZCubeInfo for a new ZCube.
   * Automatically generates a unique zCubeID.
   */
  def apply(zOrderBy: Seq[String]): ZCubeInfo = {
    val zCubeID = UUID.randomUUID.toString
    ZCubeInfo(zCubeID, zOrderBy)
  }

  private val ZCUBE_ID_KEY = AddFile.tag(ZCUBE_ID)
  private val ZORDER_BY_KEY = AddFile.tag(ZCUBE_ZORDER_BY)
  private val ZORDER_CURVE = AddFile.tag(ZCUBE_ZORDER_CURVE)

  /**
   * Serializes the given `zCubeInfo` to a Map[String, String] that can be used as or merged into
   * [[AddFile.tags]].
   */
  def toAddFileTags(zCubeInfo: ZCubeInfo): Map[String, String] = {
    Map(
      ZCUBE_ID_KEY -> zCubeInfo.zCubeID,
      ZORDER_BY_KEY -> JsonUtils.toJson(zCubeInfo.zOrderBy))
  }

  /**
   * Deserializes a `ZCubeInfo` object from an [[AddFile.tags]] map, if present.
   */
  def fromAddFileTags(tags: Map[String, String]): Option[ZCubeInfo] = {
    for {
      zCubeID <- tags.get(ZCUBE_ID_KEY)
      zOrderByColsAsJson <- tags.get(ZORDER_BY_KEY)
    } yield {
      val zOrderByCols = JsonUtils.fromJson[Seq[String]](zOrderByColsAsJson)
      ZCubeInfo(zCubeID, zOrderByCols)
    }
  }

  /**
   * If the given file was written by an OPTIMIZE ZORDER BY job,
   * return the corresponding [[ZCubeInfo]]. Otherwise return [[None]].
   */
  def getForFile(file: AddFile): Option[ZCubeInfo] = {
    for {
      tags <- Option(file.tags)
      zCubeInfo <- ZCubeInfo.fromAddFileTags(tags)
    } yield {
      zCubeInfo
    }
  }

  /**
   * Update the given file's metadata to make it part of the given zCubeInfo.
   */
  def setForFile(file: AddFile, zCubeInfo: ZCubeInfo): AddFile = {
    val oldTags = Option(file.tags).getOrElse(Map.empty)
    val newTags = oldTags ++ ZCubeInfo.toAddFileTags(zCubeInfo)
    file.copy(tags = newTags)
  }

  /**
   * Clears the ZCubeInfo metadata of the given file to make it appear as unoptimized.
   */
  def unsetForFile(file: AddFile): AddFile = {
    val oldTags = file.tags
    val newTags = if (oldTags == null) null else oldTags --
      Seq(ZCUBE_ID_KEY, ZORDER_BY_KEY, ZORDER_CURVE)
    file.copy(tags = newTags)
  }
}


