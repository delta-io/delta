package io.delta.kernel.defaults.ccv2

import java.util.{Collections, Optional, UUID}

import scala.collection.JavaConverters._

import io.delta.kernel.TransactionCommitResult
import io.delta.kernel.ccv2.ResolvedMetadata
import io.delta.kernel.data.Row
import io.delta.kernel.engine.Engine
import io.delta.kernel.internal.actions.{Metadata, Protocol}
import io.delta.kernel.internal.fs.Path
import io.delta.kernel.internal.snapshot.LogSegment
import io.delta.kernel.internal.util.FileNames
import io.delta.kernel.utils.{CloseableIterable, CloseableIterator, FileStatus}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem

/////////////////////////////////////////////////////////////////
// CatalogClient interactions -- no direct contact with Kernel //
/////////////////////////////////////////////////////////////////

trait CatalogClient {
  def resolveTable(tableName: String): ResolveTableResponse

  def getCommits(tableName: String): GetCommitsResponse

  def commit(
      tableName: String,
      commitFile: FileStatus,
      updatedProtocol: Option[Protocol] = None,
      updatedMetadata: Option[Metadata] = None): CommitResponse
}

trait ResolveTableResponse {
  def getPath: String
  def getVersion: Long
  def getProtocol: Option[Protocol]
  def getMetadata: Option[Metadata]
  def getSchemaString: Option[String]
}

trait GetCommitsResponse {
  def getCommits: Seq[FileStatus]
}

sealed trait CommitResponse

object CommitResponse {
  case object Success extends CommitResponse

  final case class TableDoesNotExist(tableName: String) extends CommitResponse

  final case class CommitVersionConflict(
      attemptedCommitVersion: Long,
      expectedVersion: Long) extends CommitResponse
}

///////////////////////////
// InMemoryCatalogClient //
///////////////////////////

class InMemoryCatalogClient extends CatalogClient {
  case class CatalogTableData(
      path: String,
      commits: scala.collection.mutable.ArrayBuffer[FileStatus],
      var latestProtocol: Option[Protocol],
      var latestMetadata: Option[Metadata]) {

    // TODO: parse the version for UUID commit files correctly
    def maxCommitVersion: Long =
      commits.lastOption.map(_.getPath).map(FileNames.deltaVersion).getOrElse(-1L)

    def latestSchemaString: Option[String] = latestMetadata.map(_.getSchemaString)
  }

  val catalogTables = new java.util.concurrent.ConcurrentHashMap[String, CatalogTableData]()

  override def resolveTable(tableName: String): ResolveTableResponse = {
    catalogTables.get(tableName) match {
      case null =>
        throw new IllegalArgumentException(s"Table $tableName not found")
      case data =>
        new ResolveTableResponse {
          override def getPath: String = data.path
          override def getVersion: Long = data.maxCommitVersion
          override def getProtocol: Option[Protocol] = data.latestProtocol
          override def getMetadata: Option[Metadata] = data.latestMetadata
          override def getSchemaString: Option[String] = data.latestSchemaString
        }
    }
  }

  override def getCommits(tableName: String): GetCommitsResponse = {
   catalogTables.get(tableName) match {
     case null =>
       throw new IllegalArgumentException(s"Table $tableName not found")
     case data =>
       new GetCommitsResponse {
         override def getCommits: Seq[FileStatus] = data.commits
       }
     }
  }

  override def commit(
      tableName: String,
      commitFile: FileStatus,
      updatedProtocol: Option[Protocol],
      updatedMetadata: Option[Metadata]): CommitResponse = {
    catalogTables.get(tableName) match {
      case null => CommitResponse.TableDoesNotExist(tableName)

      case tableData =>
        val expectedCommitVersion = tableData.maxCommitVersion + 1
        val commitVersion = FileNames.deltaVersion(commitFile.getPath)

        if (commitVersion != expectedCommitVersion) {
          return CommitResponse.CommitVersionConflict(commitVersion, expectedCommitVersion)
        }

        tableData.commits += commitFile
        updatedProtocol.foreach(newP => tableData.latestProtocol = Some(newP))
        updatedMetadata.foreach(newM => tableData.latestMetadata = Some(newM))

        CommitResponse.Success
    }
  }
}

object InMemoryCatalogClient {

}

/////////////////////////////////////
// ResolvedInMemoryCatalogMetadata //
/////////////////////////////////////

class ResolvedInMemoryCatalogMetadata(
    tableName: String,
    engine: Engine,
    inMemoryCatalogClient: InMemoryCatalogClient) extends ResolvedMetadata {

  import OptionConverters._

  private val resolvedTableResponse = inMemoryCatalogClient.resolveTable(tableName)
  private val getCommitsResponse = inMemoryCatalogClient.getCommits(tableName)

  private val dataPath = resolvedTableResponse.getPath
  private val logPath = s"$dataPath/_delta_log"
  private val uuidCommitsPath = s"$logPath/_commits"

  override def getPath: String = dataPath

  override def getVersion: Long = resolvedTableResponse.getVersion

  override def getLogSegment: Optional[LogSegment] = Optional.of(
    new LogSegment(
      new Path(resolvedTableResponse.getPath, "_delta_log"),
      resolvedTableResponse.getVersion,
      getCommitsResponse.getCommits.toList.asJava,
      Collections.emptyList(),
      100
    )
  )

  override def getProtocol: Optional[Protocol] = resolvedTableResponse.getProtocol.asJava

  override def getMetadata: Optional[Metadata] = resolvedTableResponse.getMetadata.asJava

  override def getSchemaString: Optional[String] = resolvedTableResponse.getSchemaString.asJava

  override def commit(
      commitAsVersion: Long,
      actions: CloseableIterator[Row],
      newProtocol: Optional[Protocol],
      newMetadata: Optional[Metadata]): TransactionCommitResult = {
    val commitFilePath =
      f"$uuidCommitsPath/$commitAsVersion%020d.${UUID.randomUUID().toString}.json"

    engine
      .getJsonHandler
      .writeJsonFileAtomically(commitFilePath, actions, false /* overwrite */)

    val hadoopFileStatus = FileSystem
      .getLocal(new Configuration())
      .getFileStatus(new org.apache.hadoop.fs.Path(commitFilePath))

    val kernelFileStatus =
      new FileStatus(commitFilePath, hadoopFileStatus.getLen, hadoopFileStatus.getModificationTime)

    val response = inMemoryCatalogClient
      .commit(tableName, kernelFileStatus, newProtocol.asScala, newMetadata.asScala)
  }

}

///////////
// Other //
///////////

object OptionConverters {
  implicit class ScalaOptionOps[T](val scalaOption: Option[T]) extends AnyVal {
    def asJava: Optional[T] = Optional.ofNullable(scalaOption.orNull)
  }

  implicit class JavaOptionalOps[T](val javaOptional: Optional[T]) extends AnyVal {
    def asScala: Option[T] = if (javaOptional.isPresent) Some(javaOptional.get()) else None
  }
}
