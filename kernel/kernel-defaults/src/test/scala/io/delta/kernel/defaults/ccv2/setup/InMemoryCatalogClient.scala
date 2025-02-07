package io.delta.kernel.defaults.ccv2.setup

import java.util.UUID
import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedQueue}

import io.delta.kernel.internal.actions.{Metadata, Protocol}
import io.delta.kernel.internal.fs.Path
import io.delta.kernel.internal.util.FileNames
import io.delta.kernel.utils.FileStatus

class InMemoryCatalogClient(workspace: Path = new Path("/tmp/in_memory_catalog/"))
  extends CatalogClient {
  import InMemoryCatalogClient._

  case class CatalogTableData(
      path: String,
      commits: scala.collection.mutable.ArrayBuffer[FileStatus],
      var latestProtocol: Option[Protocol],
      var latestMetadata: Option[Metadata]) {

    def maxCommitVersion: Long =
      commits.lastOption.map(_.getPath).map(FileNames.uuidCommitDeltaVersion).getOrElse(-1L)

    def latestSchemaString: Option[String] = latestMetadata.map(_.getSchemaString)
  }

  java.nio.file.Files.createDirectories(java.nio.file.Paths.get(workspace.toString))

  /** Map from tableName -> CatalogTableData */
  val catalogTables = new ConcurrentHashMap[String, CatalogTableData]()

  /** Map from tableName -> stagingTablePath */
  // TODO: support concurrent staging tables
  val stagingTables = new ConcurrentHashMap[String, String]()

  override def createStagingTable(tableName: String): CreateStagingTableResponse = {
    catalogTables.get(tableName) match {
      case null =>
        val uuid = UUID.randomUUID().toString.replace("-", "").take(15)
        val tablePath = s"${workspace.toString}/${tableName}_$uuid"
        logger.info(s"creating new staging table $tableName -> $tablePath")
        stagingTables.put(tableName, tablePath)
        CreateStagingTableResponse.Success(tablePath)
      case data =>
        logger.info(s"table already exists $tableName, cannot create a new staging table")
        CreateStagingTableResponse.TableAlreadyExists(tableName)
    }
  }

  override def resolveTable(tableName: String): ResolveTableResponse = {
    catalogTables.get(tableName) match {
      case null =>
        ResolveTableResponse.TableDoesNotExist(tableName)
      case data =>
        ResolveTableResponse.Success(
          path = data.path,
          version = data.maxCommitVersion,
          protocol = data.latestProtocol,
          metadata = data.latestMetadata,
          schemaString = data.latestSchemaString
        )
    }
  }

  override def getCommits(tableName: String): GetCommitsResponse = {
    catalogTables.get(tableName) match {
      case null =>
        logger.info(s"table does not exist :: $tableName")
        GetCommitsResponse.TableDoesNotExist(tableName)
      case tableData =>
        logger.info(s"table exists :: $tableName")
        val tableCommitsStr = tableData.commits.map(f => s"  - ${f.getPath}").mkString("\n")
        logger.info(s"tableData.commits:\n$tableCommitsStr")

        GetCommitsResponse.Success(tableData.commits)
    }
  }

  override def commit(
      tableName: String,
      commitFile: FileStatus,
      updatedProtocol: Option[Protocol],
      updatedMetadata: Option[Metadata]): CommitResponse = {
    catalogTables.get(tableName) match {
      case null =>
        stagingTables.get(tableName) match {
          case null =>
            logger.info(s"table does not exist :: $tableName")
            CommitResponse.TableDoesNotExist(tableName)
          case stagingTablePath =>
            logger.info(s"committing to an existing staging table :: $tableName")
            val tableData = CatalogTableData(
              path = stagingTablePath,
              commits = scala.collection.mutable.ArrayBuffer(commitFile),
              latestProtocol = updatedProtocol,
              latestMetadata = updatedMetadata
            )
            catalogTables.put(tableName, tableData)
            stagingTables.remove(tableName)
            CommitResponse.Success
        }
      case tableData =>
        logger.info(s"table exists :: $tableName")
        logger.info(s"commitFile: ${commitFile.getPath}")

        val expectedCommitVersion = tableData.maxCommitVersion + 1
        val commitVersion = FileNames.uuidCommitDeltaVersion(commitFile.getPath)

        logger.info(s"expectedCommitVersion: $expectedCommitVersion")
        logger.info(s"commitVersion: $commitVersion")

        if (commitVersion != expectedCommitVersion) {
          return CommitResponse.CommitVersionConflict(commitVersion, expectedCommitVersion)
        }

        tableData.commits += commitFile
        updatedProtocol.foreach(newP => tableData.latestProtocol = Some(newP))
        updatedMetadata.foreach(newM => tableData.latestMetadata = Some(newM))

        val tableCommitsStr = tableData.commits.map(f => s"  - ${f.getPath}").mkString("\n")
        logger.info(s"tableData.commits:\n$tableCommitsStr")

        CommitResponse.Success
    }
  }
}

object InMemoryCatalogClient {
  private val logger = org.slf4j.LoggerFactory.getLogger(classOf[InMemoryCatalogClient])
}
