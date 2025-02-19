package io.delta.kernel.defaults.ccv2.setup

import java.util.UUID
import java.util.concurrent.ConcurrentHashMap

import io.delta.kernel.internal.actions.{Metadata, Protocol}
import io.delta.kernel.internal.fs.Path
import io.delta.kernel.internal.util.FileNames
import io.delta.kernel.utils.FileStatus

class InMemoryCatalogClient(workspace: Path = new Path("/tmp/in_memory_catalog/"))
  extends CatalogClient {
  import InMemoryCatalogClient._

  case class CatalogTableData(
      path: String,
      var maxCommitVersion: Long = -1L,
      var commits: scala.collection.mutable.ArrayBuffer[FileStatus],
      var latestBackfilledVersion: Option[Long] = None,
      var latestProtocol: Option[Protocol],
      var latestMetadata: Option[Metadata]) {

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
        logger.info(s"tableData.commits (size=${tableData.commits.size}):\n$tableCommitsStr")

        GetCommitsResponse.Success(tableData.commits.toList) // return an IMMUTABLE COPY
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
            logger.info(s"Committing to a staging table :: $tableName")
            val commitVersion = FileNames.uuidCommitDeltaVersion(commitFile.getPath)
            logger.info(s"[staging table] commitVersion: $commitVersion")

            if (commitVersion != 0) {
              // TODO: support converting a fs table to a ccv2 table ???
              throw new RuntimeException(
                s"[staging table] Expected first commit version 0 but got version $commitVersion")
            }
            val tableData = CatalogTableData(
              path = stagingTablePath,
              maxCommitVersion = commitVersion,
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
          return CommitResponse.CommitVersionConflict(
            commitVersion,
            expectedCommitVersion,
            tableData.commits.toList // return an IMMUTABLE COPY
          )
        }

        tableData.maxCommitVersion = commitVersion
        tableData.commits += commitFile
        updatedProtocol.foreach(newP => tableData.latestProtocol = Some(newP))
        updatedMetadata.foreach(newM => tableData.latestMetadata = Some(newM))

        val tableCommitsStr = tableData.commits.map(f => s"  - ${f.getPath}").mkString("\n")
        logger.info(s"tableData.commits:\n$tableCommitsStr")

        CommitResponse.Success
    }
  }

  override def setLatestBackfilledVersion(
      tableName: String,
      latestBackfilledVersion: Long): SetLatestBackfilledVersionResponse = {
    logger.info(s"tableName: $tableName, latestBackfilledVersion: $latestBackfilledVersion")

    catalogTables.get(tableName) match {
      case null => SetLatestBackfilledVersionResponse.TableDoesNotExist(tableName)
      case tableData =>
        if (latestBackfilledVersion > tableData.latestBackfilledVersion.getOrElse(-1L)) {
          tableData.latestBackfilledVersion = Some(latestBackfilledVersion)
          logger.info(s"Updated latestBackfilledVersion to $latestBackfilledVersion")

          tableData.commits = tableData
            .commits
            .filter(f => FileNames.isUnbackfilledDeltaFile(f.getPath))
            .dropWhile(f => {
              val doDrop = FileNames.uuidCommitDeltaVersion(f.getPath) <= latestBackfilledVersion
              logger.info(s"Checking if we should drop ${f.getPath}: $doDrop")
              doDrop
            })
          logger.info(s"Removed catalog commits older than or equal to $latestBackfilledVersion")
        }
        SetLatestBackfilledVersionResponse.Success
    }
  }
}

object InMemoryCatalogClient {
  val logger = org.slf4j.LoggerFactory.getLogger(classOf[InMemoryCatalogClient])
}
