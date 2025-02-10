package io.delta.kernel.defaults.ccv2.setup

import java.util.{Collections, Optional, UUID}

import scala.collection.JavaConverters._

import io.delta.kernel.TransactionCommitResult
import io.delta.kernel.ccv2.ResolvedMetadata
import io.delta.kernel.data.Row
import io.delta.kernel.engine.Engine
import io.delta.kernel.exceptions.ConcurrentWriteException
import io.delta.kernel.internal.actions.{Metadata, Protocol}
import io.delta.kernel.internal.fs.Path
import io.delta.kernel.internal.snapshot.LogSegment
import io.delta.kernel.internal.util.FileNames
import io.delta.kernel.utils.{CloseableIterator, FileStatus}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path => HadoopPath}
import org.slf4j.Logger

class CCv2Client(engine: Engine, catalogClient: CatalogClient) {
  import CCv2Client._

  def getResolvedMetadata(tableName: String): ResolvedMetadata = {
    logger.info(s"tableName=$tableName")
    new ResolvedCatalogMetadata(tableName, engine, catalogClient)
  }

  def getStagingTableResolvedMetadata(
      tableName: String,
      engine: Engine,
      catalogClient: CatalogClient): ResolvedMetadata = {
    logger.info(s"tableName=$tableName")

    val stagingTablePath = catalogClient.createStagingTable(tableName) match {
      case CreateStagingTableResponse.Success(path) => path
      case CreateStagingTableResponse.TableAlreadyExists(tableName) =>
        throw new RuntimeException(s"Table $tableName already exists")
    }

    logger.info(s"stagingTablePath: $stagingTablePath")

    new ResolvedMetadata {
      override def getPath: String = stagingTablePath

      override def getVersion: Long = -1

      override def getLogSegment: Optional[LogSegment] = Optional.empty()

      override def getProtocol: Optional[Protocol] = Optional.empty()

      override def getMetadata: Optional[Metadata] = Optional.empty()

      override def getSchemaString: Optional[String] = Optional.empty()

      override def getCommitFunction: ResolvedMetadata.CommitFunction =
        commitFunctionImpl(logger, engine, catalogClient, tableName, stagingTablePath, Seq.empty)
    }
  }

}

object CCv2Client {
  import JavaScalaUtils._

  private val logger = org.slf4j.LoggerFactory.getLogger(classOf[CCv2Client])

  /**
   * @param unbackfilledCommits the *potentially* unbackfilled commits. They may be backfilled.
   */
  def commitFunctionImpl(
      _logger: Logger,
      engine: Engine,
      catalogClient: CatalogClient,
      tableName: String,
      dataPath: String,
      unbackfilledCommits: Seq[FileStatus]): ResolvedMetadata.CommitFunction = {
    new ResolvedMetadata.CommitFunction {
      private val logPath = new Path(dataPath, "_delta_log")
      private val hadoopFileSystem = FileSystem.getLocal(new Configuration())

      override def commit(
          commitAsVersion: Long,
          actions: CloseableIterator[Row],
          newProtocol: Optional[Protocol],
          newMetadata: Optional[Metadata]): TransactionCommitResult = {
        val logPath = s"$dataPath/_delta_log"
        val uuidCommitsPath = s"$logPath/_commits"
        val commitFilePath =
          f"$uuidCommitsPath/$commitAsVersion%020d.${UUID.randomUUID().toString}.json"

        _logger.info(s"tableName: $tableName")
        _logger.info(s"dataPath: $dataPath")
        _logger.info(s"commitAsVersion: $commitAsVersion")
        _logger.info(s"commitFilePath: $commitFilePath")
        _logger.info(s"newProtocol: $newProtocol")
        _logger.info(s"newMetadata: $newMetadata")

        _logger.info("Write UUID commit file: START")
        engine
          .getJsonHandler
          .writeJsonFileAtomically(commitFilePath, actions, false /* overwrite */)
        _logger.info("Write UUID commit file: END")

        val hadoopFs = hadoopFileSystem.getFileStatus(new HadoopPath(commitFilePath))

        val kernelFs =
          FileStatus.of(hadoopFs.getPath.toString, hadoopFs.getLen, hadoopFs.getModificationTime)

        _logger.info(s"hadoopFS: $hadoopFs")
        _logger.info(s"kernelFs: $kernelFs")

        _logger.info("Commit to catalog: START")

        val result = catalogClient
          .commit(tableName, kernelFs, newProtocol.asScala, newMetadata.asScala) match {
            case CommitResponse.Success =>
              _logger.info("Commit to catalog: SUCCESS")
              // TODO: invoke backfill since this commit was successful
              new TransactionCommitResult(commitAsVersion, Seq.empty.asJava)
            case CommitResponse.TableDoesNotExist(tableName) =>
              _logger.info("Commit to catalog: TABLE DOES NOT EXIST")
              throw new RuntimeException(s"Table $tableName does not exist in the catalog")
            case CommitResponse.CommitVersionConflict(attempted, expected) =>
              _logger.info("Commit to catalog: COMMIT VERSION CONFLICT")
              throw new ConcurrentWriteException()
        }

        try {
          if (commitAsVersion % 5 == 0) {
            backfill(commitAsVersion, kernelFs)
          } else {
            _logger.info("Skipping backfill")
          }
        } catch {
          case e: Throwable => _logger.warn("Backfill failed, ignoring", e)
        }

        result
      }

      private def backfill(commitAsVersion: Long, committedFileStatus: FileStatus): Unit = {
        _logger.info(s"Backfilling: START. commitAsVersion=$commitAsVersion")
        val allCandidateUnbackfilledFiles = unbackfilledCommits ++ Seq(committedFileStatus)

        allCandidateUnbackfilledFiles
          // e.g. perhaps some of the deltas we got back from the catalog were in fact backfilled
          .filter(fs => FileNames.isUnbackfilledDeltaFile(fs.getPath))
          .foreach { fs =>
            val fsVersion = FileNames.uuidCommitDeltaVersion(fs.getPath)
            val backfilledFilePath = FileNames.deltaFile(logPath, fsVersion)
            _logger.info(s"Unbackfilled fs: ${fs.getPath}")
            _logger.info(s"Unbackfilled version: $fsVersion")
            _logger.info(s"Backfilled file path: $backfilledFilePath")

            if (hadoopFileSystem.exists(new HadoopPath(backfilledFilePath))) {
              _logger.info(s"Backfilled file already exists: $backfilledFilePath")
            } else {
              _logger.info(s"Backfilling: $backfilledFilePath")
              val sourceUnbackfilledPath = new HadoopPath(fs.getPath)
              val targetBackfilledPath = new HadoopPath(backfilledFilePath)
              _logger.info(s"Copying $sourceUnbackfilledPath to $targetBackfilledPath")

              FileUtil.copy(
                hadoopFileSystem, // sourceFileSystem
                sourceUnbackfilledPath, // sourcePath
                hadoopFileSystem, // targetFileSystem
                targetBackfilledPath, // targetPath
                false, // deleteSource
                false, // overwrite
                hadoopFileSystem.getConf)
            }
        }

        _logger.info(s"Invoking catalog with latest backfilled version: $commitAsVersion")
        catalogClient.setLatestBackfilledVersion(tableName, commitAsVersion)
        _logger.info("Backfilling: END")
      }
    }
  }
}

class ResolvedCatalogMetadata(
    tableName: String,
    engine: Engine,
    catalogClient: CatalogClient) extends ResolvedMetadata {
  import JavaScalaUtils._
  import ResolvedCatalogMetadata._

  private val resolvedTableResponse: ResolveTableResponse.Success =
    catalogClient.resolveTable(tableName) match {
      case success: ResolveTableResponse.Success => success
      case ResolveTableResponse.TableDoesNotExist(tableName) =>
        throw new RuntimeException(s"Table $tableName does not exist")
    }

  private val getCommitsResponse: GetCommitsResponse.Success =
    catalogClient.getCommits(tableName) match {
      case success: GetCommitsResponse.Success => success
      case GetCommitsResponse.TableDoesNotExist(tableName) =>
        throw new RuntimeException(s"Table $tableName does not exist")
    }

  private val dataPath = resolvedTableResponse.path

  override def getPath: String = dataPath

  override def getVersion: Long = resolvedTableResponse.version

  override def getLogSegment: Optional[LogSegment] = Optional.of(
    new LogSegment(
      new Path(resolvedTableResponse.path, "_delta_log"),
      resolvedTableResponse.version,
      getCommitsResponse.commits.toList.asJava,
      Collections.emptyList(),
      100
    )
  )

  override def getProtocol: Optional[Protocol] = resolvedTableResponse.protocol.asJava

  override def getMetadata: Optional[Metadata] = resolvedTableResponse.metadata.asJava

  override def getSchemaString: Optional[String] = resolvedTableResponse.schemaString.asJava

  override def getCommitFunction: ResolvedMetadata.CommitFunction = {
    CCv2Client.commitFunctionImpl(
      logger,
      engine,
      catalogClient,
      tableName,
      dataPath,
      getLogSegment
        .map[List[io.delta.kernel.utils.FileStatus]](
          logSegment => logSegment.getDeltas.asScala.toList)
        .asScala
        .getOrElse(Seq.empty))
  }

}

object ResolvedCatalogMetadata {
  private val logger = org.slf4j.LoggerFactory.getLogger(classOf[ResolvedCatalogMetadata])
}
