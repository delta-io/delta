package io.delta.kernel.defaults.ccv2.setup

import java.util.{Collections, Optional, UUID}

import scala.collection.JavaConverters._

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
  def getResolvedMetadata(tableName: String): ResolvedMetadata = {
    new ResolvedCatalogMetadata(tableName, engine, catalogClient)
  }

  def getStagingTableResolvedMetadata(
      tableName: String,
      engine: Engine,
      catalogClient: CatalogClient): ResolvedMetadata = {
    new StagingCatalogResolvedMetadata(tableName, engine, catalogClient)
  }

}

//////////////////////////////////////
// ResolvedCatalogMetadataCommitter //
//////////////////////////////////////

trait ResolvedCatalogMetadataCommitter extends { self: ResolvedMetadata =>

  import JavaScalaUtils._

  // lazy so the child can finish initializing before we use `getPath`
  private lazy val logPath = new Path(getPath, "_delta_log")
  private lazy val hadoopFileSystem = FileSystem.getLocal(new Configuration())
  
  def engine: Engine
  def catalogClient: CatalogClient
  def logger: Logger
  def tableName: String

  /** The *potentially* unbackfilled commits. Some may actually be backfilled. */
  def unbackfilledCommits: Seq[FileStatus]

  override def commit(
      commitAsVersion: Long,
      finalizedActions: CloseableIterator[Row],
      newProtocol: Optional[Protocol],
      newMetadata: Optional[Metadata]): Unit = {
    val logPath = s"$getPath/_delta_log"
    val uuidCommitsPath = s"$logPath/_commits"
    val commitFilePath =
      f"$uuidCommitsPath/$commitAsVersion%020d.${UUID.randomUUID().toString}.json"

    logger.info(s"tableName: $tableName")
    logger.info(s"dataPath: $getPath")
    logger.info(s"commitAsVersion: $commitAsVersion")
    logger.info(s"commitFilePath: $commitFilePath")
    logger.info(s"newProtocol: $newProtocol")
    logger.info(s"newMetadata: $newMetadata")

    logger.info("Write UUID commit file: START")
    engine
      .getJsonHandler
      .writeJsonFileAtomically(commitFilePath, finalizedActions, false /* overwrite */)
    logger.info("Write UUID commit file: END")

    val hadoopFs = hadoopFileSystem.getFileStatus(new HadoopPath(commitFilePath))

    val kernelFs =
      FileStatus.of(hadoopFs.getPath.toString, hadoopFs.getLen, hadoopFs.getModificationTime)

    logger.info(s"hadoopFS: $hadoopFs")
    logger.info(s"kernelFs: $kernelFs")

    logger.info("Commit to catalog: START")

    catalogClient
      .commit(tableName, kernelFs, newProtocol.asScala, newMetadata.asScala) match {
      case CommitResponse.Success =>
        logger.info("Commit to catalog: SUCCESS")
      case CommitResponse.TableDoesNotExist(tableName) =>
        logger.info("Commit to catalog: TABLE DOES NOT EXIST")
        throw new RuntimeException(s"Table $tableName does not exist in the catalog")
      case CommitResponse.CommitVersionConflict(attempted, expected) =>
        logger.info("Commit to catalog: COMMIT VERSION CONFLICT")
        throw new ConcurrentWriteException()
    }

    try {
      if (commitAsVersion % 5 == 0) {
        backfill(commitAsVersion, kernelFs)
      } else {
        logger.info("Skipping backfill")
      }
    } catch {
      case e: Throwable => logger.warn("Backfill failed, ignoring", e)
    }
  }

  private def backfill(commitAsVersion: Long, committedFileStatus: FileStatus): Unit = {
    logger.info(s"Backfilling: START. commitAsVersion=$commitAsVersion")
    val allCandidateUnbackfilledFiles = unbackfilledCommits ++ Seq(committedFileStatus)

    allCandidateUnbackfilledFiles
      // e.g. perhaps some of the deltas we got back from the catalog were in fact backfilled
      .filter(fs => FileNames.isUnbackfilledDeltaFile(fs.getPath))
      .foreach { fs =>
        val fsVersion = FileNames.uuidCommitDeltaVersion(fs.getPath)
        val backfilledFilePath = FileNames.deltaFile(logPath, fsVersion)
        logger.info(s"Unbackfilled fs: ${fs.getPath}")
        logger.info(s"Unbackfilled version: $fsVersion")
        logger.info(s"Backfilled file path: $backfilledFilePath")

        if (hadoopFileSystem.exists(new HadoopPath(backfilledFilePath))) {
          logger.info(s"Backfilled file already exists: $backfilledFilePath")
        } else {
          logger.info(s"Backfilling: $backfilledFilePath")
          val sourceUnbackfilledPath = new HadoopPath(fs.getPath)
          val targetBackfilledPath = new HadoopPath(backfilledFilePath)
          logger.info(s"Copying $sourceUnbackfilledPath to $targetBackfilledPath")

          // TODO: This is not atomic btw
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

    logger.info(s"Invoking catalog with latest backfilled version: $commitAsVersion")
    catalogClient.setLatestBackfilledVersion(tableName, commitAsVersion)
    logger.info("Backfilling: END")
  }
}

////////////////////////////////////
// StagingCatalogResolvedMetadata //
////////////////////////////////////

class StagingCatalogResolvedMetadata(
    override val tableName: String,
    override val engine: Engine,
    override val catalogClient: CatalogClient)
  extends ResolvedMetadata with ResolvedCatalogMetadataCommitter {

  import StagingCatalogResolvedMetadata._

  val stagingTablePath = catalogClient.createStagingTable(tableName) match {
    case CreateStagingTableResponse.Success(path) => path
    case CreateStagingTableResponse.TableAlreadyExists(tableName) =>
      throw new RuntimeException(s"Table $tableName already exists")
  }

  _logger.info(s"stagingTablePath: $stagingTablePath")

  // ===== ResolvedCatalogMetadataCommitter overrides ===== //

  override def logger: Logger = _logger

  override def unbackfilledCommits: Seq[FileStatus] = Seq.empty

  // ===== ResolvedMetadata overrides ===== //

  override def getPath: String = stagingTablePath

  override def getVersion: Long = -1

  override def getLogSegment: Optional[LogSegment] = Optional.empty()

  override def getProtocol: Optional[Protocol] = Optional.empty()

  override def getMetadata: Optional[Metadata] = Optional.empty()

  override def getSchemaString: Optional[String] = Optional.empty()
}

object StagingCatalogResolvedMetadata {
  private val _logger = org.slf4j.LoggerFactory.getLogger(classOf[StagingCatalogResolvedMetadata])
}

/////////////////////////////
// ResolvedCatalogMetadata //
/////////////////////////////

class ResolvedCatalogMetadata(
    override val tableName: String,
    override val engine: Engine,
    override val catalogClient: CatalogClient)
  extends ResolvedMetadata with ResolvedCatalogMetadataCommitter {
  import JavaScalaUtils._
  import ResolvedCatalogMetadata._

  private val resolvedTableResponse: ResolveTableResponse.Success =
    catalogClient.resolveTable(tableName) match {
      case success: ResolveTableResponse.Success =>
        _logger.info(s"Received success ResolveTableResponse: $success")
        success
      case ResolveTableResponse.TableDoesNotExist(tableName) =>
        throw new RuntimeException(s"Table $tableName does not exist")
    }

  private val getCommitsResponse: GetCommitsResponse.Success =
    catalogClient.getCommits(tableName) match {
      case success: GetCommitsResponse.Success =>
        _logger.info(s"Received success GetCommitsResponse: $success")
        success
      case GetCommitsResponse.TableDoesNotExist(tableName) =>
        throw new RuntimeException(s"Table $tableName does not exist")
    }

  private val dataPath = resolvedTableResponse.path

  // ===== ResolvedCatalogMetadataCommitter overrides ===== //

  override def logger: Logger = _logger

  override def unbackfilledCommits: Seq[FileStatus] =
    getLogSegment
      .map[List[io.delta.kernel.utils.FileStatus]](
        logSegment => logSegment.getDeltas.asScala.toList)
      .asScala
      .getOrElse(Seq.empty)

  // ===== ResolvedMetadata overrides ===== //

  override def getPath: String = dataPath

  override def getVersion: Long = resolvedTableResponse.version

  override def getLogSegment: Optional[LogSegment] =
    Optional.of(
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

}

object ResolvedCatalogMetadata {
  private val _logger = org.slf4j.LoggerFactory.getLogger(classOf[ResolvedCatalogMetadata])
}

