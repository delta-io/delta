package io.delta.kernel.defaults.ccv2.setup

import io.delta.kernel.internal.actions.{Metadata, Protocol}
import io.delta.kernel.utils.FileStatus

/////////////////////////////////////////////////////////////////
// CatalogClient interactions -- no direct contact with Kernel //
/////////////////////////////////////////////////////////////////

trait CatalogClient {
  def createStagingTable(tableName: String): CreateStagingTableResponse

  def resolveTable(tableName: String): ResolveTableResponse

  def getCommits(tableName: String): GetCommitsResponse

  // TODO: we might want to pass the commit timestamp? For the case of ICT?
  // TODO: can we avoid passing in Kernel/Delta types to the catalog?
  def commit(
      tableName: String,
      commitFile: FileStatus,
      updatedProtocol: Option[Protocol] = None,
      updatedMetadata: Option[Metadata] = None): CommitResponse

  def setLatestBackfilledVersion(
      tableName: String,
      latestBackfilledVersion: Long): SetLatestBackfilledVersionResponse
}

// ===== CreateStagingTableResponse =====

sealed trait CreateStagingTableResponse

object CreateStagingTableResponse {
  final case class Success(path: String) extends CreateStagingTableResponse

  final case class TableAlreadyExists(tableName: String) extends CreateStagingTableResponse
}

// ===== ResolveTableResponse =====

sealed trait ResolveTableResponse

object ResolveTableResponse {

  final case class Success(
      path: String,
      version: Long,
      protocol: Option[Protocol],
      metadata: Option[Metadata],
      schemaString: Option[String]) extends ResolveTableResponse

  final case class TableDoesNotExist(tableName: String) extends ResolveTableResponse
}

// ===== GetCommitsResponse =====

sealed trait GetCommitsResponse

object GetCommitsResponse {
  final case class Success(
      commits: scala.collection.immutable.Seq[FileStatus]) extends GetCommitsResponse

  final case class TableDoesNotExist(tableName: String) extends GetCommitsResponse
}

// ===== CommitResponse =====commits

sealed trait CommitResponse

object CommitResponse {
  case object Success extends CommitResponse

  final case class TableDoesNotExist(tableName: String) extends CommitResponse

  final case class CommitVersionConflict(
      attemptedCommitVersion: Long,
      expectedVersion: Long) extends CommitResponse
}

// ===== SetLatestBackfilledVersionResponse =====

sealed trait SetLatestBackfilledVersionResponse

object SetLatestBackfilledVersionResponse {
  case object Success extends SetLatestBackfilledVersionResponse

  final case class TableDoesNotExist(tableName: String) extends SetLatestBackfilledVersionResponse
}
