package io.delta.kernel.defaults.ccv2

import io.delta.kernel.ccv2.ResolvedMetadata
import io.delta.kernel.internal.actions.{Metadata, Protocol}
import io.delta.kernel.utils.FileStatus

////////////////////////////////
// CatalogClient interactions //
////////////////////////////////

trait CatalogClient {
  def resolveTable(tableName: String): ResolveTableResponse
  def getCommits(): GetCommitsResponse
}

trait ResolveTableResponse {
  def getPath: String
  def getVersion: Long
  def getProtocol: Option[Protocol]
  def getMetadata: Option[Metadata]
  def getSchemaString: Option[String]
}

trait GetCommitsResponse {
  def getCommits(): Seq[FileStatus]
}

/////////////////////////////
// CCv2Client interactions //
/////////////////////////////

trait CCv2Client {
  def resolveTable(tableName: String): ResolvedMetadata
}

///////////////////////////
// InMemoryCatalogClient //
///////////////////////////

class InMemoryCatalogClient extends CatalogClient {
  override def resolveTable(tableName: String): ResolveTableResponse = ???
  override def getCommits(): GetCommitsResponse = ???
}

////////////////////////
// InMemoryCCv2Client //
////////////////////////

class InMemoryCCv2Client extends CCv2Client {
  override def resolveTable(tableName: String): ResolvedMetadata = {

  }
}
