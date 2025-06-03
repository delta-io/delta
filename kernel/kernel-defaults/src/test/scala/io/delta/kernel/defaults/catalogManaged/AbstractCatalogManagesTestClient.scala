package io.delta.kernel.defaults.catalogManaged

import io.delta.kernel.ResolvedTable

trait CatalogManagedTestClient {

  def createTableInCatalog(path: String): Unit

  def forceCommitToDeltaLog(path: String): Unit

  def forceCommitToCatalog(path: String): Unit

  def loadTable(
      path: String,
      versionToLoadOpt: Option[Long],
      timestampToLoadOpt: Option[Long],
      injectProtocolMetadata: Boolean): ResolvedTable
}
