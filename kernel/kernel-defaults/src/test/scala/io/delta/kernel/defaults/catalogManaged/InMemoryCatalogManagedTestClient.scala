package io.delta.kernel.defaults.catalogManaged

import io.delta.kernel.ResolvedTable

trait InMemoryCatalogManagedTestClient extends CatalogManagedTestClient {

  override def createTableInCatalog(path: String): Unit = ???

  override def forceCommitToDeltaLog(path: String): Unit = ???

  override def forceCommitToCatalog(path: String): Unit = ???

  override def loadTable(path: String, versionToLoadOpt: Option[Long], timestampToLoadOpt: Option[Long], injectProtocolMetadata: Boolean): ResolvedTable = ???
}

