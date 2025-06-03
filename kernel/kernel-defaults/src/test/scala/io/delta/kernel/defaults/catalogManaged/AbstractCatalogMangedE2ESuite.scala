package io.delta.kernel.defaults.catalogManaged

import io.delta.kernel.defaults.utils.TestUtils
import io.delta.kernel.engine.Engine
import org.scalatest.funsuite.AnyFunSuite

class InMemoryCatalogManagedE2ESuite
    extends AbstractCatalogMangedE2ESuite
    with InMemoryCatalogManagedTestClient {
  override val engine: Engine = defaultEngine
}

trait AbstractCatalogMangedE2ESuite
    extends AnyFunSuite
    with CatalogManagedTestClient
    with TestUtils {
  val engine: Engine

  // create protocol
  // create metadata
  // create getData function that takes in the commit number and returns i*100, (i+1)*100, etc.

  test("basic read") {
    withTempDir { path =>
      createTableInCatalog(...)
      forceCommitToDeltaLog(...)
      forceCommitToDeltaLog(...)
      forceCommitToCatalog(...)

      val resolvedTable = loadTable(path)


    }
    withTempDir
    createTableInCatalog()
    addToDeltaLog()
    addToCatalog()


  }
}
