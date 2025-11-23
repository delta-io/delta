package io.delta.kernel.spark.snapshot

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.types.StructType

object CatalogTableTestUtils {
  def createMockCatalogTable(catalogName: String): CatalogTable = {
    val identifier = TableIdentifier("test_table", Some("default"), Some(catalogName))
    CatalogTable(
      identifier = identifier,
      tableType = CatalogTableType.EXTERNAL,
      storage = CatalogStorageFormat.empty,
      schema = new StructType(),
      provider = Some("delta"))
  }
}
