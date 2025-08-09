package io.delta.kernel.defaults.catalogManaged.utils

import io.delta.kernel.internal.actions.Protocol
import io.delta.kernel.test.ActionUtils
import io.delta.kernel.types.{LongType, StructType}

trait CatalogManagedTestFixtures extends ActionUtils {
  val schema = new StructType().add("part1", LongType.LONG).add("col1", LongType.LONG)
  val partCols = Seq("part1")
  val metadata = testMetadata(schema, partCols)
  val protocol = new Protocol(3, 7)
  val partitionSize = 10
}
