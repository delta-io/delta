package io.delta.kernel.internal.clustering
import io.delta.kernel.internal.actions.{Format, Metadata}
import io.delta.kernel.internal.util.{ColumnMapping, VectorUtils}
import io.delta.kernel.internal.util.ColumnMapping.ColumnMappingMode.ID
import io.delta.kernel.types._
import org.scalatest.funsuite.AnyFunSuite

import scala.collection.JavaConverters._

import java.util.{Collections, Optional, UUID}

class ClusteringMetadataDomainSuite extends AnyFunSuite {
  private def createMetadata(schema: StructType) = new Metadata(
    UUID.randomUUID.toString,
    Optional.empty(),
    Optional.empty(),
    new Format,
    schema.toJson,
    schema,
    VectorUtils.buildArrayValue(Collections.emptyList(), StringType.STRING),
    Optional.empty(),
    VectorUtils.stringStringMapValue(Collections.emptyMap()))

  test("Correctly maps logical column names to physical column names") {
    val schema = new StructType()
      .add("id", IntegerType.INTEGER, true)
      .add("name", IntegerType.INTEGER, true)

    val metadata = ColumnMapping.updateColumnMappingMetadata(createMetadata(schema), ID, true)

    val clusteringMetadata = new ClusteringMetadataDomain(List("name").asJava, metadata.getSchema)

    assert(clusteringMetadata.toDomainMetadata.getDomain == "delta.clustering")
    assert(clusteringMetadata.getClusteringColumns.asScala == List("name"))
  }
}
