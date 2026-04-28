/*
 * Copyright (2026) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.delta

import org.apache.spark.sql.delta.actions.{Metadata, Protocol}
import org.apache.spark.sql.delta.catalog.DeltaTableV2
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.{DeltaSQLCommandTest, DeltaSQLTestUtils}
import org.apache.spark.sql.delta.test.DeltaTestImplicits._

import org.apache.spark.SparkThrowable
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._

/**
 * Tests for the GeoSpatial table feature in the Delta Spark connector. Verifies:
 *  - Auto-enablement of the [[GeoSpatialTableFeature]] when a geospatial column is present.
 *  - Required protocol versions (3, 7).
 *  - SRID validation at commit time.
 *  - Removability contract (drop is rejected while geospatial columns still exist).
 */
class DeltaGeoSuite extends QueryTest
  with SharedSparkSession
  with DeltaSQLCommandTest
  with DeltaSQLTestUtils {

  // Default SRID used in Parquet/Iceberg/Delta specs (OGC:CRS84).
  private val DefaultSrid = 4326

  private def metadataWithSchema(schema: StructType): Metadata = {
    Metadata(schemaString = schema.json)
  }

  test("containsGeoColumns detects top-level geometry and geography columns") {
    val schemaGeom = new StructType()
      .add("id", IntegerType)
      .add("g", GeometryType(DefaultSrid))
    val schemaGeog = new StructType()
      .add("id", IntegerType)
      .add("g", GeographyType(DefaultSrid))
    val schemaPlain = new StructType()
      .add("id", IntegerType)
      .add("s", StringType)

    assert(DeltaGeoSpatial.containsGeoColumns(schemaGeom))
    assert(DeltaGeoSpatial.containsGeoColumns(schemaGeog))
    assert(!DeltaGeoSpatial.containsGeoColumns(schemaPlain))
  }

  test("containsGeoColumns detects nested geospatial columns") {
    val nested = new StructType()
      .add("outer", new StructType().add("inner", GeometryType(DefaultSrid)))
    assert(DeltaGeoSpatial.containsGeoColumns(nested))

    val arr = new StructType().add("xs", ArrayType(GeometryType(DefaultSrid)))
    assert(DeltaGeoSpatial.containsGeoColumns(arr))

    val map = new StructType().add("m", MapType(StringType, GeographyType(DefaultSrid)))
    assert(DeltaGeoSpatial.containsGeoColumns(map))
  }

  test("findGeoColumnsRecursively returns the first matching type") {
    val schema = new StructType()
      .add("id", IntegerType)
      .add("g", GeographyType(DefaultSrid))
    val result = DeltaGeoSpatial.findGeoColumnsRecursively(schema)
    assert(result.isDefined)
    assert(result.get.isInstanceOf[GeographyType])
  }

  test("assertSridSupported passes for default fixed SRIDs") {
    val schema = new StructType()
      .add("g1", GeometryType(DefaultSrid))
      .add("g2", GeographyType(DefaultSrid))
    DeltaGeoSpatial.assertSridSupported(schema)
  }

  test("isGeoSpatialType returns true for geometry and geography") {
    assert(DeltaGeoSpatial.isGeoSpatialType(GeometryType(DefaultSrid)))
    assert(DeltaGeoSpatial.isGeoSpatialType(GeographyType(DefaultSrid)))
    assert(!DeltaGeoSpatial.isGeoSpatialType(StringType))
  }

  test("isSupported checks both preview and stable features") {
    val protoNone = Protocol(3, 7)
    val protoStable = Protocol(3, 7).withFeature(GeoSpatialTableFeature)
    val protoPreview = Protocol(3, 7).withFeature(GeoSpatialPreviewTableFeature)
    assert(!DeltaGeoSpatial.isSupported(protoNone))
    assert(DeltaGeoSpatial.isSupported(protoStable))
    assert(DeltaGeoSpatial.isSupported(protoPreview))
  }

  test("GeoSpatialTableFeature is a reader+writer feature with min protocol (3, 7)") {
    assert(GeoSpatialTableFeature.isReaderWriterFeature)
    assert(GeoSpatialTableFeature.minReaderVersion == 3)
    assert(GeoSpatialTableFeature.minWriterVersion == 7)
    assert(GeoSpatialTableFeature.name == "geospatial")
    assert(GeoSpatialPreviewTableFeature.name == "geospatial-dev")
  }

  test("Both GeoSpatial features are registered in allSupportedFeaturesMap") {
    assert(TableFeature.featureNameToFeature("geospatial").contains(GeoSpatialTableFeature))
    assert(TableFeature.featureNameToFeature("geospatial-dev")
      .contains(GeoSpatialPreviewTableFeature))
  }

  test("metadataRequiresFeatureToBeEnabled is true iff schema has geospatial columns") {
    val withGeo = metadataWithSchema(new StructType().add("g", GeometryType(DefaultSrid)))
    val withoutGeo = metadataWithSchema(new StructType().add("s", StringType))
    assert(GeoSpatialTableFeature.metadataRequiresFeatureToBeEnabled(
      Protocol(1, 2), withGeo, spark))
    assert(!GeoSpatialTableFeature.metadataRequiresFeatureToBeEnabled(
      Protocol(1, 2), withoutGeo, spark))
  }

  test("stable feature does not auto-enable when preview feature is already supported") {
    val withGeo = metadataWithSchema(new StructType().add("g", GeometryType(DefaultSrid)))
    val protocolWithPreview =
      Protocol(3, 7).withFeature(GeoSpatialPreviewTableFeature)
    assert(!GeoSpatialTableFeature.metadataRequiresFeatureToBeEnabled(
      protocolWithPreview, withGeo, spark))
  }

  test("automaticallyUpdateProtocolOfExistingTables returns true") {
    assert(GeoSpatialTableFeature.automaticallyUpdateProtocolOfExistingTables)
  }

  test("creating a table with a geometry column auto-enables GeoSpatial feature") {
    withTable("tbl") {
      sql(s"CREATE TABLE tbl(id INT, g GEOMETRY($DefaultSrid)) USING delta")
      val protocol = getProtocolForTable("tbl")
      assert(protocol.isFeatureSupported(GeoSpatialTableFeature),
        s"Expected GeoSpatialTableFeature in protocol but got: $protocol")
      assert(protocol.minReaderVersion >= GeoSpatialTableFeature.minReaderVersion)
      assert(protocol.minWriterVersion >= GeoSpatialTableFeature.minWriterVersion)
    }
  }

  test("creating a table without geospatial columns does not enable GeoSpatial feature") {
    withTable("tbl") {
      sql("CREATE TABLE tbl(id INT, s STRING) USING delta")
      val protocol = getProtocolForTable("tbl")
      assert(!protocol.isFeatureSupported(GeoSpatialTableFeature))
      assert(!protocol.isFeatureSupported(GeoSpatialPreviewTableFeature))
    }
  }

  test("ALTER TABLE ADD COLUMN with geography auto-enables GeoSpatial feature") {
    withTable("tbl") {
      sql("CREATE TABLE tbl(id INT) USING delta")
      assert(!getProtocolForTable("tbl").isFeatureSupported(GeoSpatialTableFeature))
      sql(s"ALTER TABLE tbl ADD COLUMN g GEOGRAPHY($DefaultSrid)")
      assert(getProtocolForTable("tbl").isFeatureSupported(GeoSpatialTableFeature))
    }
  }

  test("commit fails when geo column is added and preview conf is disabled") {
    withTable("tbl") {
      sql("CREATE TABLE tbl(id INT) USING delta")
      withSQLConf(DeltaSQLConf.DELTA_GEO_PREVIEW_ENABLED.key -> "false") {
        val ex = intercept[SparkThrowable] {
          sql(s"ALTER TABLE tbl ADD COLUMN g GEOMETRY($DefaultSrid)")
        }
        assert(ex.getErrorClass == "DELTA_GEOSPATIAL_NOT_SUPPORTED",
          s"Unexpected error class: ${ex.getErrorClass}")
      }
    }
  }

  test("DROP FEATURE is rejected while geospatial columns remain in the schema") {
    withTable("tbl") {
      sql(s"CREATE TABLE tbl(id INT, g GEOMETRY($DefaultSrid)) USING delta")
      assert(getProtocolForTable("tbl").isFeatureSupported(GeoSpatialTableFeature))

      val ex = intercept[DeltaAnalysisException] {
        sql(s"ALTER TABLE tbl DROP FEATURE ${GeoSpatialTableFeature.name}")
      }
      assert(ex.getErrorClass == "DELTA_CANNOT_DROP_GEOSPATIAL_FEATURE",
        s"Unexpected error class: ${ex.getErrorClass}")
      assert(ex.getMessage.contains("g"))
    }
  }

  test("validateDropInvariants returns true after geospatial columns are dropped") {
    withTable("tbl") {
      sql(
        s"""CREATE TABLE tbl(id INT, g GEOMETRY($DefaultSrid)) USING delta
           |TBLPROPERTIES('delta.columnMapping.mode' = 'name')""".stripMargin)
      val tableBefore = DeltaTableV2(spark, TableIdentifier("tbl"))
      assert(!GeoSpatialTableFeature.validateDropInvariants(
        tableBefore, tableBefore.initialSnapshot))

      sql("ALTER TABLE tbl DROP COLUMN g")
      val tableAfter = DeltaTableV2(spark, TableIdentifier("tbl"))
      assert(GeoSpatialTableFeature.validateDropInvariants(
        tableAfter, tableAfter.initialSnapshot))
    }
  }

  test("actionUsesFeature returns false for all actions (history protection disabled)") {
    val geoMeta = metadataWithSchema(new StructType().add("g", GeometryType(DefaultSrid)))
    val plainMeta = metadataWithSchema(new StructType().add("s", StringType))
    assert(!GeoSpatialTableFeature.actionUsesFeature(geoMeta))
    assert(!GeoSpatialTableFeature.actionUsesFeature(plainMeta))
    assert(!GeoSpatialPreviewTableFeature.actionUsesFeature(geoMeta))
  }
}
