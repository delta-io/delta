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
import org.apache.spark.sql.delta.schema.SchemaMergingUtils
import org.apache.spark.sql.delta.shims.GeoTypesShim
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.{DeltaSQLCommandTest, DeltaSQLTestUtils}
import org.apache.spark.sql.delta.test.DeltaTestImplicits._

import org.apache.spark.SparkThrowable
import org.apache.spark.sql.{AnalysisException, QueryTest}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.st.{
  ST_AsBinary, ST_GeogFromWKB, ST_GeomFromWKB, ST_SetSrid, ST_Srid}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._

/**
 * Tests for the GeoSpatial table feature in the Delta Spark connector. Verifies:
 *  - Auto-enablement of the [[GeoSpatialTableFeature]] when a geospatial column is present.
 *  - Required protocol versions (3, 7).
 *  - SRID validation at commit time.
 *  - Removability contract (drop is rejected while geospatial columns still exist).
 *
 * Note: this suite lives under the spark-4.1 test shim directory because Spark's
 * `GeometryType` / `GeographyType` catalyst types were only added in Spark 4.1
 * (SPARK-53760). The GeoSpatial table feature is therefore a no-op on Spark 4.0.
 */
class DeltaGeoSuite extends QueryTest
  with SharedSparkSession
  with DeltaSQLCommandTest
  with DeltaSQLTestUtils {

  // Default SRID used in Parquet/Iceberg/Delta specs (OGC:CRS84).
  private val DefaultSrid = 4326

  // Pre-computed little-endian WKB hex literals for a few well-known points. WKB layout:
  //   byteOrder (1 byte, 0x01 = little-endian)
  //   wkbType (4 bytes, 0x00000001 LE = Point)
  //   X (8 bytes, IEEE 754 double LE)
  //   Y (8 bytes, IEEE 754 double LE)
  // Used in SQL via `ST_GeomFromWKB(X'<hex>', srid)`. OSS Spark exposes ST_GeomFromWKB but
  // not ST_GeomFromText, so all geo literals in these tests are constructed via WKB.
  private val PointZeroZeroWkb = "010100000000000000000000000000000000000000"
  private val PointOneOneWkb = "0101000000000000000000F03F000000000000F03F"
  private val PointTwoTwoWkb = "010100000000000000000000400000000000000040"

  /** Convert a byte array to an uppercase hex string for direct comparison with WKB literals. */
  private def bytesToHex(bytes: Array[Byte]): String =
    bytes.map(b => f"${b & 0xff}%02X").mkString

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

  test("DeltaDataSource accepts GeoSpatial types via supportsDataType") {
    val ds = new sources.DeltaDataSource
    assert(ds.supportsDataType(GeometryType(DefaultSrid)))
    assert(ds.supportsDataType(GeographyType(DefaultSrid)))
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

  test("PartitionUtils rejects GeoSpatial type as a partition column") {
    // Geometry / Geography are not orderable and thus cannot be partition columns. Delta's
    // `util/PartitionUtils.validatePartitionColumn` is the write-path backstop; the catalog
    // (DDL) path goes through Spark's own `PartitioningUtils` which is out of Delta's hands.
    val schema = new StructType()
      .add("id", IntegerType)
      .add("g", GeometryType(DefaultSrid))
    val ex = intercept[DeltaAnalysisException] {
      util.PartitionUtils.validatePartitionColumn(schema, Seq("g"), caseSensitive = false)
    }
    assert(ex.getErrorClass == "DELTA_INVALID_PARTITION_COLUMN_TYPE",
      s"Unexpected error class: ${ex.getErrorClass}")
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

  test("read fails when table has geo column and preview conf is disabled") {
    // Covers the read-path guard wired through [[ProtocolMetadataAdapterV1.assertTableReadable]]:
    // a `SELECT *` against a table with a geo column triggers DeltaParquetFileFormat
    // construction, which invokes [[DeltaGeoSpatial.assertTableReadable]] and throws when the
    // preview config is off.
    withTable("tbl") {
      sql(s"CREATE TABLE tbl(id INT, g GEOMETRY($DefaultSrid)) USING delta")
      withSQLConf(DeltaSQLConf.DELTA_GEO_PREVIEW_ENABLED.key -> "false") {
        val ex = intercept[SparkThrowable] {
          sql("SELECT * FROM tbl").collect()
        }
        assert(ex.getErrorClass == "DELTA_GEOSPATIAL_NOT_SUPPORTED",
          s"Unexpected error class: ${ex.getErrorClass}")
      }
    }
  }

  test("assertMetadata refuses a geo column in a committed Metadata action when preview is off") {
    // Covers the inline guard in [[OptimisticTransactionImpl.assertMetadata]].
    // The standard DDL path hits [[GeoSpatialTableFeature.metadataRequiresFeatureToBeEnabled]]
    // first - see the test above - so we exercise the inline guard by committing a Metadata
    // action directly, which routes through `prepareCommit -> updateMetadataAndProtocolWith-
    // RequiredFeatures -> assertMetadata` before the table-feature gate runs.
    withTable("tbl") {
      sql("CREATE TABLE tbl(id INT) USING delta")
      withSQLConf(DeltaSQLConf.DELTA_GEO_PREVIEW_ENABLED.key -> "false") {
        val log = DeltaLog.forTable(spark, TableIdentifier("tbl"))
        val txn = log.startTransaction()
        val geoSchema = new StructType()
          .add("id", IntegerType)
          .add("g", GeometryType(DefaultSrid))
        val geoMetadata = txn.snapshot.metadata.copy(schemaString = geoSchema.json)

        val ex = intercept[SparkThrowable] {
          txn.commit(Seq(geoMetadata), DeltaOperations.ManualUpdate)
        }
        assert(ex.getErrorClass == "UNSUPPORTED_DATATYPE",
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

  test("validateDropInvariants flips to true after dropping a struct containing nested geo") {
    // `validateDropInvariants` checks `containsGeoColumns` recursively, so a geo column
    // hidden inside a struct should block feature removal exactly like a top-level geo
    // column does. After the entire struct column is dropped, the table feature must
    // become droppable again. This protects against a regression where the drop-check
    // is reduced to a flat field scan instead of a recursive one.
    withTable("tbl") {
      sql(
        s"""CREATE TABLE tbl(id INT, outer STRUCT<inner: GEOMETRY($DefaultSrid)>)
           |USING delta
           |TBLPROPERTIES('delta.columnMapping.mode' = 'name')""".stripMargin)
      val tableBefore = DeltaTableV2(spark, TableIdentifier("tbl"))
      assert(!GeoSpatialTableFeature.validateDropInvariants(
        tableBefore, tableBefore.initialSnapshot),
        "Nested geo inside a struct should block feature removal")

      // Also confirm the user-facing DROP FEATURE rejection fires for nested geo.
      val ex = intercept[DeltaAnalysisException] {
        sql(s"ALTER TABLE tbl DROP FEATURE ${GeoSpatialTableFeature.name}")
      }
      assert(ex.getErrorClass == "DELTA_CANNOT_DROP_GEOSPATIAL_FEATURE",
        s"Unexpected error class: ${ex.getErrorClass}")

      sql("ALTER TABLE tbl DROP COLUMN outer")
      val tableAfter = DeltaTableV2(spark, TableIdentifier("tbl"))
      assert(GeoSpatialTableFeature.validateDropInvariants(
        tableAfter, tableAfter.initialSnapshot),
        "After dropping the struct column, feature removal should be allowed")
    }
  }

  test("actionUsesFeature returns false for all actions (history protection disabled)") {
    val geoMeta = metadataWithSchema(new StructType().add("g", GeometryType(DefaultSrid)))
    val plainMeta = metadataWithSchema(new StructType().add("s", StringType))
    assert(!GeoSpatialTableFeature.actionUsesFeature(geoMeta))
    assert(!GeoSpatialTableFeature.actionUsesFeature(plainMeta))
    assert(!GeoSpatialPreviewTableFeature.actionUsesFeature(geoMeta))
  }

  // ---------------------------------------------------------------------------
  // Geo schema type handling (Delta log JSON ser/de support)
  // ---------------------------------------------------------------------------

  test("Schema JSON round-trips a top-level geometry column") {
    val original = new StructType()
      .add("id", IntegerType)
      .add("g", GeometryType(DefaultSrid))
    val roundtripped = DataType.fromJson(original.json).asInstanceOf[StructType]
    assert(roundtripped === original)
    assert(roundtripped("g").dataType.isInstanceOf[GeometryType])
    assert(roundtripped("g").dataType.asInstanceOf[GeometryType].srid == DefaultSrid)
  }

  test("Schema JSON round-trips a top-level geography column") {
    val original = new StructType()
      .add("id", IntegerType)
      .add("g", GeographyType(DefaultSrid))
    val roundtripped = DataType.fromJson(original.json).asInstanceOf[StructType]
    assert(roundtripped === original)
    assert(roundtripped("g").dataType.isInstanceOf[GeographyType])
    assert(roundtripped("g").dataType.asInstanceOf[GeographyType].srid == DefaultSrid)
  }

  test("Schema JSON round-trips nested geo columns inside structs, arrays, and maps") {
    val nested = new StructType()
      .add("outer", new StructType().add("inner", GeometryType(DefaultSrid)))
      .add("arr", ArrayType(GeographyType(DefaultSrid)))
      .add("map", MapType(StringType, GeometryType(DefaultSrid)))
    val roundtripped = DataType.fromJson(nested.json).asInstanceOf[StructType]
    assert(roundtripped === nested)
  }

  test("assertSridSupported throws AnalysisException for unsupported SRID on Geometry") {
    // `GeometryType("ANY")` produces a type whose `srid == MIXED_SRID (-1)`. Spark accepts that
    // type at construction, but `GeometryType.isSridSupported(-1)` is false, so it's exactly the
    // gap that `DeltaGeoSpatial.assertSridSupported` is meant to catch. (Direct `GeometryType(-1)`
    // would now throw `ST_INVALID_SRID_VALUE` from the int constructor and never reach Delta.)
    val schema = new StructType().add("g", GeometryType("ANY"))
    val ex = intercept[AnalysisException] {
      DeltaGeoSpatial.assertSridSupported(schema)
    }
    assert(ex.getCondition == "DELTA_GEOSPATIAL_SRID_NOT_SUPPORTED")
  }

  test("assertSridSupported throws AnalysisException for unsupported SRID on Geography") {
    val schema = new StructType().add("g", GeographyType("ANY"))
    val ex = intercept[AnalysisException] {
      DeltaGeoSpatial.assertSridSupported(schema)
    }
    assert(ex.getCondition == "DELTA_GEOSPATIAL_SRID_NOT_SUPPORTED")
  }

  test("assertSridSupported is a no-op for the default supported SRID") {
    val schema = new StructType()
      .add("a", GeometryType(DefaultSrid))
      .add("b", GeographyType(DefaultSrid))
    // Should not throw.
    DeltaGeoSpatial.assertSridSupported(schema)
  }

  test("validateCommitActions rejects unsupported-SRID metadata even when preview is enabled") {
    val schema = new StructType().add("g", GeometryType("ANY"))
    val metadata = metadataWithSchema(schema)
    val protocol = Protocol(
      GeoSpatialTableFeature.minReaderVersion,
      GeoSpatialTableFeature.minWriterVersion)
      .merge(Protocol.forTableFeature(GeoSpatialTableFeature))
    val ex = intercept[AnalysisException] {
      DeltaGeoSpatial.validateCommitActions(spark, protocol, Seq(metadata))
    }
    assert(ex.getCondition == "DELTA_GEOSPATIAL_SRID_NOT_SUPPORTED")
  }

  // Negative-SRID regression pins. Delta's `assertSridSupported` delegates the
  // `isSridSupported` check to Spark catalyst (`GeometryType.isSridSupported` /
  // `GeographyType.isSridSupported`). These tests document the Spark-side contract Delta
  // relies on, so a future Spark change that accepts negative SRIDs cannot silently flip
  // Delta to accepting them too. Negative SRIDs are how the `"ANY"` mixed-SRID type
  // materialises (srid == MIXED_SRID == -1), which is the only path through which a
  // negative SRID can reach Delta - `GeometryType(-1)` is rejected by the catalyst int
  // constructor before any Delta code runs.
  test("Spark contract: negative SRIDs are not supported for Geometry or Geography") {
    assert(!GeometryType.isSridSupported(-1),
      "Spark contract changed: GeometryType.isSridSupported(-1) now returns true. " +
        "Delta's negative-SRID rejection relies on this returning false.")
    assert(!GeographyType.isSridSupported(-1),
      "Spark contract changed: GeographyType.isSridSupported(-1) now returns true. " +
        "Delta's negative-SRID rejection relies on this returning false.")
  }

  test("GeometryType(\"ANY\") materialises with srid == -1 (mixed SRID sentinel)") {
    // Pin the construction path: assertSridSupported relies on `GeometryType("ANY")`
    // producing a negative `srid` so the unsupported-SRID check below can fire. If this
    // ever changes, the assertSridSupported tests above would silently start passing for
    // a no-op reason.
    val geometryAnySrid = GeometryType("ANY").srid
    val geographyAnySrid = GeographyType("ANY").srid
    assert(geometryAnySrid == -1,
      s"""Expected GeometryType("ANY").srid == -1, got $geometryAnySrid""")
    assert(geographyAnySrid == -1,
      s"""Expected GeographyType("ANY").srid == -1, got $geographyAnySrid""")
  }

  test("assertSridSupported rejects nested geo columns with negative SRIDs") {
    // Defence-in-depth: a top-level scalar with a bad SRID is already tested; ensure
    // recursion through `typeExistsRecursively` also catches a bad SRID buried inside a
    // struct, since that's how the worst-case bug would manifest (a hidden negative SRID
    // sneaking past the JSON ser/de).
    val schema = new StructType()
      .add("outer", new StructType().add("inner", GeometryType("ANY")))
    val ex = intercept[AnalysisException] {
      DeltaGeoSpatial.assertSridSupported(schema)
    }
    assert(ex.getCondition == "DELTA_GEOSPATIAL_SRID_NOT_SUPPORTED")
  }

  // ---------------------------------------------------------------------------
  // DML command support and validation for geospatial columns
  // ---------------------------------------------------------------------------

  test("GeoTypesShim.geoExpressions contains the expected ST_ catalyst classes") {
    val expected = Set[Class[_]](
      classOf[ST_AsBinary],
      classOf[ST_GeogFromWKB],
      classOf[ST_GeomFromWKB],
      classOf[ST_SetSrid],
      classOf[ST_Srid])
    assert(GeoTypesShim.geoExpressions == expected)
  }

  test("AllowedUserProvidedExpressions whitelists the ST_ classes via the shim") {
    // The geo ST_* classes are appended to `checkConstraintExpressions` (not `expressions`),
    // so they are usable in CHECK constraints only and not in generated columns. The shim
    // contributes the five classes on top of the static `checkConstraintExpressions` Set.
    val whitelist = AllowedUserProvidedExpressions.checkConstraintExpressions
    assert(whitelist.contains(classOf[ST_AsBinary]))
    assert(whitelist.contains(classOf[ST_GeogFromWKB]))
    assert(whitelist.contains(classOf[ST_GeomFromWKB]))
    assert(whitelist.contains(classOf[ST_SetSrid]))
    assert(whitelist.contains(classOf[ST_Srid]))
  }

  test("failIfSchemaHasGeoColumn rejects schemas containing geometry") {
    val schema = new StructType()
      .add("id", IntegerType)
      .add("g", GeometryType(DefaultSrid))
    val ex = intercept[Throwable] {
      DeltaGeoSpatial.failIfSchemaHasGeoColumn(schema, "TEST OP")
    }
    assert(ex.getMessage.contains("TEST OP"))
  }

  test("failIfSchemaHasGeoColumn rejects nested geography columns") {
    val schema = new StructType()
      .add("outer", new StructType().add("inner", GeographyType(DefaultSrid)))
    val ex = intercept[Throwable] {
      DeltaGeoSpatial.failIfSchemaHasGeoColumn(schema, "TEST NESTED")
    }
    assert(ex.getMessage.contains("TEST NESTED"))
  }

  test("failIfSchemaHasGeoColumn is a no-op for non-geo schemas") {
    val schema = new StructType()
      .add("id", IntegerType)
      .add("s", StringType)
      .add("xs", ArrayType(IntegerType))
    // Should not throw.
    DeltaGeoSpatial.failIfSchemaHasGeoColumn(schema, "TEST")
  }

  test("ZORDER BY a geometry column is rejected with DELTA_OPERATION_NOT_SUPPORTED_FOR_DATATYPES") {
    withTable("tbl") {
      sql(s"CREATE TABLE tbl(id INT, g GEOMETRY($DefaultSrid)) USING delta")
      // No INSERT - `validateZorderByColumns` runs against the snapshot schema before any data
      // is touched. Inserting geo values would fail in OSS Spark's Parquet writer, which doesn't
      // (yet) know how to write GeometryType.
      val ex = intercept[AnalysisException] {
        sql("OPTIMIZE tbl ZORDER BY (g)")
      }
      assert(ex.getCondition == "DELTA_OPERATION_NOT_SUPPORTED_FOR_DATATYPES",
        s"Expected DELTA_OPERATION_NOT_SUPPORTED_FOR_DATATYPES, got: ${ex.getCondition}")
    }
  }

  test("Column default values are rejected on geometry columns at CREATE TABLE") {
    // `CREATE TABLE ... DEFAULT ...` goes through `CreateTable`, which is one of the ops
    // `OptimisticTransactionImpl.checkColumnDefaults` matches on. (Note: `ALTER TABLE ADD COLUMN
    // ... DEFAULT ...` is always rejected by Spark with
    // `WRONG_COLUMN_DEFAULTS_FOR_DELTA_ALTER_TABLE_ADD_COLUMN_NOT_SUPPORTED` regardless of type
    // and never reaches the Delta-side check.)
    //
    // Use a unique table name + explicit LOCATION inside a temp dir: the CREATE TABLE is
    // expected to throw, but the catalog/Hadoop FS will already have created the table
    // directory by the time `checkColumnDefaults` fires, and the default `withTable` cleanup
    // (just `DROP TABLE IF EXISTS`) would leave that directory behind because the table was
    // never actually registered. Pointing LOCATION at a `withTempDir` path ensures it is
    // cleaned up regardless, so subsequent tests can re-create their own `tbl`.
    withTempDir { tempDir =>
      withTable("tbl_geo_default_create") {
        val ex = intercept[AnalysisException] {
          sql(s"CREATE TABLE tbl_geo_default_create(g GEOMETRY($DefaultSrid) DEFAULT NULL) " +
            s"USING delta LOCATION '${tempDir.getAbsolutePath}/t' " +
            "TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported')")
        }
        assert(ex.getCondition == "DELTA_OPERATION_NOT_SUPPORTED_FOR_DATATYPES",
          s"Expected DELTA_OPERATION_NOT_SUPPORTED_FOR_DATATYPES, got: ${ex.getCondition}")
      }
    }
  }

  test("Column default values are rejected on geometry columns via ALTER COLUMN SET DEFAULT") {
    // `ALTER TABLE ... ALTER COLUMN ... SET DEFAULT ...` goes through `ChangeColumn`, which is
    // also matched by `checkColumnDefaults`.
    withTable("tbl") {
      sql(s"CREATE TABLE tbl(g GEOMETRY($DefaultSrid)) USING delta " +
        "TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported')")
      val ex = intercept[AnalysisException] {
        sql("ALTER TABLE tbl ALTER COLUMN g SET DEFAULT NULL")
      }
      assert(ex.getCondition == "DELTA_OPERATION_NOT_SUPPORTED_FOR_DATATYPES",
        s"Expected DELTA_OPERATION_NOT_SUPPORTED_FOR_DATATYPES, got: ${ex.getCondition}")
    }
  }

  test("Column default on a STRUCT containing a geo field is rejected at CREATE TABLE") {
    // `checkColumnDefaults` recurses through the column's data type via
    // `typeExistsRecursively`, so a DEFAULT on a top-level struct column whose nested
    // field is geo must also be rejected. This catches the regression where the default
    // check stops at the top-level field type and ignores nested geo.
    withTempDir { tempDir =>
      withTable("tbl_geo_default_nested") {
        val ex = intercept[AnalysisException] {
          sql(s"CREATE TABLE tbl_geo_default_nested(" +
            s"s STRUCT<inner: GEOMETRY($DefaultSrid)> DEFAULT NULL) " +
            s"USING delta LOCATION '${tempDir.getAbsolutePath}/t' " +
            "TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported')")
        }
        assert(ex.getCondition == "DELTA_OPERATION_NOT_SUPPORTED_FOR_DATATYPES",
          s"Expected DELTA_OPERATION_NOT_SUPPORTED_FOR_DATATYPES, got: ${ex.getCondition}")
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Universal Format (Hudi) compatibility
  //
  // `UniversalFormat.enforceHudiDependencies` rejects any schema containing a geo type
  // when UniForm (Hudi) is enabled, throwing `DELTA_UNIVERSAL_FORMAT_VIOLATION`. These
  // tests cover that code path on both the create-time and alter-time entry points.
  // ---------------------------------------------------------------------------

  test("UniForm (Hudi) rejects creating a table with a geometry column") {
    withTable("tbl") {
      val ex = intercept[DeltaUnsupportedOperationException] {
        sql(s"CREATE TABLE tbl(id INT, g GEOMETRY($DefaultSrid)) USING delta " +
          "TBLPROPERTIES(" +
          "'delta.universalFormat.enabledFormats' = 'hudi', " +
          "'delta.enableDeletionVectors' = 'false'" +
          ")")
      }
      assert(ex.getErrorClass == "DELTA_UNIVERSAL_FORMAT_VIOLATION",
        s"Expected DELTA_UNIVERSAL_FORMAT_VIOLATION, got: ${ex.getErrorClass}")
      assert(ex.getMessage.toLowerCase(java.util.Locale.ROOT).contains("geometry"),
        s"Expected message to mention GeometryType, got: ${ex.getMessage}")
    }
  }

  test("UniForm (Hudi) rejects creating a table with a geography column") {
    withTable("tbl") {
      val ex = intercept[DeltaUnsupportedOperationException] {
        sql(s"CREATE TABLE tbl(id INT, g GEOGRAPHY($DefaultSrid)) USING delta " +
          "TBLPROPERTIES(" +
          "'delta.universalFormat.enabledFormats' = 'hudi', " +
          "'delta.enableDeletionVectors' = 'false'" +
          ")")
      }
      assert(ex.getErrorClass == "DELTA_UNIVERSAL_FORMAT_VIOLATION",
        s"Expected DELTA_UNIVERSAL_FORMAT_VIOLATION, got: ${ex.getErrorClass}")
      assert(ex.getMessage.toLowerCase(java.util.Locale.ROOT).contains("geography"),
        s"Expected message to mention GeographyType, got: ${ex.getMessage}")
    }
  }

  test("UniForm (Hudi) rejects geo nested inside a struct") {
    // Nested coverage: `findAnyTypeRecursively` is what `enforceHudiDependencies` calls,
    // so a geo column buried inside a struct must also be rejected by the Hudi block.
    withTable("tbl") {
      val ex = intercept[DeltaUnsupportedOperationException] {
        sql(s"CREATE TABLE tbl(s STRUCT<inner: GEOMETRY($DefaultSrid)>) USING delta " +
          "TBLPROPERTIES(" +
          "'delta.universalFormat.enabledFormats' = 'hudi', " +
          "'delta.enableDeletionVectors' = 'false'" +
          ")")
      }
      assert(ex.getErrorClass == "DELTA_UNIVERSAL_FORMAT_VIOLATION",
        s"Expected DELTA_UNIVERSAL_FORMAT_VIOLATION, got: ${ex.getErrorClass}")
    }
  }

  // ---------------------------------------------------------------------------
  // DML read/write on actual geo data (Spark 4.2+ Parquet writer)
  //
  // These tests exercise the end-to-end read/write CUJ: real `ST_GeomFromWKB` values get
  // encoded through the Parquet writer, persisted to Delta files, read back, and inspected
  // with `ST_AsBinary`. They are 4.2+ only because OSS Spark 4.1's `ParquetWriteSupport`
  // does not yet implement geo encoding (`UnsupportedDataType GeometryType(...)` in
  // `ParquetWriteSupport.makeWriter`); the 4.1 shim covers the same operations at the
  // validation layer only.
  // ---------------------------------------------------------------------------

  test("INSERT INTO geo table round-trips geo values through Delta + Parquet") {
    withTable("t_ins") {
      sql(s"CREATE TABLE t_ins(id INT, g GEOMETRY($DefaultSrid)) USING delta")
      sql(s"INSERT INTO t_ins VALUES " +
        s"(1, ST_GeomFromWKB(X'$PointZeroZeroWkb', $DefaultSrid)), " +
        s"(2, ST_GeomFromWKB(X'$PointOneOneWkb',  $DefaultSrid)), " +
        s"(3, NULL)")
      // Read back the WKB representation and verify shape + values.
      val rows = sql("SELECT id, ST_AsBinary(g) AS wkb FROM t_ins ORDER BY id")
        .collect()
        .map(r => (r.getInt(0), Option(r.getAs[Array[Byte]]("wkb")).map(bytesToHex)))
      assert(rows.toSeq === Seq(
        (1, Some(PointZeroZeroWkb)),
        (2, Some(PointOneOneWkb)),
        (3, None)))
      assert(getProtocolForTable("t_ins").isFeatureSupported(GeoSpatialTableFeature))
    }
  }

  test("INSERT INTO ... SELECT preserves geo values across two Delta tables") {
    withTable("t_src", "t_dst") {
      sql(s"CREATE TABLE t_src(id INT, g GEOMETRY($DefaultSrid)) USING delta")
      sql(s"INSERT INTO t_src VALUES " +
        s"(1, ST_GeomFromWKB(X'$PointZeroZeroWkb', $DefaultSrid)), " +
        s"(2, ST_GeomFromWKB(X'$PointOneOneWkb',  $DefaultSrid))")
      sql(s"CREATE TABLE t_dst(id INT, g GEOMETRY($DefaultSrid)) USING delta")
      sql("INSERT INTO t_dst SELECT * FROM t_src")
      val rows = sql("SELECT id, ST_AsBinary(g) AS wkb FROM t_dst ORDER BY id")
        .collect()
        .map(r => (r.getInt(0), bytesToHex(r.getAs[Array[Byte]]("wkb"))))
      assert(rows.toSeq === Seq((1, PointZeroZeroWkb), (2, PointOneOneWkb)))
    }
  }

  test("UPDATE rewrites a geo column on matching rows") {
    withTable("t_upd") {
      sql(s"CREATE TABLE t_upd(id INT, g GEOMETRY($DefaultSrid)) USING delta")
      sql(s"INSERT INTO t_upd VALUES " +
        s"(1, ST_GeomFromWKB(X'$PointZeroZeroWkb', $DefaultSrid)), " +
        s"(2, ST_GeomFromWKB(X'$PointOneOneWkb',  $DefaultSrid))")
      sql(s"UPDATE t_upd SET g = ST_GeomFromWKB(X'$PointTwoTwoWkb', $DefaultSrid) WHERE id = 1")
      val rows = sql("SELECT id, ST_AsBinary(g) AS wkb FROM t_upd ORDER BY id")
        .collect()
        .map(r => (r.getInt(0), bytesToHex(r.getAs[Array[Byte]]("wkb"))))
      assert(rows.toSeq === Seq((1, PointTwoTwoWkb), (2, PointOneOneWkb)))
    }
  }

  test("DELETE removes rows from a geo table") {
    withTable("t_del") {
      sql(s"CREATE TABLE t_del(id INT, g GEOMETRY($DefaultSrid)) USING delta")
      sql(s"INSERT INTO t_del VALUES " +
        s"(1, ST_GeomFromWKB(X'$PointZeroZeroWkb', $DefaultSrid)), " +
        s"(2, ST_GeomFromWKB(X'$PointOneOneWkb',  $DefaultSrid)), " +
        s"(3, ST_GeomFromWKB(X'$PointTwoTwoWkb',  $DefaultSrid))")
      sql("DELETE FROM t_del WHERE id = 2")
      val ids = sql("SELECT id FROM t_del ORDER BY id").collect().map(_.getInt(0)).toSeq
      assert(ids === Seq(1, 3))
    }
  }

  test("Nested geo column inside a struct round-trips through write/read") {
    withTable("t_nested") {
      sql(s"CREATE TABLE t_nested(id INT, s STRUCT<inner: GEOMETRY($DefaultSrid)>) USING delta")
      sql(s"INSERT INTO t_nested VALUES " +
        s"(1, named_struct('inner', ST_GeomFromWKB(X'$PointOneOneWkb', $DefaultSrid)))")
      val rows = sql("SELECT id, ST_AsBinary(s.inner) AS wkb FROM t_nested ORDER BY id")
        .collect()
        .map(r => (r.getInt(0), bytesToHex(r.getAs[Array[Byte]]("wkb"))))
      assert(rows.toSeq === Seq((1, PointOneOneWkb)))
      // Auto-enablement should also fire for nested geo columns.
      assert(getProtocolForTable("t_nested").isFeatureSupported(GeoSpatialTableFeature))
    }
  }

  test("INSERT INTO ARRAY<GEOMETRY> round-trips geo values through Delta + Parquet") {
    // Coverage parity with the struct-of-geo case: a top-level ARRAY column whose
    // element is geo must also write + read back correctly.
    withTable("t_arr") {
      sql(s"CREATE TABLE t_arr(id INT, xs ARRAY<GEOMETRY($DefaultSrid)>) USING delta")
      sql(s"INSERT INTO t_arr VALUES " +
        s"(1, ARRAY(" +
        s"ST_GeomFromWKB(X'$PointZeroZeroWkb', $DefaultSrid), " +
        s"ST_GeomFromWKB(X'$PointOneOneWkb',  $DefaultSrid)))")
      val rows = sql(
          "SELECT id, transform(xs, x -> ST_AsBinary(x)) AS wkbs FROM t_arr ORDER BY id")
        .collect()
        .map { r =>
          val wkbs = r.getSeq[Array[Byte]](1).map(bytesToHex)
          (r.getInt(0), wkbs.toSeq)
        }
      assert(rows.toSeq === Seq((1, Seq(PointZeroZeroWkb, PointOneOneWkb))))
      assert(getProtocolForTable("t_arr").isFeatureSupported(GeoSpatialTableFeature))
    }
  }

  test("INSERT INTO MAP<STRING, GEOGRAPHY> round-trips geo values through Delta + Parquet") {
    // Coverage parity with array-of-geo: a MAP whose value type is geo must write + read
    // back correctly. Uses GEOGRAPHY to give the map test a non-Geometry geo type.
    withTable("t_map") {
      sql(s"CREATE TABLE t_map(id INT, m MAP<STRING, GEOGRAPHY($DefaultSrid)>) USING delta")
      sql(s"INSERT INTO t_map VALUES " +
        s"(1, MAP('a', ST_GeogFromWKB(X'$PointZeroZeroWkb')))")
      val rows = sql("SELECT id, ST_AsBinary(m['a']) AS wkb FROM t_map ORDER BY id")
        .collect()
        .map(r => (r.getInt(0), bytesToHex(r.getAs[Array[Byte]]("wkb"))))
      assert(rows.toSeq === Seq((1, PointZeroZeroWkb)))
      assert(getProtocolForTable("t_map").isFeatureSupported(GeoSpatialTableFeature))
    }
  }

  // ---------------------------------------------------------------------------
  // DML read/write on GEOGRAPHY values (Spark 4.2+ Parquet writer)
  //
  // Parity coverage with the GEOMETRY suite above: the DML path is unified, but the
  // Geography type has its own Spark catalyst class and its own Parquet logical
  // annotation, so the same operations need to be confirmed end-to-end against
  // GeographyType too.
  // ---------------------------------------------------------------------------

  test("INSERT INTO geography table round-trips geog values through Delta + Parquet") {
    withTable("t_geog_ins") {
      sql(s"CREATE TABLE t_geog_ins(id INT, g GEOGRAPHY($DefaultSrid)) USING delta")
      sql(s"INSERT INTO t_geog_ins VALUES " +
        s"(1, ST_GeogFromWKB(X'$PointZeroZeroWkb')), " +
        s"(2, ST_GeogFromWKB(X'$PointOneOneWkb')), " +
        s"(3, NULL)")
      val rows = sql("SELECT id, ST_AsBinary(g) AS wkb FROM t_geog_ins ORDER BY id")
        .collect()
        .map(r => (r.getInt(0), Option(r.getAs[Array[Byte]]("wkb")).map(bytesToHex)))
      assert(rows.toSeq === Seq(
        (1, Some(PointZeroZeroWkb)),
        (2, Some(PointOneOneWkb)),
        (3, None)))
      assert(getProtocolForTable("t_geog_ins").isFeatureSupported(GeoSpatialTableFeature))
      // Sanity: the persisted schema must round-trip as GeographyType, not GeometryType.
      val schema = DeltaLog.forTable(spark, TableIdentifier("t_geog_ins")).update().metadata.schema
      assert(schema("g").dataType.isInstanceOf[GeographyType])
    }
  }

  test("UPDATE rewrites a geography column on matching rows") {
    withTable("t_geog_upd") {
      sql(s"CREATE TABLE t_geog_upd(id INT, g GEOGRAPHY($DefaultSrid)) USING delta")
      sql(s"INSERT INTO t_geog_upd VALUES " +
        s"(1, ST_GeogFromWKB(X'$PointZeroZeroWkb')), " +
        s"(2, ST_GeogFromWKB(X'$PointOneOneWkb'))")
      sql(s"UPDATE t_geog_upd SET g = ST_GeogFromWKB(X'$PointTwoTwoWkb') " +
        s"WHERE id = 1")
      val rows = sql("SELECT id, ST_AsBinary(g) AS wkb FROM t_geog_upd ORDER BY id")
        .collect()
        .map(r => (r.getInt(0), bytesToHex(r.getAs[Array[Byte]]("wkb"))))
      assert(rows.toSeq === Seq((1, PointTwoTwoWkb), (2, PointOneOneWkb)))
    }
  }

  test("DELETE removes rows from a geography table") {
    withTable("t_geog_del") {
      sql(s"CREATE TABLE t_geog_del(id INT, g GEOGRAPHY($DefaultSrid)) USING delta")
      sql(s"INSERT INTO t_geog_del VALUES " +
        s"(1, ST_GeogFromWKB(X'$PointZeroZeroWkb')), " +
        s"(2, ST_GeogFromWKB(X'$PointOneOneWkb')), " +
        s"(3, ST_GeogFromWKB(X'$PointTwoTwoWkb'))")
      sql("DELETE FROM t_geog_del WHERE id = 2")
      val ids = sql("SELECT id FROM t_geog_del ORDER BY id").collect().map(_.getInt(0)).toSeq
      assert(ids === Seq(1, 3))
    }
  }

  test("MERGE WITH SCHEMA EVOLUTION adds a new geography column from source to target") {
    withTable("src_geog", "tgt_geog") {
      sql(s"CREATE TABLE src_geog(id INT, g GEOGRAPHY($DefaultSrid)) USING delta")
      sql(s"INSERT INTO src_geog VALUES " +
        s"(1, ST_GeogFromWKB(X'$PointOneOneWkb')), " +
        s"(2, ST_GeogFromWKB(X'$PointTwoTwoWkb'))")
      sql("CREATE TABLE tgt_geog(id INT) USING delta")
      sql("INSERT INTO tgt_geog VALUES (1), (3)")

      sql(
        """MERGE WITH SCHEMA EVOLUTION INTO tgt_geog USING src_geog
          |  ON tgt_geog.id = src_geog.id
          |WHEN MATCHED THEN UPDATE SET *
          |WHEN NOT MATCHED THEN INSERT *""".stripMargin)

      val schema =
        DeltaLog.forTable(spark, TableIdentifier("tgt_geog")).update().metadata.schema
      assert(schema("g").dataType.isInstanceOf[GeographyType])
      assert(getProtocolForTable("tgt_geog").isFeatureSupported(GeoSpatialTableFeature))
      val rows = sql("SELECT id, ST_AsBinary(g) AS wkb FROM tgt_geog ORDER BY id")
        .collect()
        .map(r => (r.getInt(0), Option(r.getAs[Array[Byte]]("wkb")).map(bytesToHex)))
      assert(rows.toSeq === Seq(
        (1, Some(PointOneOneWkb)),
        (2, Some(PointTwoTwoWkb)),
        (3, None)))
    }
  }

  // ---------------------------------------------------------------------------
  // Schema evolution for geospatial columns through MERGE
  // ---------------------------------------------------------------------------

  test("Adding a new geo column via ALTER TABLE writes a Metadata action with the geo schema") {
    withTable("tbl") {
      sql("CREATE TABLE tbl(id INT, s STRING) USING delta")
      sql(s"ALTER TABLE tbl ADD COLUMN g GEOGRAPHY($DefaultSrid)")
      val deltaLog = DeltaLog.forTable(spark, TableIdentifier("tbl"))
      val schema = deltaLog.update().metadata.schema
      assert(schema("g").dataType.isInstanceOf[GeographyType])
    }
  }

  test("SchemaMergingUtils.mergeSchemas rejects geo columns with different SRIDs " +
    "(blocks SRID change / MERGE schema evolution with mismatched SRIDs)") {
    // Two schemas that agree on column name and broad type (geometry) but differ in SRID. The
    // geo guard in `SchemaMergingUtils.typeForImplicitCast` returns None for any geo-to-geo
    // mismatch, so the merge falls through and the field-merge wrapper rethrows as
    // `DELTA_FAILED_TO_MERGE_FIELDS` (whose cause is `DELTA_MERGE_INCOMPATIBLE_DATATYPE`). This
    // is the same path that fires on `ALTER TABLE ... ALTER COLUMN g TYPE GEOMETRY(<other srid>)`
    // and on `MERGE WITH SCHEMA EVOLUTION` when source/target SRIDs differ.
    val tableSchema = new StructType().add("g", GeometryType(DefaultSrid))
    val dataSchema = new StructType().add("g", GeometryType(0))
    val ex = intercept[DeltaAnalysisException] {
      SchemaMergingUtils.mergeSchemas(
        tableSchema,
        dataSchema,
        allowImplicitConversions = true)
    }
    assert(ex.getErrorClass == "DELTA_FAILED_TO_MERGE_FIELDS",
      s"Expected DELTA_FAILED_TO_MERGE_FIELDS, got: ${ex.getErrorClass}")
  }

  test("SchemaMergingUtils.mergeSchemas rejects geo->non-geo and non-geo->geo merges") {
    // The geo guard short-circuits whenever EITHER side is geo, so swapping a non-geo column
    // for a geo one (or vice versa) also fails to merge with `DELTA_FAILED_TO_MERGE_FIELDS`.
    val geoToString = new StructType().add("g", GeometryType(DefaultSrid))
    val stringToGeo = new StructType().add("g", StringType)
    val ex1 = intercept[DeltaAnalysisException] {
      SchemaMergingUtils.mergeSchemas(geoToString, stringToGeo, allowImplicitConversions = true)
    }
    assert(ex1.getErrorClass == "DELTA_FAILED_TO_MERGE_FIELDS")
    val ex2 = intercept[DeltaAnalysisException] {
      SchemaMergingUtils.mergeSchemas(stringToGeo, geoToString, allowImplicitConversions = true)
    }
    assert(ex2.getErrorClass == "DELTA_FAILED_TO_MERGE_FIELDS")
  }

  test("ALTER TABLE ALTER COLUMN ... TYPE between geo SRIDs is rejected") {
    // `ALTER TABLE ... ALTER COLUMN g TYPE GEOMETRY(<other srid>)` routes through
    // `AlterTableChangeColumnDeltaCommand`, which goes through schema merge. The geo guard
    // above blocks the change. No data needs to be written, so this works even on OSS Spark
    // 4.1 (whose Parquet writer cannot encode geo values).
    withTable("tbl") {
      sql(s"CREATE TABLE tbl(g GEOMETRY($DefaultSrid)) USING delta")
      val ex = intercept[AnalysisException] {
        sql("ALTER TABLE tbl ALTER COLUMN g TYPE GEOMETRY(0)")
      }
      assert(ex.getMessage.toUpperCase(java.util.Locale.ROOT).contains("GEOMETRY"),
        s"Expected the rejection message to mention GEOMETRY, got: ${ex.getMessage}")
    }
  }

  test("MERGE WITH SCHEMA EVOLUTION adds a new geo column from source to target") {
    // Real MERGE end-to-end. The source carries a geo column that the target does not yet
    // have; schema evolution adds the column and the matched/non-matched data is written
    // through the Parquet writer. This exercises the full read+write CUJ for MERGE on geo.
    withTable("src", "tgt") {
      sql(s"CREATE TABLE src(id INT, g GEOMETRY($DefaultSrid)) USING delta")
      sql(s"INSERT INTO src VALUES " +
        s"(1, ST_GeomFromWKB(X'$PointOneOneWkb', $DefaultSrid)), " +
        s"(2, ST_GeomFromWKB(X'$PointTwoTwoWkb', $DefaultSrid))")
      sql("CREATE TABLE tgt(id INT) USING delta")
      sql("INSERT INTO tgt VALUES (1), (3)")

      sql(
        """MERGE WITH SCHEMA EVOLUTION INTO tgt USING src ON tgt.id = src.id
          |WHEN MATCHED THEN UPDATE SET *
          |WHEN NOT MATCHED THEN INSERT *""".stripMargin)

      // Target table should now carry the geo column with the correct values.
      val schema = DeltaLog.forTable(spark, TableIdentifier("tgt")).update().metadata.schema
      assert(schema("g").dataType.isInstanceOf[GeometryType])
      assert(getProtocolForTable("tgt").isFeatureSupported(GeoSpatialTableFeature))
      val rows = sql("SELECT id, ST_AsBinary(g) AS wkb FROM tgt ORDER BY id")
        .collect()
        .map(r => (r.getInt(0), Option(r.getAs[Array[Byte]]("wkb")).map(bytesToHex)))
      // id=1 was matched: g comes from source.
      // id=2 was in source but not target: WHEN NOT MATCHED INSERT * adds it with source.g.
      // id=3 was in target but not source: it is preserved unchanged, with no g value.
      assert(rows.toSeq === Seq(
        (1, Some(PointOneOneWkb)),
        (2, Some(PointTwoTwoWkb)),
        (3, None)))
    }
  }

  test("MERGE WITH SCHEMA EVOLUTION rejects source/target SRID mismatch on the same column") {
    // Source and target both already have a geo column `g`, but with different SRIDs. Schema
    // merge fires `typeForImplicitCast(GeometryType(0), GeometryType(4326))` which returns
    // None (geo guard); the field-merge wrapper rethrows as `DELTA_FAILED_TO_MERGE_FIELDS`.
    withTable("src", "tgt") {
      sql(s"CREATE TABLE src(id INT, g GEOMETRY(0)) USING delta")
      sql("INSERT INTO src VALUES " +
        s"(1, ST_GeomFromWKB(X'$PointOneOneWkb', 0))")
      sql(s"CREATE TABLE tgt(id INT, g GEOMETRY($DefaultSrid)) USING delta")
      sql(s"INSERT INTO tgt VALUES " +
        s"(1, ST_GeomFromWKB(X'$PointZeroZeroWkb', $DefaultSrid))")

      val ex = intercept[DeltaAnalysisException] {
        sql(
          """MERGE WITH SCHEMA EVOLUTION INTO tgt USING src ON tgt.id = src.id
            |WHEN MATCHED THEN UPDATE SET *""".stripMargin)
      }
      assert(ex.getErrorClass == "DELTA_FAILED_TO_MERGE_FIELDS",
        s"Expected DELTA_FAILED_TO_MERGE_FIELDS, got: ${ex.getErrorClass}")
    }
  }

  // ---------------------------------------------------------------------------
  // Convert to Delta with geospatial compatibility
  // ---------------------------------------------------------------------------

  test("CONVERT TO DELTA fails when the source parquet schema contains geo types") {
    // End-to-end SQL test: build a real parquet table with a geometry column (and a row of
    // data, so the schema is durable on disk), then try to CONVERT TO DELTA. The validation
    // hook in `ConvertToDeltaCommandBase.validateConvert` should reject it before any
    // conversion happens. Spark 4.2 supports writing GeometryType through Parquet, so this
    // test is unconditional here; on Spark 4.1 the equivalent test lives as a unit-level
    // `failIfSchemaHasGeoColumn` call because the 4.1 Parquet writer cannot encode geo.
    withTable("src_parquet") {
      sql(s"CREATE TABLE src_parquet(id INT, g GEOMETRY($DefaultSrid)) USING parquet")
      sql(s"INSERT INTO src_parquet VALUES " +
        s"(1, ST_GeomFromWKB(X'$PointZeroZeroWkb', $DefaultSrid))")

      val ex = intercept[AnalysisException] {
        sql("CONVERT TO DELTA src_parquet")
      }
      assert(ex.getCondition == "DELTA_OPERATION_NOT_SUPPORTED_FOR_DATATYPES",
        s"Expected DELTA_OPERATION_NOT_SUPPORTED_FOR_DATATYPES, got: ${ex.getCondition}")
      assert(ex.getMessage.contains("CONVERT TO DELTA"),
        s"Expected message to mention CONVERT TO DELTA, got: ${ex.getMessage}")
    }
  }

  // ---------------------------------------------------------------------------
  // Change Data Feed support for geospatial columns
  // ---------------------------------------------------------------------------

  test("Enabling CDF on a table with a geo column does not throw at table creation") {
    withTable("tbl") {
      sql(s"CREATE TABLE tbl(id INT, g GEOMETRY($DefaultSrid)) USING delta " +
        "TBLPROPERTIES('delta.enableChangeDataFeed' = 'true')")
      val protocol = getProtocolForTable("tbl")
      // GeoSpatial feature must be supported alongside CDF.
      assert(protocol.isFeatureSupported(GeoSpatialTableFeature))
    }
  }

  test("CDF schema contains the geo column with original type and SRID") {
    withTable("tbl") {
      sql(s"CREATE TABLE tbl(id INT, g GEOMETRY($DefaultSrid)) USING delta " +
        "TBLPROPERTIES('delta.enableChangeDataFeed' = 'true')")
      val deltaLog = DeltaLog.forTable(spark, TableIdentifier("tbl"))
      val schema = deltaLog.update().metadata.schema
      val geoField = schema("g")
      assert(geoField.dataType.isInstanceOf[GeometryType])
      assert(geoField.dataType.asInstanceOf[GeometryType].srid == DefaultSrid)
    }
  }

  test("CDF lifecycle: INSERT/UPDATE/DELETE on geo column shows up in table_changes") {
    // Full CDF lifecycle test (Spark 4.2-only; needs the Parquet writer to support geo):
    //  v1: INSERT 2 rows
    //  v2: UPDATE one row -> CDF emits update_preimage + update_postimage
    //  v3: DELETE the other row -> CDF emits delete
    // Then read table_changes starting at version 1 and verify the geo values appear in
    // every change record with the correct change_type and the right WKB encoding.
    withTable("tbl") {
      sql(s"CREATE TABLE tbl(id INT, g GEOMETRY($DefaultSrid)) USING delta " +
        "TBLPROPERTIES('delta.enableChangeDataFeed' = 'true')")
      sql(s"INSERT INTO tbl VALUES " +
        s"(1, ST_GeomFromWKB(X'$PointZeroZeroWkb', $DefaultSrid)), " +
        s"(2, ST_GeomFromWKB(X'$PointOneOneWkb',  $DefaultSrid))")
      sql(s"UPDATE tbl SET g = ST_GeomFromWKB(X'$PointTwoTwoWkb', $DefaultSrid) WHERE id = 1")
      sql("DELETE FROM tbl WHERE id = 2")

      val cdf = spark.read.format("delta")
        .option("readChangeFeed", "true")
        .option("startingVersion", 1L)
        .table("tbl")
      // Sanity-check schema: data columns + CDF metadata columns.
      val cdfSchema = cdf.schema
      assert(cdfSchema("g").dataType.isInstanceOf[GeometryType])
      assert(cdfSchema.fieldNames.contains("_change_type"))
      assert(cdfSchema.fieldNames.contains("_commit_version"))

      val records = cdf
        .selectExpr("id", "ST_AsBinary(g) AS wkb", "_change_type")
        .collect()
        .map(r => (
          r.getInt(0),
          Option(r.getAs[Array[Byte]]("wkb")).map(bytesToHex),
          r.getString(2)))
        // Order across commits via change type + id to make assertion deterministic.
        .sortBy { case (id, _, ct) => (ct, id) }

      // Ordered by (_change_type, id) ascending: alphabetically "delete" < "insert" <
      // "update_postimage" < "update_preimage", which is what the .sortBy above yields.
      assert(records.toSeq === Seq(
        // delete on (id=2, POINT(1,1))
        (2, Some(PointOneOneWkb), "delete"),
        // insert v1
        (1, Some(PointZeroZeroWkb), "insert"),
        (2, Some(PointOneOneWkb), "insert"),
        // update v2: post and pre images for id=1
        (1, Some(PointTwoTwoWkb), "update_postimage"),
        (1, Some(PointZeroZeroWkb), "update_preimage")))
    }
  }

  test("CDF lifecycle: INSERT/UPDATE on a nested geo column shows up in table_changes") {
    // Same coverage as the top-level CDF lifecycle test above, but the geo column lives
    // inside a struct. Ensures CDF preimage/postimage capture works correctly when geo
    // is nested, since the write path goes through struct projection as well as Parquet
    // encoding.
    withTable("tbl") {
      sql(
        s"""CREATE TABLE tbl(id INT, s STRUCT<inner: GEOMETRY($DefaultSrid)>) USING delta
           |TBLPROPERTIES('delta.enableChangeDataFeed' = 'true')""".stripMargin)
      sql(s"INSERT INTO tbl VALUES " +
        s"(1, named_struct('inner', ST_GeomFromWKB(X'$PointZeroZeroWkb', $DefaultSrid)))")
      sql(s"UPDATE tbl SET s = named_struct(" +
        s"'inner', ST_GeomFromWKB(X'$PointTwoTwoWkb', $DefaultSrid)) WHERE id = 1")

      val cdf = spark.read.format("delta")
        .option("readChangeFeed", "true")
        .option("startingVersion", 1L)
        .table("tbl")
      val cdfSchema = cdf.schema
      val sField = cdfSchema("s").dataType.asInstanceOf[StructType]
      assert(sField("inner").dataType.isInstanceOf[GeometryType],
        s"Expected GeometryType inside CDF struct, got: ${sField("inner").dataType}")

      val records = cdf
        .selectExpr("id", "ST_AsBinary(s.inner) AS wkb", "_change_type")
        .collect()
        .map(r => (
          r.getInt(0),
          Option(r.getAs[Array[Byte]]("wkb")).map(bytesToHex),
          r.getString(2)))
        .sortBy { case (id, _, ct) => (ct, id) }

      // .sortBy yields _change_type ascending: "insert" < "update_postimage" <
      // "update_preimage".
      assert(records.toSeq === Seq(
        (1, Some(PointZeroZeroWkb), "insert"),
        (1, Some(PointTwoTwoWkb), "update_postimage"),
        (1, Some(PointZeroZeroWkb), "update_preimage")))
    }
  }

  // ---------------------------------------------------------------------------
  // Iceberg / UniForm / Hudi compatibility for geospatial columns
  // ---------------------------------------------------------------------------

  test("IcebergCompatV1 is rejected on a table with a geometry column") {
    // `CheckGeoSpatialTableFeatureDisabled` runs as part of `IcebergCompatV1.checks` and rejects
    // the metadata before any conversion happens. V1 has no `CheckTypeInV2AllowList`, so this
    // check is the only guard for geo columns under V1.
    withTable("tbl") {
      val ex = intercept[DeltaUnsupportedOperationException] {
        sql(s"CREATE TABLE tbl(id INT, g GEOMETRY($DefaultSrid)) USING delta " +
          "TBLPROPERTIES('delta.enableIcebergCompatV1' = 'true', " +
          "'delta.columnMapping.mode' = 'name')")
      }
      assert(ex.getErrorClass == "DELTA_ICEBERG_COMPAT_VIOLATION.UNSUPPORTED_DATA_TYPE",
        s"Unexpected error class: ${ex.getErrorClass}")
    }
  }

  test("IcebergCompatV2 is rejected on a table with a geography column") {
    // Same check as above, registered in `IcebergCompatV2.checks`. V2 also has
    // `CheckTypeInV2AllowList` which would reject geo with a more generic error, but the
    // dedicated geo check is ordered to fire first and produces the same error class.
    withTable("tbl") {
      val ex = intercept[DeltaUnsupportedOperationException] {
        sql(s"CREATE TABLE tbl(id INT, g GEOGRAPHY($DefaultSrid)) USING delta " +
          "TBLPROPERTIES('delta.enableIcebergCompatV2' = 'true', " +
          "'delta.columnMapping.mode' = 'name')")
      }
      assert(ex.getErrorClass == "DELTA_ICEBERG_COMPAT_VIOLATION.UNSUPPORTED_DATA_TYPE",
        s"Unexpected error class: ${ex.getErrorClass}")
    }
  }

  test("Hudi UniForm on a table with a geometry column throws DELTA_UNIVERSAL_FORMAT_VIOLATION") {
    // Covers the geo branch added to `UniversalFormat.enforceHudiDependencies`. The geo type
    // is not representable in Hudi's schema, so we reject at commit time.
    withTable("tbl") {
      val ex = intercept[DeltaUnsupportedOperationException] {
        sql(s"CREATE TABLE tbl(id INT, g GEOMETRY($DefaultSrid)) USING delta " +
          "TBLPROPERTIES('delta.universalFormat.enabledFormats' = 'hudi', " +
          "'delta.enableDeletionVectors' = 'false', " +
          "'delta.columnMapping.mode' = 'name')")
      }
      assert(ex.getErrorClass == "DELTA_UNIVERSAL_FORMAT_VIOLATION",
        s"Unexpected error class: ${ex.getErrorClass}")
      assert(ex.getMessage.contains("hudi"),
        s"Expected message to mention hudi, got: ${ex.getMessage}")
    }
  }
}
