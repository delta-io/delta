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

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.types._

/**
 * Spark 4.2 GeoSpatial suite. The shared assertions live in [[DeltaGeoSuiteBase]]
 * (`scala-shims/spark-4.1-4.2`); this subclass adds the 4.2-only coverage.
 *
 * Spark 4.2's Parquet writer can encode `GeometryType`/`GeographyType`, so this suite adds
 * the end-to-end read/write CUJ (INSERT/UPDATE/DELETE, nested/array/map geo, MERGE WITH SCHEMA
 * EVOLUTION, CDF lifecycle, and CONVERT TO DELTA) that OSS Spark 4.1 cannot run.
 */
class DeltaGeoSuite extends DeltaGeoSuiteBase {

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
  // Change Data Feed lifecycle on geo data (Spark 4.2+ Parquet writer)
  // ---------------------------------------------------------------------------

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
}
