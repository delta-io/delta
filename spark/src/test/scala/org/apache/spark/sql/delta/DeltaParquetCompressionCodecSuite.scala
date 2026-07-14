/*
 * Copyright (2021) The Delta Lake Project Authors.
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

import java.util.Locale

import scala.collection.JavaConverters._

import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.hadoop.util.HadoopInputFile

import org.apache.spark.sql.{DataFrameWriter, QueryTest, Row}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.ExternalCatalogUtils
import org.apache.spark.sql.functions.current_timestamp
import org.apache.spark.sql.test.SharedSparkSession

/**
 * Tests for the `delta.parquet.compression.codec` table property defined in the Delta protocol:
 * https://github.com/delta-io/delta/blob/master/PROTOCOL.md#table-properties
 */
class DeltaParquetCompressionCodecSuite
    extends QueryTest
    with SharedSparkSession
    with DeltaSQLCommandTest {
  import testImplicits._

  private val tablePropertyKey: String = DeltaConfigs.PARQUET_COMPRESSION_CODEC.key
  private val writerOptionKey: String = DeltaOptions.COMPRESSION
  private val sessionCodecKey: String = "spark.sql.parquet.compression.codec"

  /**
   * Reads the compression codecs from all data Parquet files of the given Delta table.
   */
  private def collectCodecsOfDataFiles(table: String): Set[CompressionCodecName] = {
    val catalogTable = spark.sessionState.catalog.getTableMetadata(TableIdentifier(table))
    val files = DeltaLog
      .forTable(spark, catalogTable)
      .update()
      .allFiles
      .collect()
    assert(files.nonEmpty, "Expected at least one data file in the table")

    files.flatMap { addFile =>
      val pathStr = ExternalCatalogUtils.unescapePathName(
        s"${catalogTable.location}/${addFile.path}")
      readParquetFileCodecs(new Path(pathStr))
    }.toSet
  }

  /**
   * Reads the compression codecs from a single Parquet file (any column-chunk in any row-group).
   */
  private def readParquetFileCodecs(path: Path): Set[CompressionCodecName] = {
    val file = HadoopInputFile.fromPath(path, new Configuration())
    val reader = ParquetFileReader.open(file)
    try {
      reader.getFooter.getBlocks.asScala.flatMap { block =>
        block.getColumns.asScala.map(_.getCodec)
      }.toSet
    } finally {
      reader.close()
    }
  }

  private def writeDF: DataFrameWriter[Row] = {
    (1 to 100)
      .map(i => (i, i.toString))
      .toDF("c0", "c1")
      .withColumn("c2", current_timestamp())
      .repartition(2)
      .write
      .format("delta")
  }

  private def getProperties(tableName: String): Map[String, String] =
    sql(s"describe detail $tableName")
      .collect()
      .head
      .getAs[Map[String, String]]("properties")

  /** Maps a codec name (as accepted by the Delta property) to the expected ParquetCodec name. */
  private def expectedCodec(name: String): CompressionCodecName =
    name.toLowerCase(Locale.ROOT) match {
    case "uncompressed" | "none" => CompressionCodecName.UNCOMPRESSED
    case "snappy" => CompressionCodecName.SNAPPY
    case "gzip" => CompressionCodecName.GZIP
    case "lz4" => CompressionCodecName.LZ4
    case "lz4_raw" => CompressionCodecName.LZ4_RAW
    case "zstd" => CompressionCodecName.ZSTD
    case other => fail(s"Unexpected codec name: $other")
  }

  test("DeltaConfig: accepts every protocol-defined codec, case-insensitively") {
    for {
      codec <- Seq("uncompressed", "none", "snappy", "gzip", "lz4", "lz4_raw", "zstd")
      casing <- Seq[String => String](identity, _.toUpperCase(Locale.ROOT), _.capitalize)
    } {
      val pair = DeltaConfigs.PARQUET_COMPRESSION_CODEC(casing(codec))
      assert(pair._1 == tablePropertyKey)
      assert(pair._2 == casing(codec))
    }
  }

  test("DeltaConfig: rejects invalid codecs") {
    val ex = intercept[IllegalArgumentException] {
      DeltaConfigs.PARQUET_COMPRESSION_CODEC("invalid")
    }
    assert(ex.getMessage.contains(tablePropertyKey))
  }

  for {
    (desc, config, expected) <- Seq(
      ("returns lowercase regardless of stored case",
        Map(tablePropertyKey -> "GZIP"), Some("gzip")),
      ("returns None when the property is absent",
        Map.empty[String, String], Option.empty[String]))
  } {
    test(s"DeltaConfig: fromMetaData $desc") {
      val metadata = actions.Metadata(configuration = config)
      assert(DeltaConfigs.PARQUET_COMPRESSION_CODEC.fromMetaData(metadata) == expected)
    }
  }

  /**
   * A single-write data-file compression scenario: configure the table property, session conf,
   * and/or writer option as specified, append data, then assert the codec(s) of the resulting
   * data files. These cases are effectively the same test with different parameters, so they are
   * expressed as data rather than written out one by one.
   *
   * @param tableProperty        codec to set via `TBLPROPERTIES` at CREATE time, if any.
   * @param sessionCodec         value for `spark.sql.parquet.compression.codec`, if any.
   * @param writerOption         `(key, value)` DataFrame writer option to set, if any.
   * @param expectedStoredCodec  when set, assert the stored table property equals this value.
   * @param verifyCodecs         assertion on the set of codecs found across the data files.
   */
  private case class DataFileCodecCase(
      name: String,
      tableProperty: Option[String] = None,
      sessionCodec: Option[String] = None,
      writerOption: Option[(String, String)] = None,
      expectedStoredCodec: Option[String] = None,
      verifyCodecs: Set[CompressionCodecName] => Unit)

  private val dataFileCodecCases: Seq[DataFileCodecCase] = {
    // Every protocol-defined codec set via the table property is honored for data files.
    val perProtocolCodec =
      Seq("snappy", "gzip", "zstd", "uncompressed", "none", "lz4", "lz4_raw").map { codec =>
        DataFileCodecCase(
          name = s"table property '$codec' is respected",
          tableProperty = Some(codec),
          expectedStoredCodec = Some(codec),
          verifyCodecs = codecs => assert(codecs == Set(expectedCodec(codec)),
            s"Expected only codec ${expectedCodec(codec)} but got: $codecs"))
      }
    perProtocolCodec ++ Seq(
      DataFileCodecCase(
        name = "mixed-case table property is normalized to lowercase when written",
        tableProperty = Some("GzIp"),
        verifyCodecs = codecs => assert(codecs == Set(CompressionCodecName.GZIP))),
      DataFileCodecCase(
        name = "DataFrame .option('compression', ...) overrides the table property",
        tableProperty = Some("gzip"),
        writerOption = Some(writerOptionKey -> "snappy"),
        verifyCodecs = codecs => assert(codecs == Set(CompressionCodecName.SNAPPY))),
      DataFileCodecCase(
        name = "mixed-case DataFrame compression option overrides the table property",
        tableProperty = Some("gzip"),
        // Spark normalizes the option key case-insensitively; ensure Delta does too.
        writerOption = Some("Compression" -> "snappy"),
        verifyCodecs = codecs => assert(codecs == Set(CompressionCodecName.SNAPPY))),
      DataFileCodecCase(
        name = "unset table property falls back to Spark's default behavior",
        // Spark's default Parquet codec is SNAPPY. We don't hard-code it - just assert that we
        // don't accidentally write something exotic, and that we get one consistent codec.
        verifyCodecs = codecs => assert(codecs.size == 1,
          s"Expected exactly one codec but got: $codecs")),
      DataFileCodecCase(
        name = "unset table property honors spark.sql.parquet.compression.codec override",
        sessionCodec = Some("gzip"),
        verifyCodecs = codecs => assert(codecs == Set(CompressionCodecName.GZIP),
          s"Expected GZIP from session conf override but got: $codecs")))
  }

  for (testCase <- dataFileCodecCases) {
    test(s"data files: ${testCase.name}") {
      def runScenario(): Unit = withTable("t") {
        val tblProperties = testCase.tableProperty
          .map(codec => s" TBLPROPERTIES ('$tablePropertyKey' = '$codec')")
          .getOrElse("")
        sql(s"CREATE TABLE t (c0 INT, c1 STRING, c2 TIMESTAMP) USING DELTA$tblProperties")
        if (testCase.tableProperty.isEmpty) {
          assert(!getProperties("t").contains(tablePropertyKey))
        }
        val writer = testCase.writerOption match {
          case Some((key, value)) => writeDF.option(key, value)
          case None => writeDF
        }
        writer.mode("append").saveAsTable("t")
        testCase.expectedStoredCodec.foreach { stored =>
          assert(getProperties("t").get(tablePropertyKey).contains(stored))
        }
        testCase.verifyCodecs(collectCodecsOfDataFiles("t"))
      }
      testCase.sessionCodec match {
        case Some(codec) => withSQLConf(sessionCodecKey -> codec)(runScenario())
        case None => runScenario()
      }
    }
  }

  test("ALTER TABLE can change codec; existing files keep their original codec") {
    withTable("t") {
      sql(s"""CREATE TABLE t (c0 INT, c1 STRING, c2 TIMESTAMP) USING DELTA
             |TBLPROPERTIES ('$tablePropertyKey' = 'gzip')""".stripMargin)
      writeDF.mode("append").saveAsTable("t")
      sql(s"ALTER TABLE t SET TBLPROPERTIES ('$tablePropertyKey' = 'snappy')")
      assert(getProperties("t").get(tablePropertyKey).contains("snappy"))
      writeDF.mode("append").saveAsTable("t")
      val codecs = collectCodecsOfDataFiles("t")
      assert(codecs == Set(CompressionCodecName.GZIP, CompressionCodecName.SNAPPY),
        s"Expected both GZIP and SNAPPY files but got: $codecs")
    }
  }

  /** Recursively lists all parquet files in the given _delta_log directory (incl. _sidecars/). */
  private def listAllCheckpointParquetFiles(
      logPath: Path,
      hadoopConf: Configuration): Seq[Path] = {
    val fs = logPath.getFileSystem(hadoopConf)
    if (!fs.exists(logPath)) return Seq.empty
    val result = scala.collection.mutable.ArrayBuffer.empty[Path]
    val recursive = true
    val it = fs.listFiles(logPath, recursive)
    while (it.hasNext) {
      val st = it.next()
      val name = st.getPath.getName
      if (name.endsWith(".parquet") && name.contains(".checkpoint.")) {
        result += st.getPath
      }
    }
    result.toSeq
  }

  // Both classic and V2 Parquet checkpoints must write their Parquet files (top-level manifest
  // and sidecars) using the table's configured compression codec. The only difference between
  // the two cases is the checkpoint policy, so they are parameterized over the session confs.
  for {
    (checkpointPolicy, extraConfs) <- Seq(
      "V2" -> Seq(
        DeltaConfigs.CHECKPOINT_POLICY.defaultTablePropertyKey -> CheckpointPolicy.V2.name,
        DeltaSQLConf.CHECKPOINT_V2_TOP_LEVEL_FILE_FORMAT.key -> V2Checkpoint.Format.PARQUET.name),
      "Classic" -> Seq.empty[(String, String)])
  } {
    test(s"$checkpointPolicy checkpoint files honor the codec") {
      withSQLConf(extraConfs: _*) {
        withTempDir { dir =>
          val path = dir.getCanonicalPath
          sql(s"""CREATE TABLE delta.`$path` (c0 BIGINT) USING delta
                 |TBLPROPERTIES ('$tablePropertyKey' = 'gzip')""".stripMargin)
          // Generate some commits so the snapshot has content to checkpoint.
          (1 to 5).foreach { i =>
            spark.range(i, i + 1).toDF("c0").write.format("delta").mode("append").save(path)
          }
          val log = DeltaLog.forTable(spark, path)
          log.checkpoint(log.update())
          val checkpointFiles =
            listAllCheckpointParquetFiles(log.logPath, log.newDeltaHadoopConf())
          assert(checkpointFiles.nonEmpty,
            s"Expected at least one $checkpointPolicy checkpoint parquet file")
          checkpointFiles.foreach { p =>
            val codecs = readParquetFileCodecs(p)
            assert(codecs == Set(CompressionCodecName.GZIP),
              s"Expected GZIP codec in checkpoint $p but got: $codecs")
          }
        }
      }
    }
  }
}
