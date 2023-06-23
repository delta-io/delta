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

package benchmark

import java.util.Locale

import org.apache.spark.sql.functions.{col, countDistinct, hash, isnull, max, rand}


case class MergeDataLoadConf(
    scaleInGB: Int = 0,
    userDefinedDbName: Option[String] = None,
    sourcePath: Option[String] = None,
    benchmarkPath: Option[String] = None,
    excludeNulls: Boolean = true) extends MergeConf {
}

/**
 * Represents a source table configuration used in merge benchmarks. Each [[MergeTestCase]] has one
 * [[MergeSourceTable]] associated with it, the data loader will collect all source table
 * configurations for all tests and create the required source tables.
 * @param fileMatchedFraction Fraction of files from the target table that will get sampled to
 *                            create the source table.
 * @param rowMatchedFraction Fraction of rows from the selected files that will get sampled to form
 *                           the part of the source table that matches the merge condition.
 * @param rowNotMatchedFraction Fraction of rows from the selected files that will get sampled to
 *                              form the part of the source table that doesn't match the merge
 *                              condition.
 */
case class MergeSourceTable(
    fileMatchedFraction: Double,
    rowMatchedFraction: Double,
    rowNotMatchedFraction: Double) {
  def name: String = formatTableName(s"source_" +
    s"_fileMatchedFraction_$fileMatchedFraction" +
    s"_rowMatchedFraction_$rowMatchedFraction" +
    s"_rowNotMatchedFraction_$rowNotMatchedFraction")

  protected def formatTableName(s: String): String = {
    s.toLowerCase(Locale.ROOT).replaceAll("\\s+", "_").replaceAll("[-,.]", "_")
  }
}

object MergeDataLoadConf {
  import scopt.OParser
  private val builder = OParser.builder[MergeDataLoadConf]
  private val argParser = {
    import builder._
    OParser.sequence(
      programName("Merge Data Load"),
      opt[String]("scale-in-gb")
        .required()
        .valueName("<scale of benchmark in GBs>")
        .action((x, c) => c.copy(scaleInGB = x.toInt))
        .text("Scale factor of the Merge benchmark"),
      opt[String]("benchmark-path")
        .required()
        .valueName("<cloud storage path>")
        .action((x, c) => c.copy(benchmarkPath = Some(x)))
        .text("Cloud storage path to be used for creating table and generating reports"),
      opt[String]("db-name")
        .optional()
        .valueName("<database name>")
        .action((x, c) => c.copy(userDefinedDbName = Some(x)))
        .text("Name of the target database to create with TPC-DS tables in necessary format"),
      opt[String]("source-path")
        .optional()
        .valueName("<path to the TPC-DS raw input data>")
        .action((x, c) => c.copy(sourcePath = Some(x)))
        .text("The location of the TPC-DS raw input data"),
      opt[String]("exclude-nulls")
        .optional()
        .valueName("true/false")
        .action((x, c) => c.copy(excludeNulls = x.toBoolean))
        .text("Whether to remove null primary keys when loading data, default = false"),                                                                                                                                                          )
  }

  def parse(args: Array[String]): Option[MergeDataLoadConf] = {
    OParser.parse(argParser, args, MergeDataLoadConf())
  }
}

class MergeDataLoad(conf: MergeDataLoadConf) extends Benchmark(conf) {

  protected def targetTableFullName = s"`${conf.dbName}`.`target_${conf.tableName}`"

  protected def dataSourceLocation: String = conf.sourcePath.getOrElse {
    s"s3://devrel-delta-datasets/tpcds-2.13/tpcds_sf${conf.scaleInGB}_parquet/${conf.tableName}/"
  }

  /**
   * Creates the target table and all source table configuration used in merge test cases.
   */
  def runInternal(): Unit = {
    val dbName = conf.dbName
    val dbLocation = conf.dbLocation(dbName, suffix = benchmarkId.replace("-", "_"))
    val dbCatalog = "spark_catalog"

    require(conf.scaleInGB > 0)
    require(Seq(1, 3000).contains(conf.scaleInGB), "")

    log(s"====== Creating database =======")
    runQuery(s"DROP DATABASE IF EXISTS ${dbName} CASCADE", s"drop-database")
    runQuery(s"CREATE DATABASE IF NOT EXISTS ${dbName}", s"create-database")

    log(s"====== Creating target table =======")
    loadTargetTable()
    log(s"====== Creating source tables =======")
    MergeTestCases.testCases.map(_.sourceTable).distinct.foreach(loadSourceTable)
    log(s"====== Created all tables in database ${dbName} at '${dbLocation}' =======")

    runQuery(s"USE $dbCatalog.$dbName;")
    runQuery("SHOW TABLES", printRows = true)
  }

  /**
   * Creates the target Delta table and performs sanity checks.
   */
  protected def loadTargetTable(): Unit = {
    val dbLocation = conf.dbLocation(conf.dbName, suffix = benchmarkId.replace("-", "_"))
    val location = s"${dbLocation}/${conf.tableName}/"
    val sourceFormat = "parquet"

    runQuery(s"DROP TABLE IF EXISTS $targetTableFullName", s"drop-table-$targetTableFullName")

    runQuery(
      s"""CREATE TABLE $targetTableFullName
                 USING DELTA
                 LOCATION '$location'
                 SELECT * FROM `${sourceFormat}`.`$dataSourceLocation`
              """, s"create-table-$targetTableFullName", ignoreError = true)

    val sourceRowCount =
      spark.sql(s"SELECT * FROM `${sourceFormat}`.`$dataSourceLocation`").count()
    val targetRowCount = spark.table(targetTableFullName).count()
    val targetFileCount =
      spark.table(targetTableFullName).select(countDistinct("_metadata.file_path"))
    log(s"Target file count: $targetFileCount")
    log(s"Target row count: $targetRowCount")

    assert(targetRowCount == sourceRowCount,
      s"Row count mismatch: source table = $sourceRowCount, " +
      s"target $targetTableFullName = $targetRowCount")
  }

  /**
   * Creates a source table for the given source table configuration by sampling the target table.
   */
  protected def loadSourceTable(sourceTable: MergeSourceTable): Unit = {
    val fullTableName = s"`${conf.dbName}`.`${sourceTable.name}`"
    val dbLocation = conf.dbLocation(conf.dbName, suffix = benchmarkId.replace("-", "_"))

    runQuery(s"DROP TABLE IF EXISTS $fullTableName", s"drop-table-${sourceTable.name}")

    val deltaDf = spark.read.format("delta")
      .load(s"${dbLocation}/${conf.tableName}/")
      // Sample files by hashing their paths.
      .filter(hash(col("_metadata.file_path")) * 0.5 / Integer.MAX_VALUE + 0.5 < sourceTable.fileMatchedFraction)
    log(s"Matching files row count: ${deltaDf.count}")

    val numberOfNulls = deltaDf.filter(isnull(col("wr_order_number"))).count
    log(s"wr_order_number contains $numberOfNulls null values")
    val matchedData = deltaDf.sample(sourceTable.rowMatchedFraction)
    val notMatchedData = deltaDf.sample(sourceTable.rowNotMatchedFraction)
      .withColumn("wr_order_number", rand())
      .withColumn("wr_item_sk", rand())


    val data = matchedData.union(notMatchedData)

    val dupes = data.groupBy("wr_order_number", "wr_item_sk").count.filter("count > 1")
    log(s"Duplicates: ${dupes.collect().mkString("Array(", ",\n", ")")}")
    data.write.format("delta").saveAsTable(fullTableName)
  }
}

object MergeDataLoad {
  def main(args: Array[String]): Unit = {
    MergeDataLoadConf.parse(args).foreach { conf =>
      new MergeDataLoad(conf).run()
    }
  }
}
