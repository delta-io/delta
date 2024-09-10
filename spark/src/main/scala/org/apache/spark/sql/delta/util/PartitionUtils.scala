/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * This file contains code from the Apache Spark project (original license above).
 * It contains modifications, which are licensed as follows:
 */

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

package org.apache.spark.sql.delta.util

import java.lang.{Double => JDouble, Long => JLong}
import java.math.{BigDecimal => JBigDecimal}
import java.time.ZoneId
import java.util.{Locale, TimeZone}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.Try

import org.apache.spark.sql.delta.{DeltaAnalysisException, DeltaErrors}
import org.apache.hadoop.fs.Path
import org.apache.spark.unsafe.types.UTF8String

// scalastyle:off import.ordering.noEmptyLine
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.expressions.{Attribute, Cast, Literal}
import org.apache.spark.sql.catalyst.types.DataTypeUtils
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.types._

/**
 * This file is forked from [[org.apache.spark.sql.execution.datasources.PartitioningUtils]].
 */


// In open-source Apache Spark, PartitionPath is defined as
//
//  case class PartitionPath(values: InternalRow, path: Path)
//
// but in Databricks we use a different representation where the Path is stored as a String
// and converted back to a Path only when read. This significantly cuts memory consumption because
// Hadoop Path objects are heavyweight. See SC-7591 for details.
object PartitionPath {
  // Used only in tests:
  def apply(values: InternalRow, path: String): PartitionPath = {
    // Roundtrip through `new Path` to ensure any normalization done there is applied:
    apply(values, new Path(path))
  }

  def apply(values: InternalRow, path: Path): PartitionPath = {
    new PartitionPath(values, path.toString)
  }
}

/**
 * Holds a directory in a partitioned collection of files as well as the partition values
 * in the form of a Row.  Before scanning, the files at `path` need to be enumerated.
 */
class PartitionPath private (val values: InternalRow, val pathStr: String) {
  // Note: this isn't a case class because we don't want to have a public apply() method which
  // accepts a string. The goal is to force every value stored in `pathStr` to have gone through
  // a `new Path(...).toString` to ensure that canonicalization / normalization has taken place.
  def path: Path = new Path(pathStr)
  def withNewValues(newValues: InternalRow): PartitionPath = {
    new PartitionPath(newValues, pathStr)
  }
  override def equals(other: Any): Boolean = other match {
    case that: PartitionPath => values == that.values && pathStr == that.pathStr
    case _ => false
  }
  override def hashCode(): Int = {
    (values, pathStr).hashCode()
  }
  override def toString: String = {
    s"PartitionPath($values, $pathStr)"
  }
}

case class PartitionSpec(
    partitionColumns: StructType,
    partitions: Seq[PartitionPath])

object PartitionSpec {
  val emptySpec = PartitionSpec(StructType(Seq.empty[StructField]), Seq.empty[PartitionPath])
}

private[delta] object PartitionUtils {

  lazy val timestampPartitionPattern = "yyyy-MM-dd HH:mm:ss[.S]"
  lazy val utcFormatter = TimestampFormatter("yyyy-MM-dd'T'HH:mm:ss.SSSSSSz", ZoneId.of("Z"))

  case class PartitionValues(columnNames: Seq[String], literals: Seq[Literal])
  {
    require(columnNames.size == literals.size)
  }

  import org.apache.spark.sql.catalyst.catalog.ExternalCatalogUtils.{escapePathName, unescapePathName, DEFAULT_PARTITION_NAME}

  /**
   * Given a group of qualified paths, tries to parse them and returns a partition specification.
   * For example, given:
   * {{{
   *   hdfs://<host>:<port>/path/to/partition/a=1/b=hello/c=3.14
   *   hdfs://<host>:<port>/path/to/partition/a=2/b=world/c=6.28
   * }}}
   * it returns:
   * {{{
   *   PartitionSpec(
   *     partitionColumns = StructType(
   *       StructField(name = "a", dataType = IntegerType, nullable = true),
   *       StructField(name = "b", dataType = StringType, nullable = true),
   *       StructField(name = "c", dataType = DoubleType, nullable = true)),
   *     partitions = Seq(
   *       Partition(
   *         values = Row(1, "hello", 3.14),
   *         path = "hdfs://<host>:<port>/path/to/partition/a=1/b=hello/c=3.14"),
   *       Partition(
   *         values = Row(2, "world", 6.28),
   *         path = "hdfs://<host>:<port>/path/to/partition/a=2/b=world/c=6.28")))
   * }}}
   */
  def parsePartitions(
      paths: Seq[Path],
      typeInference: Boolean,
      basePaths: Set[Path],
      userSpecifiedSchema: Option[StructType],
      caseSensitive: Boolean,
      validatePartitionColumns: Boolean,
      timeZoneId: String): PartitionSpec = {
    parsePartitions(paths, typeInference, basePaths, userSpecifiedSchema, caseSensitive,
      validatePartitionColumns, DateTimeUtils.getTimeZone(timeZoneId))
  }

  def parsePartitions(
      paths: Seq[Path],
      typeInference: Boolean,
      basePaths: Set[Path],
      userSpecifiedSchema: Option[StructType],
      caseSensitive: Boolean,
      validatePartitionColumns: Boolean,
      timeZone: TimeZone): PartitionSpec = {
    val userSpecifiedDataTypes = if (userSpecifiedSchema.isDefined) {
      val nameToDataType = userSpecifiedSchema.get.fields.map(f => f.name -> f.dataType).toMap
      if (!caseSensitive) {
        CaseInsensitiveMap(nameToDataType)
      } else {
        nameToDataType
      }
    } else {
      Map.empty[String, DataType]
    }

    // SPARK-26990: use user specified field names if case insensitive.
    val userSpecifiedNames = if (userSpecifiedSchema.isDefined && !caseSensitive) {
      CaseInsensitiveMap(userSpecifiedSchema.get.fields.map(f => f.name -> f.name).toMap)
    } else {
      Map.empty[String, String]
    }

    val dateFormatter = DateFormatter()
    val timestampFormatter = TimestampFormatter(timestampPartitionPattern, timeZone)
    // First, we need to parse every partition's path and see if we can find partition values.
    val (partitionValues, optDiscoveredBasePaths) = paths.map { path =>
      parsePartition(path, typeInference, basePaths, userSpecifiedDataTypes,
        validatePartitionColumns, timeZone, dateFormatter, timestampFormatter)
    }.unzip

    // We create pairs of (path -> path's partition value) here
    // If the corresponding partition value is None, the pair will be skipped
    val pathsWithPartitionValues = paths.zip(partitionValues).flatMap(x => x._2.map(x._1 -> _))

    if (pathsWithPartitionValues.isEmpty) {
      // This dataset is not partitioned.
      PartitionSpec.emptySpec
    } else {
      // This dataset is partitioned. We need to check whether all partitions have the same
      // partition columns and resolve potential type conflicts.

      // Check if there is conflicting directory structure.
      // For the paths such as:
      // var paths = Seq(
      //   "hdfs://host:9000/invalidPath",
      //   "hdfs://host:9000/path/a=10/b=20",
      //   "hdfs://host:9000/path/a=10.5/b=hello")
      // It will be recognised as conflicting directory structure:
      //   "hdfs://host:9000/invalidPath"
      //   "hdfs://host:9000/path"
      // TODO: Selective case sensitivity.
      val discoveredBasePaths = optDiscoveredBasePaths.flatten.map(_.toString.toLowerCase())
      assert(
        discoveredBasePaths.distinct.size == 1,
        "Conflicting directory structures detected. Suspicious paths:\b" +
          discoveredBasePaths.distinct.mkString("\n\t", "\n\t", "\n\n") +
          "If provided paths are partition directories, please set " +
          "\"basePath\" in the options of the data source to specify the " +
          "root directory of the table. If there are multiple root directories, " +
          "please load them separately and then union them.")

      val resolvedPartitionValues =
        resolvePartitions(pathsWithPartitionValues, caseSensitive, timeZone)

      // Creates the StructType which represents the partition columns.
      val fields = {
        val PartitionValues(columnNames, literals) = resolvedPartitionValues.head
        columnNames.zip(literals).map { case (name, Literal(_, dataType)) =>
          // We always assume partition columns are nullable since we've no idea whether null values
          // will be appended in the future.
          val resultName = userSpecifiedNames.getOrElse(name, name)
          val resultDataType = userSpecifiedDataTypes.getOrElse(name, dataType)
          StructField(resultName, resultDataType, nullable = true)
        }
      }

      // Finally, we create `Partition`s based on paths and resolved partition values.
      val partitions = resolvedPartitionValues.zip(pathsWithPartitionValues).map {
        case (PartitionValues(_, literals), (path, _)) =>
          PartitionPath(InternalRow.fromSeq(literals.map(_.value)), path)
      }

      PartitionSpec(StructType(fields), partitions)
    }
  }

  /**
   * Parses a single partition, returns column names and values of each partition column, also
   * the path when we stop partition discovery.  For example, given:
   * {{{
   *   path = hdfs://<host>:<port>/path/to/partition/a=42/b=hello/c=3.14
   * }}}
   * it returns the partition:
   * {{{
   *   PartitionValues(
   *     Seq("a", "b", "c"),
   *     Seq(
   *       Literal.create(42, IntegerType),
   *       Literal.create("hello", StringType),
   *       Literal.create(3.14, DoubleType)))
   * }}}
   * and the path when we stop the discovery is:
   * {{{
   *   hdfs://<host>:<port>/path/to/partition
   * }}}
   */
  def parsePartition(
      path: Path,
      typeInference: Boolean,
      basePaths: Set[Path],
      userSpecifiedDataTypes: Map[String, DataType],
      validatePartitionColumns: Boolean,
      timeZone: TimeZone,
      dateFormatter: DateFormatter,
      timestampFormatter: TimestampFormatter,
      useUtcNormalizedTimestamp: Boolean = false): (Option[PartitionValues], Option[Path]) = {
    val columns = ArrayBuffer.empty[(String, Literal)]
    // Old Hadoop versions don't have `Path.isRoot`
    var finished = path.getParent == null
    // currentPath is the current path that we will use to parse partition column value.
    var currentPath: Path = path

    while (!finished) {
      // Sometimes (e.g., when speculative task is enabled), temporary directories may be left
      // uncleaned. Here we simply ignore them.
      if (currentPath.getName.toLowerCase(Locale.ROOT) == "_temporary") {
        return (None, None)
      }

      if (basePaths.contains(currentPath)) {
        // If the currentPath is one of base paths. We should stop.
        finished = true
      } else {
        // Let's say currentPath is a path of "/table/a=1/", currentPath.getName will give us a=1.
        // Once we get the string, we try to parse it and find the partition column and value.
        val maybeColumn =
        parsePartitionColumn(currentPath.getName, typeInference, userSpecifiedDataTypes,
          validatePartitionColumns, timeZone, dateFormatter, timestampFormatter,
          useUtcNormalizedTimestamp)
        maybeColumn.foreach(columns += _)

        // Now, we determine if we should stop.
        // When we hit any of the following cases, we will stop:
        //  - In this iteration, we could not parse the value of partition column and value,
        //    i.e. maybeColumn is None, and columns is not empty. At here we check if columns is
        //    empty to handle cases like /table/a=1/_temporary/something (we need to find a=1 in
        //    this case).
        //  - After we get the new currentPath, this new currentPath represent the top level dir
        //    i.e. currentPath.getParent == null. For the example of "/table/a=1/",
        //    the top level dir is "/table".
        finished =
          (maybeColumn.isEmpty && columns.nonEmpty) || currentPath.getParent == null

        if (!finished) {
          // For the above example, currentPath will be "/table/".
          currentPath = currentPath.getParent
        }
      }
    }

    if (columns.isEmpty) {
      (None, Some(path))
    } else {
      val (columnNames, values) = columns.reverse.unzip
      (Some(PartitionValues(columnNames.toSeq, values.toSeq)), Some(currentPath))
    }
  }

  private def parsePartitionColumn(
      columnSpec: String,
      typeInference: Boolean,
      userSpecifiedDataTypes: Map[String, DataType],
      validatePartitionColumns: Boolean,
      timeZone: TimeZone,
      dateFormatter: DateFormatter,
      timestampFormatter: TimestampFormatter,
      useUtcNormalizedTimestamp: Boolean = false): Option[(String, Literal)] = {
    val equalSignIndex = columnSpec.indexOf('=')
    if (equalSignIndex == -1) {
      None
    } else {
      val columnName = unescapePathName(columnSpec.take(equalSignIndex))
      assert(columnName.nonEmpty, s"Empty partition column name in '$columnSpec'")

      val rawColumnValue = columnSpec.drop(equalSignIndex + 1)
      assert(rawColumnValue.nonEmpty, s"Empty partition column value in '$columnSpec'")

      val literal = if (userSpecifiedDataTypes.contains(columnName)) {
        // SPARK-26188: if user provides corresponding column schema, get the column value without
        //              inference, and then cast it as user specified data type.
        val dataType = userSpecifiedDataTypes(columnName)
        val columnValueLiteral = inferPartitionColumnValue(
          rawColumnValue,
          false,
          timeZone,
          dateFormatter,
          timestampFormatter)
        val columnValue = columnValueLiteral.eval()
        if (dataType == DataTypes.TimestampType) {
          if (useUtcNormalizedTimestamp) {
            Try {
              Literal.create(
                utcFormatter.format(
                  timestampFormatter.parse(columnValue.asInstanceOf[UTF8String].toString)),
                StringType)
            }.getOrElse(columnValueLiteral)
          } else {
            columnValueLiteral
          }
        } else {
          val castedValue = Cast(columnValueLiteral, dataType, Option(timeZone.getID)).eval()
          if (validatePartitionColumns && columnValue != null && castedValue == null) {
            throw DeltaErrors.partitionColumnCastFailed(
              columnValue.toString, dataType.toString, columnName)
          }
          Literal.create(castedValue, dataType)
        }
      } else {
        inferPartitionColumnValue(
          rawColumnValue,
          typeInference,
          timeZone,
          dateFormatter,
          timestampFormatter)
      }
      Some(columnName -> literal)
    }
  }

  /**
   * Given a partition path fragment, e.g. `fieldOne=1/fieldTwo=2`, returns a parsed spec
   * for that fragment as a `TablePartitionSpec`, e.g. `Map(("fieldOne", "1"), ("fieldTwo", "2"))`.
   */
  def parsePathFragment(pathFragment: String): TablePartitionSpec = {
    parsePathFragmentAsSeq(pathFragment).toMap
  }

  /**
   * Given a partition path fragment, e.g. `fieldOne=1/fieldTwo=2`, returns a parsed spec
   * for that fragment as a `Seq[(String, String)]`, e.g.
   * `Seq(("fieldOne", "1"), ("fieldTwo", "2"))`.
   */
  def parsePathFragmentAsSeq(pathFragment: String): Seq[(String, String)] = {
    pathFragment.stripPrefix("data/").split("/").map { kv =>
      val pair = kv.split("=", 2)
      (unescapePathName(pair(0)), unescapePathName(pair(1)))
    }
  }

  /**
   * This is the inverse of parsePathFragment().
   */
  def getPathFragment(spec: TablePartitionSpec, partitionSchema: StructType): String = {
    partitionSchema.map { field =>
      escapePathName(field.name) + "=" + escapePathName(spec(field.name))
    }.mkString("/")
  }

  def getPathFragment(spec: TablePartitionSpec, partitionColumns: Seq[Attribute]): String = {
    getPathFragment(spec, DataTypeUtils.fromAttributes(partitionColumns))
  }

  /**
   * Normalize the column names in partition specification, w.r.t. the real partition column names
   * and case sensitivity. e.g., if the partition spec has a column named `monTh`, and there is a
   * partition column named `month`, and it's case insensitive, we will normalize `monTh` to
   * `month`.
   */
  def normalizePartitionSpec[T](
      partitionSpec: Map[String, T],
      partColNames: Seq[String],
      tblName: String,
      resolver: Resolver): Map[String, T] = {
    val normalizedPartSpec = partitionSpec.toSeq.map { case (key, value) =>
      val normalizedKey = partColNames.find(resolver(_, key)).getOrElse {
        throw DeltaErrors.invalidPartitionColumn(key, tblName)
      }
      normalizedKey -> value
    }

    checkColumnNameDuplication(
      normalizedPartSpec.map(_._1), "in the partition schema", resolver)

    normalizedPartSpec.toMap
  }

  /**
   * Resolves possible type conflicts between partitions by up-casting "lower" types using
   * [[findWiderTypeForPartitionColumn]].
   */
  def resolvePartitions(
      pathsWithPartitionValues: Seq[(Path, PartitionValues)],
      caseSensitive: Boolean,
      timeZone: TimeZone): Seq[PartitionValues] = {
    if (pathsWithPartitionValues.isEmpty) {
      Seq.empty
    } else {
      val partColNames = if (caseSensitive) {
        pathsWithPartitionValues.map(_._2.columnNames)
      } else {
        pathsWithPartitionValues.map(_._2.columnNames.map(_.toLowerCase()))
      }
      assert(
        partColNames.distinct.size == 1,
        listConflictingPartitionColumns(pathsWithPartitionValues))

      // Resolves possible type conflicts for each column
      val values = pathsWithPartitionValues.map(_._2)
      val columnCount = values.head.columnNames.size
      val resolvedValues = (0 until columnCount).map { i =>
        resolveTypeConflicts(values.map(_.literals(i)), timeZone)
      }

      // Fills resolved literals back to each partition
      values.zipWithIndex.map { case (d, index) =>
        d.copy(literals = resolvedValues.map(_(index)))
      }
    }
  }

  def listConflictingPartitionColumns(
      pathWithPartitionValues: Seq[(Path, PartitionValues)]): String = {
    val distinctPartColNames = pathWithPartitionValues.map(_._2.columnNames).distinct

    def groupByKey[K, V](seq: Seq[(K, V)]): Map[K, Iterable[V]] =
      seq.groupBy { case (key, _) => key }.mapValues(_.map { case (_, value) => value }).toMap

    val partColNamesToPaths = groupByKey(pathWithPartitionValues.map {
      case (path, partValues) => partValues.columnNames -> path
    })

    val distinctPartColLists = distinctPartColNames.map(_.mkString(", ")).zipWithIndex.map {
      case (names, index) =>
        s"Partition column name list #$index: $names"
    }

    // Lists out those non-leaf partition directories that also contain files
    val suspiciousPaths = distinctPartColNames.sortBy(_.length).flatMap(partColNamesToPaths)

    s"Conflicting partition column names detected:\n" +
      distinctPartColLists.mkString("\n\t", "\n\t", "\n\n") +
      "For partitioned table directories, data files should only live in leaf directories.\n" +
      "And directories at the same level should have the same partition column name.\n" +
      "Please check the following directories for unexpected files or " +
      "inconsistent partition column names:\n" +
      suspiciousPaths.map("\t" + _).mkString("\n", "\n", "")
  }

  // scalastyle:off line.size.limit
  /**
   * Converts a string to a [[Literal]] with automatic type inference. Currently only supports
   * [[NullType]], [[IntegerType]], [[LongType]], [[DoubleType]], [[DecimalType]], [[DateType]]
   * [[TimestampType]], and [[StringType]].
   *
   * When resolving conflicts, it follows the table below:
   *
   * +--------------------+-------------------+-------------------+-------------------+--------------------+------------+---------------+---------------+------------+
   * | InputA \ InputB    | NullType          | IntegerType       | LongType          | DecimalType(38,0)* | DoubleType | DateType      | TimestampType | StringType |
   * +--------------------+-------------------+-------------------+-------------------+--------------------+------------+---------------+---------------+------------+
   * | NullType           | NullType          | IntegerType       | LongType          | DecimalType(38,0)  | DoubleType | DateType      | TimestampType | StringType |
   * | IntegerType        | IntegerType       | IntegerType       | LongType          | DecimalType(38,0)  | DoubleType | StringType    | StringType    | StringType |
   * | LongType           | LongType          | LongType          | LongType          | DecimalType(38,0)  | StringType | StringType    | StringType    | StringType |
   * | DecimalType(38,0)* | DecimalType(38,0) | DecimalType(38,0) | DecimalType(38,0) | DecimalType(38,0)  | StringType | StringType    | StringType    | StringType |
   * | DoubleType         | DoubleType        | DoubleType        | StringType        | StringType         | DoubleType | StringType    | StringType    | StringType |
   * | DateType           | DateType          | StringType        | StringType        | StringType         | StringType | DateType      | TimestampType | StringType |
   * | TimestampType      | TimestampType     | StringType        | StringType        | StringType         | StringType | TimestampType | TimestampType | StringType |
   * | StringType         | StringType        | StringType        | StringType        | StringType         | StringType | StringType    | StringType    | StringType |
   * +--------------------+-------------------+-------------------+-------------------+--------------------+------------+---------------+---------------+------------+
   * Note that, for DecimalType(38,0)*, the table above intentionally does not cover all other
   * combinations of scales and precisions because currently we only infer decimal type like
   * `BigInteger`/`BigInt`. For example, 1.1 is inferred as double type.
   */
  // scalastyle:on line.size.limit
  def inferPartitionColumnValue(
      raw: String,
      typeInference: Boolean,
      timeZone: TimeZone,
      dateFormatter: DateFormatter,
      timestampFormatter: TimestampFormatter): Literal = {
    def decimalTry = Try {
      // `BigDecimal` conversion can fail when the `field` is not a form of number.
      val bigDecimal = new JBigDecimal(raw)
      // It reduces the cases for decimals by disallowing values having scale (eg. `1.1`).
      require(bigDecimal.scale <= 0)
      // `DecimalType` conversion can fail when
      //   1. The precision is bigger than 38.
      //   2. scale is bigger than precision.
      Literal(bigDecimal)
    }

    def dateTry = Try {
      // try and parse the date, if no exception occurs this is a candidate to be resolved as
      // DateType
      dateFormatter.parse(raw)
      // SPARK-23436: Casting the string to date may still return null if a bad Date is provided.
      // This can happen since DateFormat.parse  may not use the entire text of the given string:
      // so if there are extra-characters after the date, it returns correctly.
      // We need to check that we can cast the raw string since we later can use Cast to get
      // the partition values with the right DataType (see
      // org.apache.spark.sql.execution.datasources.PartitioningAwareFileIndex.inferPartitioning)
      val dateValue = Cast(Literal(raw), DateType).eval()
      // Disallow DateType if the cast returned null
      require(dateValue != null)
      Literal.create(dateValue, DateType)
    }

    def timestampTry = Try {
      val unescapedRaw = unescapePathName(raw)
      // try and parse the date, if no exception occurs this is a candidate to be resolved as
      // TimestampType
      timestampFormatter.parse(unescapedRaw)
      // SPARK-23436: see comment for date
      val timestampValue = Cast(Literal(unescapedRaw), TimestampType, Some(timeZone.getID)).eval()
      // Disallow TimestampType if the cast returned null
      require(timestampValue != null)
      Literal.create(timestampValue, TimestampType)
    }

    if (typeInference) {
      // First tries integral types
      Try(Literal.create(Integer.parseInt(raw), IntegerType))
        .orElse(Try(Literal.create(JLong.parseLong(raw), LongType)))
        .orElse(decimalTry)
        // Then falls back to fractional types
        .orElse(Try(Literal.create(JDouble.parseDouble(raw), DoubleType)))
        // Then falls back to date/timestamp types
        .orElse(timestampTry)
        .orElse(dateTry)
        // Then falls back to string
        .getOrElse {
          if (raw == DEFAULT_PARTITION_NAME) {
            Literal.default(NullType)
          } else {
            Literal.create(unescapePathName(raw), StringType)
          }
        }
    } else {
      if (raw == DEFAULT_PARTITION_NAME) {
        Literal.default(NullType)
      } else {
        Literal.create(unescapePathName(raw), StringType)
      }
    }
  }

  def validatePartitionColumn(
      schema: StructType,
      partitionColumns: Seq[String],
      caseSensitive: Boolean): Unit = {
    checkColumnNameDuplication(
      partitionColumns,
      "in the partition columns",
      caseSensitive)

    partitionColumnsSchema(schema, partitionColumns, caseSensitive).foreach {
      field => field.dataType match {
        // Variant types are not orderable and thus cannot be partition columns.
        case a: AtomicType if !VariantShims.isVariantType(a) => // OK
        case _ => throw DeltaErrors.cannotUseDataTypeForPartitionColumnError(field)
      }
    }

    if (partitionColumns.nonEmpty && partitionColumns.size == schema.fields.length) {
      throw new DeltaAnalysisException(
        errorClass = "DELTA_CANNOT_USE_ALL_COLUMNS_FOR_PARTITION",
        Array.empty)
    }
  }

  def partitionColumnsSchema(
      schema: StructType,
      partitionColumns: Seq[String],
      caseSensitive: Boolean): StructType = {
    val equality = columnNameEquality(caseSensitive)
    StructType(partitionColumns.map { col =>
      schema.find(f => equality(f.name, col)).getOrElse {
        val schemaCatalog = schema.catalogString
        throw DeltaErrors.missingPartitionColumn(col, schemaCatalog)
      }
    }).asNullable
  }

  def mergeDataAndPartitionSchema(
      dataSchema: StructType,
      partitionSchema: StructType,
      caseSensitive: Boolean): (StructType, Map[String, StructField]) = {
    val overlappedPartCols = mutable.Map.empty[String, StructField]
    partitionSchema.foreach { partitionField =>
      val partitionFieldName = getColName(partitionField, caseSensitive)
      if (dataSchema.exists(getColName(_, caseSensitive) == partitionFieldName)) {
        overlappedPartCols += partitionFieldName -> partitionField
      }
    }

    // When data and partition schemas have overlapping columns, the output
    // schema respects the order of the data schema for the overlapping columns, and it
    // respects the data types of the partition schema.
    // `HadoopFsRelation` will be mapped to `FileSourceScanExec`, which always output
    // all the partition columns physically. Here we need to make sure the final schema
    // contains all the partition columns.
    val fullSchema =
    StructType(dataSchema.map(f => overlappedPartCols.getOrElse(getColName(f, caseSensitive), f)) ++
      partitionSchema.filterNot(f => overlappedPartCols.contains(getColName(f, caseSensitive))))
    (fullSchema, overlappedPartCols.toMap)
  }

  def getColName(f: StructField, caseSensitive: Boolean): String = {
    if (caseSensitive) {
      f.name
    } else {
      f.name.toLowerCase(Locale.ROOT)
    }
  }

  private def columnNameEquality(caseSensitive: Boolean): (String, String) => Boolean = {
    if (caseSensitive) {
      org.apache.spark.sql.catalyst.analysis.caseSensitiveResolution
    } else {
      org.apache.spark.sql.catalyst.analysis.caseInsensitiveResolution
    }
  }

  /**
   * Given a collection of [[Literal]]s, resolves possible type conflicts by
   * [[findWiderTypeForPartitionColumn]].
   */
  private def resolveTypeConflicts(literals: Seq[Literal], timeZone: TimeZone): Seq[Literal] = {
    val litTypes = literals.map(_.dataType)
    val desiredType = litTypes.reduce(findWiderTypeForPartitionColumn)

    literals.map { case l @ Literal(_, dataType) =>
      Literal.create(Cast(l, desiredType, Some(timeZone.getID)).eval(), desiredType)
    }
  }

  /**
   * Type widening rule for partition column types. It is similar to
   * [[TypeCoercion.findWiderTypeForTwo]] but the main difference is that here we disallow
   * precision loss when widening double/long and decimal, and fall back to string.
   */
  private val findWiderTypeForPartitionColumn: (DataType, DataType) => DataType = {
    case (DoubleType, _: DecimalType) | (_: DecimalType, DoubleType) => StringType
    case (DoubleType, LongType) | (LongType, DoubleType) => StringType
    case (t1, t2) => TypeCoercion.findWiderTypeForTwo(t1, t2).getOrElse(StringType)
  }

  /** The methods below are forked from [[org.apache.spark.sql.util.SchemaUtils]] */

  /**
   * Checks if input column names have duplicate identifiers. This throws an exception if
   * the duplication exists.
   *
   * @param columnNames column names to check
   * @param colType column type name, used in an exception message
   * @param resolver resolver used to determine if two identifiers are equal
   */
  def checkColumnNameDuplication(
      columnNames: Seq[String], colType: String, resolver: Resolver): Unit = {
    checkColumnNameDuplication(columnNames, colType, isCaseSensitiveAnalysis(resolver))
  }

  /**
   * Checks if input column names have duplicate identifiers. This throws an exception if
   * the duplication exists.
   *
   * @param columnNames column names to check
   * @param colType column type name, used in an exception message
   * @param caseSensitiveAnalysis whether duplication checks should be case sensitive or not
   */
  def checkColumnNameDuplication(
      columnNames: Seq[String], colType: String, caseSensitiveAnalysis: Boolean): Unit = {
    // scalastyle:off caselocale
    val names = if (caseSensitiveAnalysis) columnNames else columnNames.map(_.toLowerCase)
    // scalastyle:on caselocale
    if (names.distinct.length != names.length) {
      val duplicateColumns = names.groupBy(identity).collect {
        case (x, ys) if ys.length > 1 => s"`$x`"
      }
      throw DeltaErrors.foundDuplicateColumnsException(colType,
        duplicateColumns.mkString(", "))
    }
  }

  // Returns true if a given resolver is case-sensitive
  private def isCaseSensitiveAnalysis(resolver: Resolver): Boolean = {
    if (resolver == caseSensitiveResolution) {
      true
    } else if (resolver == caseInsensitiveResolution) {
      false
    } else {
      sys.error("A resolver to check if two identifiers are equal must be " +
        "`caseSensitiveResolution` or `caseInsensitiveResolution` in o.a.s.sql.catalyst.")
    }
  }
}
