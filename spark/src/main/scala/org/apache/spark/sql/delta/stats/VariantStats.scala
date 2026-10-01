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

package org.apache.spark.sql.delta.stats

import java.math.{BigDecimal => JBigDecimal, BigInteger}

import scala.collection.{immutable, mutable}
import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._

import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.parquet.column.statistics._
import org.apache.parquet.hadoop.metadata.ParquetMetadata
import org.apache.parquet.schema.LogicalTypeAnnotation._
import org.apache.parquet.schema.PrimitiveType
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName

import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression, GetStructField, Literal}
import org.apache.spark.sql.catalyst.expressions.aggregate.DeclarativeAggregate
import org.apache.spark.sql.catalyst.expressions.objects.Invoke
import org.apache.spark.sql.catalyst.trees.UnaryLike
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataType, Decimal, VariantType}
import org.apache.spark.types.variant.VariantBuilder
import org.apache.spark.unsafe.types.VariantVal

sealed trait VariantStatType
case object MIN_STAT extends VariantStatType
case object MAX_STAT extends VariantStatType
case object NULL_COUNT_STAT extends VariantStatType

/**
 * Template class for Variant stat aggregate expressions `MinVariantStats` and `MaxVariantStats`.
 * These expressions do nothing during the actual aggregation phase as the shredding schema is not
 * determined by then, once a parquet footer is ready, variant stats are inferred from the footer
 * during the `evaluateExpression` phase.
 */
abstract class VariantStatsExpression(child: Expression) extends DeclarativeAggregate
  with UnaryLike[Expression] {
  override def nullable: Boolean = true

  /**
   * The path corresponding to the Variant stats expression
   */
  lazy val path: Seq[String] = {
    val path = ArrayBuffer[String]()
    VariantStatsUtils.childPath(child, path)
    path.toSeq
  }

  protected val statType: VariantStatType

  private var variantStatsData: Option[VariantStatsData] = None

  def addVariantStatsData(statsData: VariantStatsData): Unit = {
    variantStatsData = Some(statsData)
  }

  override def dataType: DataType = VariantType

  override lazy val initialValues: Seq[Literal] = Seq(Literal.create(null, VariantType))

  override lazy val updateExpressions: Seq[Expression] = {
    Seq(Literal.create(null, VariantType))
  }

  override lazy val mergeExpressions: Seq[Expression] = {
    Seq(Literal.create(null, VariantType))
  }

  // Get the VariantVal representing the stats from the footer corresponding to the Variant column
  // in the stats expression
  def getStats: VariantVal = {
    variantStatsData match {
      case Some(data) => data.forPrefixAndStatType(path, statType)
      case None => null
    }
  }

  override lazy val evaluateExpression: Expression = {
    Invoke(
      Literal.fromObject(this),
      "getStats",
      VariantType,
      Seq()
    )
  }
}

case class MinVariantStats(child: Expression) extends VariantStatsExpression(child) {
  private lazy val min = AttributeReference("min", child.dataType)()

  override lazy val aggBufferAttributes: Seq[AttributeReference] = min :: Nil

  override val statType: VariantStatType = MIN_STAT

  override protected def withNewChildInternal(newChild: Expression): MinVariantStats =
      copy(child = newChild)
}

case class MaxVariantStats(child: Expression) extends VariantStatsExpression(child) {
  private lazy val max = AttributeReference("max", child.dataType)()

  override lazy val aggBufferAttributes: Seq[AttributeReference] = max :: Nil

  override val statType: VariantStatType = MAX_STAT

  override protected def withNewChildInternal(newChild: Expression): MaxVariantStats =
    copy(child = newChild)
}

/**
 * This class represents an intermediate state in computing variant statistics which consists of
 * aggregated stats of columns that may be relevant to shredded variants across row-groups. These
 * aggregated stats are stored in `leafPathStatisticsOpt` while the types of these columns are
 * stored in `leafPathTypesOpt`. The columns that are considered in the computation of this state
 * are the ones that definitely do not correspond to typed_value columns in shredded variants,
 * for example, column that do not end in `typed_value`. Shredded columns where the companion
 * `value` columns have non-null values are also filtered out.
 *
 * If there are multiple shredded variant columns in a parquet footer, this class will contain stats
 * for all of them. The stats corresponding to the individual variant columns can be extracted by
 * the variant stats expressions corresponding to these individual columns via
 * `forPrefixAndStatType`.
 *
 * This class acts as a memoizer which aims to prevent the whole parquet footer from being parsed
 * for each shredded variant column.
 *
 * `dateRebaseFunc` and `timestampRebaseFunc` rebase date and timestamp values from the calendar
 * in which the parquet file was written (potentially the legacy Julian calendar for files
 * produced by older Spark versions) into Spark's Proleptic Gregorian calendar. The mode is
 * derived from the parquet file's key/value metadata and the
 * `spark.sql.parquet.datetimeRebaseModeInRead` SQL conf.
 */
class VariantStatsData(
  leafPathStatisticsSuperset: mutable.HashMap[Seq[String], Statistics[_]],
  leafPathTypesSuperset: mutable.HashMap[Seq[String], PrimitiveType],
  dateRebaseFunc: Int => Int,
  timestampRebaseFunc: Long => Long
) extends Serializable {
  private def genericGet[T <: Comparable[T]](stat: Statistics[T], statType: VariantStatType): T =
    if (statType == MIN_STAT) {
      stat.genericGetMin()
    } else {
      stat.genericGetMax()
    }

  /**
   * The class consists of all data in the parquet footer that may be relevant to variants. This
   * function extracts the final Variant stats for a particular variant column by filtering out
   * paths that:
   * 1. don't share the same prefix as the given path.
   * 2. may correspond to shredded arrays
   */
  def forPrefixAndStatType(prefix: Seq[String], statType: VariantStatType): VariantVal = {
    val (leafPathStatistics, leafPathTypes) =
      filterPathMapsByPrefix(leafPathStatisticsSuperset, leafPathTypesSuperset, prefix)
    if (leafPathStatistics.nonEmpty) {
      val vb = new VariantBuilder(false)
      val fields = new java.util.ArrayList[VariantBuilder.FieldEntry]()
      for ((leafPath, stats) <- leafPathStatistics) {
        val leafPathType = leafPathTypes(leafPath)
        // canonicalize path string
        val pathString = VariantStatsUtils.normalizedJsonPath(leafPath)
        def addKey(path: String): Int = {
          val id = vb.addKey(path)
          fields.add(new VariantBuilder.FieldEntry(path, id, vb.getWritePos))
          id
        }
        leafPathType.getPrimitiveTypeName match {
          case PrimitiveTypeName.INT32 =>
            // Look at logical type annotation. It could be an integral, decimal or date
            // type
            leafPathType.getLogicalTypeAnnotation match {
              case i: IntLogicalTypeAnnotation if i.getBitWidth == 8 && i.isSigned =>
                // INT8 - Currently, this code should never be encountered since we always shred
                // ints as INT64 right now.
                stats match {
                  case stat: IntStatistics if stat.hasNonNullValue =>
                    addKey(pathString)
                    val statVal = genericGet(stat, statType).toInt
                    VariantStatsHelper.appendIntFixedPrecision(vb, statVal, 8)
                  case _ =>
                }
              case i: IntLogicalTypeAnnotation if i.getBitWidth == 16 && i.isSigned =>
                // INT16 - Currently, this code should never be encountered since we always shred
                // ints as INT64 right now.
                stats match {
                  case stat: IntStatistics if stat.hasNonNullValue =>
                    addKey(pathString)
                    val statVal = genericGet(stat, statType)
                    VariantStatsHelper.appendIntFixedPrecision(vb, statVal.toInt, 16)
                  case _ =>
                }
              case null =>
                // Basic INT32 - Currently, this code should never be encountered since we always
                // shred ints as INT64 right now.
                stats match {
                  case stat: IntStatistics if stat.hasNonNullValue =>
                    addKey(pathString)
                    val statVal = genericGet(stat, statType)
                    VariantStatsHelper.appendIntFixedPrecision(vb, statVal.toInt, 32)
                  case _ =>
                }
              case i: IntLogicalTypeAnnotation if i.getBitWidth == 32 && i.isSigned =>
                // Basic INT32 - Currently, this code should never be encountered since we always
                // shred ints as INT64 right now.
                stats match {
                  case stat: IntStatistics if stat.hasNonNullValue =>
                    addKey(pathString)
                    val statVal = genericGet(stat, statType)
                    VariantStatsHelper.appendIntFixedPrecision(vb, statVal.toInt, 32)
                  case _ =>
                }
              case _: DateLogicalTypeAnnotation =>
                // Date
                stats match {
                  case stat: IntStatistics if stat.hasNonNullValue =>
                    addKey(pathString)
                    val statVal = dateRebaseFunc(genericGet(stat, statType))
                    vb.appendDate(statVal)
                  case _ =>
                }
              case d: DecimalLogicalTypeAnnotation =>
                // Decimal4 - Currently this code should never be encountered since we always shred
                // decimal as Decimal8
                val scale = d.getScale
                val precision = d.getPrecision
                stats match {
                  case stat: IntStatistics if stat.hasNonNullValue =>
                    val unscaled = genericGet(stat, statType)
                    val decimal = new Decimal().setOrNull(unscaled.toLong, precision, scale)
                      .toJavaBigDecimal
                    if (decimal != null) {
                      addKey(pathString)
                      VariantStatsHelper.appendDecimalFixedPrecision(vb, decimal, 32)
                    }
                  case _ =>
                }
              case _ =>
            }
          case PrimitiveTypeName.INT64 =>
            // Look at logical type annotation. It could be an integral, decimal or timestamp
            leafPathType.getLogicalTypeAnnotation match {
              case null =>
                // Basic INT64
                stats match {
                  case stat: LongStatistics if stat.hasNonNullValue =>
                    addKey(pathString)
                    val statVal = genericGet(stat, statType)
                    VariantStatsHelper.appendIntFixedPrecision(vb, statVal, 64)
                  case _ =>
                }
              case i: IntLogicalTypeAnnotation if i.getBitWidth == 64 && i.isSigned =>
                // Basic INT64
                stats match {
                  case stat: LongStatistics if stat.hasNonNullValue =>
                    addKey(pathString)
                    val statVal = genericGet(stat, statType)
                    VariantStatsHelper.appendIntFixedPrecision(vb, statVal, 64)
                  case _ =>
                }
              case t: TimestampLogicalTypeAnnotation if t.isAdjustedToUTC &&
                t.getUnit == TimeUnit.MICROS =>
                // Timestamp - Spark only supports microsecond timestamps
                stats match {
                  case stat: LongStatistics if stat.hasNonNullValue =>
                    addKey(pathString)
                    val statVal = timestampRebaseFunc(genericGet(stat, statType))
                    vb.appendTimestamp(statVal)
                  case _ =>
                }
              case t: TimestampLogicalTypeAnnotation if !t.isAdjustedToUTC &&
                t.getUnit == TimeUnit.MICROS =>
                // TimestampNTZ - Spark only supports microsecond timestamps
                stats match {
                  case stat: LongStatistics if stat.hasNonNullValue =>
                    addKey(pathString)
                    val statVal = genericGet(stat, statType)
                    vb.appendTimestampNtz(statVal)
                  case _ =>
                }
              case d: DecimalLogicalTypeAnnotation =>
                // Decimal8
                val scale = d.getScale
                val precision = d.getPrecision
                stats match {
                  case stat: LongStatistics if stat.hasNonNullValue =>
                    val unscaled = genericGet(stat, statType)
                    val decimal = new Decimal().setOrNull(unscaled, precision, scale)
                      .toJavaBigDecimal
                    if (decimal != null) {
                      addKey(pathString)
                      VariantStatsHelper.appendDecimalFixedPrecision(vb, decimal, 64)
                    }
                  case _ =>
                }
              case _ =>
            }
          case PrimitiveTypeName.FLOAT =>
            // Append Float
            stats match {
              case stat: FloatStatistics if stat.hasNonNullValue =>
                addKey(pathString)
                val statVal = genericGet(stat, statType)
                vb.appendFloat(statVal)
              case _ =>
            }
          case PrimitiveTypeName.DOUBLE =>
            // Append Double
            stats match {
              case stat: DoubleStatistics if stat.hasNonNullValue =>
                addKey(pathString)
                val statVal = genericGet(stat, statType)
                vb.appendDouble(statVal)
              case _ =>
            }
          case PrimitiveTypeName.BINARY =>
            // Look at logical type annotation. Could be string or binary
            leafPathType.getLogicalTypeAnnotation match {
              case _: StringLogicalTypeAnnotation =>
                stats match {
                  case stat: BinaryStatistics if stat.hasNonNullValue =>
                    if (statType == MIN_STAT) {
                      addKey(pathString)
                      val minString = stat.minAsString()
                      vb.appendString(minString.take(stringPrefixLength))
                    } else {
                      val maxString = stat.maxAsString()
                      if (maxString.length <= stringPrefixLength) {
                        addKey(pathString)
                        vb.appendString(maxString)
                      } else {
                        val result = maxString.take(stringPrefixLength) + maxStringTruncatedSuffix
                        if (result.compareTo(maxString) > 0) {
                          addKey(pathString)
                          vb.appendString(result)
                        }
                      }
                    }
                  case _ =>
                }
              case null =>
              // Binary - We don't collect stats on Binary columns yet
              case _ =>
            }
          case PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY =>
            // Look at logical type annotation. Could be decimal or UUID - We only collect stats for
            // Decimal since Spark does not have a UUID type
            leafPathType.getLogicalTypeAnnotation match {
              case d: DecimalLogicalTypeAnnotation =>
                // Decimal16
                val scale = d.getScale
                stats match {
                  case stat: BinaryStatistics if stat.hasNonNullValue =>
                    val unscaled = if (statType == MIN_STAT) {
                      new BigInteger(stat.genericGetMin.getBytes)
                    } else {
                      new BigInteger(stat.genericGetMax.getBytes)
                    }
                    val decimal = new JBigDecimal(unscaled, scale)
                    if (decimal != null) {
                      addKey(pathString)
                      VariantStatsHelper.appendDecimalFixedPrecision(vb, decimal, 128)
                    }
                  case _ =>
                }
              case _ =>
            }
          case _ =>
        }
      }
      if (!fields.isEmpty) {
        vb.finishWritingObject(0, fields)
        val v = vb.result()
        new VariantVal(v.getValue, v.getMetadata)
      } else {
        null
      }
    } else {
      null
    }
  }

  // Filter paths assumed to be `valid` according to `extractPathsFromFooter` to only retain the
  // paths which have the given path `prefix` as a prefix.
  def filterPathMapsByPrefix(pathStats: mutable.HashMap[Seq[String], Statistics[_]],
                             pathTypes: mutable.HashMap[Seq[String], PrimitiveType],
                             prefix: Seq[String]):
  (mutable.HashMap[Seq[String], Statistics[_]], mutable.HashMap[Seq[String], PrimitiveType]) = {
    val p1 = mutable.HashMap.empty[Seq[String], Statistics[_]]
    val p2 = mutable.HashMap.empty[Seq[String], PrimitiveType]
    pathStats.foreach {
      case (k, v) if k.startsWith(prefix) =>
        val remainingPath = k.drop(prefix.length)
        // Make sure the path does not have an array
        if (!remainingPath.containsSlice(Seq("list", "element"))) {
          p1 += (remainingPath -> v.asInstanceOf[Statistics[_]])
          p2 += (remainingPath -> pathTypes(k))
        }
      case _ =>
    }
    (p1, p2)
  }

  private val stringPrefixLength =
    SQLConf.get.getConf(DeltaSQLConf.DATA_SKIPPING_STRING_PREFIX_LENGTH)
  private val maxStringTruncatedSuffix = new String(Character.toChars(Character.MAX_CODE_POINT))
}

object VariantStatsData {
  def apply(footer: ParquetMetadata,
            dateRebaseFunc: Int => Int,
            timestampRebaseFunc: Long => Long): VariantStatsData = {
    val paths = extractPathsFromFooter(footer)
    new VariantStatsData(
      paths._1,
      paths._2,
      dateRebaseFunc,
      timestampRebaseFunc)
  }

  /**
   * Extract data about paths ending with `typed_value` from a Parquet footer.
   * A Path is considered to be valid if its companion path - the path that shares the same prefix
   * but ends with `value` instead of `typed_value` - exists and its null count equals the number of
   * records in the column.
   *
   * This function aggregates the statistics of valid paths across row groups and returns these
   * statistics as well as the column types.
   *
   * It may be possible that non-variant paths also have `typed_value` suffixes with `value`
   * companions but these will get filtered out during `filterPathMapsByPrefix`.
   */
  private def extractPathsFromFooter(footer: ParquetMetadata):
  (mutable.HashMap[Seq[String], Statistics[_]], mutable.HashMap[Seq[String], PrimitiveType]) = {
    val leafPathStatistics = mutable.HashMap[Seq[String], Statistics[_]]()
    val leafPathTypes = mutable.HashMap[Seq[String], PrimitiveType]()
    if (footer == null) return (leafPathStatistics, leafPathTypes)
    val blocks = footer.getBlocks.asScala
    // This map maintains the validity of each column path. A path is deemed invalid if the null
    // count of its companion untyped `value` column contains any non-null value.
    val leafPathValidity = mutable.HashMap[Seq[String], Boolean]()
    blocks.foreach { block =>
      val initialNumRecords = block.getRowCount
      val columns = immutable.HashMap(block.getColumns.asScala.toSeq.map {
        c => (c.getPath.asScala.toSeq, c)
      }.toSeq: _*)
      columns.filter(_._1.last == "typed_value").map {
        case (typedValuePath, typedValueColumn) =>
          val leafPath = typedValuePath.init
          val valuePath = leafPath :+ "value"
          val valueColumn = columns.get(valuePath)
          // If the companion `value` column is not present, `valueNullCount` would be zero and
          // therefore, the column would not be selected for selected for stats collection because
          // the `value` field should be all-null for it to be selected for stats collection.
          val valueNullCount = valueColumn.map(_.getStatistics.getNumNulls).getOrElse(0)
          val leafStats = typedValueColumn.getStatistics
          if (leafPathValidity.contains(leafPath)) {
            if (leafPathValidity(leafPath)) {
              if (typedValueColumn.getPrimitiveType == leafPathTypes(leafPath)) {
                // If the rescue `value` column has non-null values, we cannot collect stats on this
                // shredded column.
                leafPathValidity(leafPath) &= valueNullCount == initialNumRecords
                leafPathStatistics(leafPath).mergeStatistics(leafStats)
              } else {
                throw new IllegalStateException(s"The primitive type of the column " +
                  s"${leafPath.mkString(".")} is inconsistent across blocks.")
              }
            }
          } else {
            leafPathValidity(leafPath) = valueNullCount == initialNumRecords
            leafPathStatistics(leafPath) = leafStats
            leafPathTypes(leafPath) = typedValueColumn.getPrimitiveType
          }
      }
    }
    // Filter out invalid paths
    leafPathValidity.filterNot(_._2).foreach { case (leafPath, _) =>
      leafPathStatistics -= leafPath
      leafPathTypes -= leafPath
    }
    (leafPathStatistics, leafPathTypes)
  }
}

object VariantStatsUtils {
  // Append the path corresponding to `currentChild` to the `currentPath` array.
  // The path must only be comprised of `GetStructField` and `AttributeReference` expressions.
  // A path like `v.typed_value.name.typed_value` would be represented as
  // GetStructField(GetStructField(GetStructField(Attr(v), "typed_value"), "name"), "typed_value")
  def childPath(currentChild: Expression, currentPath: ArrayBuffer[String]): Unit = {
    currentChild match {
      case attr: AttributeReference => currentPath += attr.name
      case field: GetStructField =>
        childPath(field.child, currentPath)
        if (currentPath.nonEmpty) {
          // If the path was empty, an unknown expression was encountered on the way.
          currentPath += field.extractFieldName
        }
      case _ => throw new IllegalArgumentException(
        "The input expression VariantStatsUtils.childPath() must only be comprised of " +
          "GetStructField and AttributeReference expressions."
      )
    }
  }

  /**
   * Escapes a field name for use in a JSON path according to RFC-9535.
   *
   * This function handles escaping of problematic characters in JSON path field names:
   * - Control characters (< 0x20) are escaped as \\uXXXX
   * - Single quotes (0x27) are escaped as \'
   * - Backslashes (0x5C) are escaped as \\
   * - Special escape sequences: \b, \f, \n, \r, \t
   *
   * @param fieldName the field name to escape
   * @return the escaped field name, or the original if no escaping is needed
   */
  def escapeJsonField(fieldName: String): String = {
    if (fieldName == null || fieldName.isEmpty) {
      return fieldName
    }

    // Check if any problematic characters exist
    val hasProblematicChars = fieldName.exists { c =>
      c.toInt < maxControlCharacter || c == '\'' || c == '\\'
    }

    if (!hasProblematicChars) {
      return fieldName
    }

    val buffer = new StringBuilder(fieldName.length + 1)

    fieldName.foreach {
      case '\\' => buffer.append("\\\\")
      case '\'' => buffer.append("\\'")
      case '\b' => buffer.append("\\b")
      case '\f' => buffer.append("\\f")
      case '\n' => buffer.append("\\n")
      case '\r' => buffer.append("\\r")
      case '\t' => buffer.append("\\t")
      case c =>
        // Check if this is a control character
        val code = c.toInt
        if (code < maxControlCharacter) {
          buffer.append(f"\\u${code}%04x")
        } else {
          // Safe character, add as-is
          buffer.append(c)
        }
    }

    buffer.toString
  }

  def normalizedJsonPath(path: Seq[String]): String = {
    /*
    The path will always have `typed_value` at every alternate location like:
    "typed_value", "<root_field>", "typed_value", "<l1_field>", "typed_value", "<l2_field>", ...

    This needs to be translated into $['<root_field>']['<l1_field>']['<l2_field>']...

    The field names must be escaped properly
     */
    path.zipWithIndex.foreach {
      case (field, index) if index % 2 == 0 =>
        if (field != "typed_value") {
          throw new IllegalStateException(s"Expected field `typed_value` but found `$field`")
        }
      case _ =>
    }
    val fieldNames = path.zipWithIndex.filter(_._2 % 2 == 1).map(_._1)

    val escapedFields = fieldNames.map(escapeJsonField)
    val pathParts = escapedFields.map(field => s"['$field']")
    "$" + pathParts.mkString
  }

  val maxControlCharacter = 0x20
}
