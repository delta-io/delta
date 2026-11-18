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

import java.time.{LocalDate, LocalDateTime, OffsetDateTime}
import java.time.format.DateTimeFormatter

import scala.util.control.NonFatal

import org.apache.spark.sql.catalyst.util.ResolveDefaultColumns.CURRENT_DEFAULT_COLUMN_METADATA_KEY
import org.apache.spark.sql.types._

object DeltaColumnDefaults {
  /**
   * Check if a StructField contains default values with "DEFAULT" keyword
   * @param field the given field
   */
  def hasWriteDefault(field: StructField): Boolean =
    field.metadata.contains(CURRENT_DEFAULT_COLUMN_METADATA_KEY)

  /**
   * Check if a StructField contains literal-only default values
   * @param field the given field
   */
  def hasLiteralDefault(field: StructField): Boolean = {
    if (field.metadata.contains(CURRENT_DEFAULT_COLUMN_METADATA_KEY)) {
      val defaultValueStr = field.metadata.getString(CURRENT_DEFAULT_COLUMN_METADATA_KEY)
      isLiteral(defaultValueStr, field.dataType)
    } else {
      false
    }
  }

  /**
   * Follow Spark's string escape rule to unescape a default value string
   * @param input string to be unescaped
   * @return unescaped string
   */
  private def unescapeString(input: String): String = {
    val table = Map[Char, String](
      'b' -> "\u0008", 't' -> "\t", 'n' -> "\n", 'r' -> "\r",
      'Z' -> "\u001A", '\\' -> "\\", '%' -> "\\%", '_' -> "\\_", '\'' -> "'"
    )

    def isHex(c: Char): Boolean = Character.digit(c, 16) >= 0
    def isOct(c: Char): Boolean = c >= '0' && c <= '7'

    def hexAt(pos: Int, n: Int): String = {
      if (pos + n <= input.length && (0 until n).forall(k => isHex(input.charAt(pos + k)))) {
        val cp = Integer.parseInt(input.substring(pos, pos + n), 16)
        new String(Character.toChars(cp))
      } else null
    }

    def octAt(pos: Int): String = {
      if (pos + 3 <= input.length && (0 until 3).forall(k => isOct(input.charAt(pos + k)))) {
        val cp = Integer.parseInt(input.substring(pos, pos + 3), 8)
        if (cp <= 255) new String(Character.toChars(cp)) else null
      } else null
    }

    val out = new StringBuilder
    var i = 0
    while (i < input.length) {
      val c = input.charAt(i)
      if (c != '\\') { out.append(c); i += 1 }
      else {
        if (i + 1 >= input.length) throw new IllegalStateException("dangling escape")
        val d = input.charAt(i + 1)

        if (d >= '0' && d <= '7') {
          val oct = octAt(i + 1)
          if (oct != null) { out.append(oct); i += 4 }
          else if (d == '0') { out.append("\u0000"); i += 2 }
          else { out.append(d); i += 2 }
        } else if (d == 'u' || d == 'U') {
          val h8 = hexAt(i + 2, 8)
          val h4 = if (h8 == null) hexAt(i + 2, 4) else null
          val h = if (h8 != null) h8 else h4
          if (h != null) { out.append(h); i += (if (h8 != null) 10 else 6) }
          else { out.append(d); i += 2 } // \x => x rule
        } else {
          out.append(table.getOrElse(d, d.toString))
          i += 2
        }
      }
    }
    out.toString
  }

  private def isLiteral(str: String, dataType: DataType): Boolean = {
    def parseString(input: String): String = {
      if (input.length > 1 && ((input.head == '\'' && input.last == '\'')
        || (input.head == '"' && input.last == '"'))) {
        unescapeString(input.substring(1, input.length - 1))
      } else {
        throw new UnsupportedOperationException(s"String missing quotation marks: $input")
      }
    }
    // Parse either hex encoded literal x'....' or string literal(utf8) into binary
    def parseBinary(input: String): Unit = {
      if (input.startsWith("x") || input.startsWith("X")) {
        // Hex encoded literal
        val hexString = parseString(input.substring(1))
        hexString.sliding(1, 1).foreach(Integer.parseInt(_, 16))
      }
    }
    // Parse timestamp string without time zone info
    def parseLocalTimestamp(input: String): Unit = {
      val formats = Seq(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"),
        DateTimeFormatter.ISO_LOCAL_DATE_TIME)
      val stripped = parseString(input)
      val parsed = formats.flatMap { format =>
        try {
          Some(LocalDateTime.parse(stripped, format))
        } catch {
          case NonFatal(_) => None
        }
      }
      if (parsed.isEmpty) {
        throw new IllegalArgumentException()
      }
    }
    // Parse string with time zone info. If the input has no time zone, assume its UTC.
    def parseTimestamp(input: String): Unit = {
      val stripped = parseString(input)
      try {
        OffsetDateTime.parse(stripped, DateTimeFormatter.ISO_DATE_TIME).toInstant
      } catch {
        case NonFatal(_) => parseLocalTimestamp(input)
      }
    }

    try {
      dataType match {
        case StringType => parseString(str)
        case LongType => java.lang.Long.valueOf(str.replaceAll("[lL]$", ""))
        case IntegerType | ShortType | ByteType => Integer.valueOf(str)
        case FloatType => java.lang.Float.valueOf(str)
        case DoubleType => java.lang.Double.valueOf(str)
        // The number should be correctly formatted without need to rounding
        case d: DecimalType =>
          new java.math.BigDecimal(str, new java.math.MathContext(d.precision)).setScale(d.scale)
        case BooleanType => java.lang.Boolean.valueOf(str)
        case BinaryType => parseBinary(str)
        case DateType => LocalDate.parse(parseString(str), DateTimeFormatter.ISO_LOCAL_DATE)
        case TimestampType => parseTimestamp(str)
        case TimestampNTZType => parseLocalTimestamp(str)
        case _ =>
          throw new UnsupportedOperationException(
            s"Could not convert default value: $dataType: $str")
      }
      true
    } catch {
      case NonFatal(_) => false
    }
  }
}
