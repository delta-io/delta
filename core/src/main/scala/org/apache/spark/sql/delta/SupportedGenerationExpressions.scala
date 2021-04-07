/*
 * Copyright (2020) The Delta Lake Project Authors.
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

import scala.reflect.ClassTag

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.xml._

/** This class defines the list of expressions that can be used in a generated column. */
object SupportedGenerationExpressions {

  /**
   * This method has the same signature as `FunctionRegistry.expression` so that we can define the
   * list in the same format as `FunctionRegistry.expressions` and that's easy to diff.
   */
  private def expression[T <: Expression : ClassTag](
      name: String,
      setAlias: Boolean = false): Class[_] = {
    implicitly[ClassTag[T]].runtimeClass
  }

  // scalastyle:off
  /**
   * The white list is copied from `FunctionRegistry.expressions()` except the following types of
   * functions:
   * - explode functions. In other words, generate multiple rows from one row.
   * - aggerate functions.
   * - window functions.
   * - grouping sets.
   * - non deterministic functions.
   * - deterministic functions in one query but non deterministic in multiple queries,
   *   such as, current_timestamp, rand, etc.
   *
   * To review the difference, you can run
   * `diff sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/analysis/FunctionRegistry.scala sql/core/src/main/scala/com/databricks/sql/transaction/SupportedGenerationExpression.scala`
   */
  // scalastyle:on
  val expressions: Set[Class[_]] = Set(
    // misc non-aggregate functions
    expression[Abs]("abs"),
    expression[Coalesce]("coalesce"),
    expression[Greatest]("greatest"),
    expression[If]("if"),
    expression[If]("iff", true),
    expression[IsNaN]("isnan"),
    expression[IfNull]("ifnull"),
    expression[IsNull]("isnull"),
    expression[IsNotNull]("isnotnull"),
    expression[Least]("least"),
    expression[NaNvl]("nanvl"),
    expression[NullIf]("nullif"),
    expression[Nvl]("nvl"),
    expression[Nvl2]("nvl2"),
    expression[CaseWhen]("when"),

    // math functions
    expression[Acos]("acos"),
    expression[Acosh]("acosh"),
    expression[Asin]("asin"),
    expression[Asinh]("asinh"),
    expression[Atan]("atan"),
    expression[Atan2]("atan2"),
    expression[Atanh]("atanh"),
    expression[Bin]("bin"),
    expression[BRound]("bround"),
    expression[Cbrt]("cbrt"),
    expression[Ceil]("ceil"),
    expression[Ceil]("ceiling", true),
    expression[Cos]("cos"),
    expression[Cosh]("cosh"),
    expression[Conv]("conv"),
    expression[ToDegrees]("degrees"),
    expression[EulerNumber]("e"),
    expression[Exp]("exp"),
    expression[Expm1]("expm1"),
    expression[Floor]("floor"),
    expression[Factorial]("factorial"),
    expression[Hex]("hex"),
    expression[Hypot]("hypot"),
    expression[Logarithm]("log"),
    expression[Log10]("log10"),
    expression[Log1p]("log1p"),
    expression[Log2]("log2"),
    expression[Log]("ln"),
    expression[Remainder]("mod", true),
    expression[UnaryMinus]("negative", true),
    expression[Pi]("pi"),
    expression[Pmod]("pmod"),
    expression[UnaryPositive]("positive"),
    expression[Pow]("pow", true),
    expression[Pow]("power"),
    expression[ToRadians]("radians"),
    expression[Rint]("rint"),
    expression[Round]("round"),
    expression[ShiftLeft]("shiftleft"),
    expression[ShiftRight]("shiftright"),
    expression[ShiftRightUnsigned]("shiftrightunsigned"),
    expression[Signum]("sign", true),
    expression[Signum]("signum"),
    expression[Sin]("sin"),
    expression[Sinh]("sinh"),
    expression[StringToMap]("str_to_map"),
    expression[Sqrt]("sqrt"),
    expression[Tan]("tan"),
    expression[Cot]("cot"),
    expression[Tanh]("tanh"),

    expression[Add]("+"),
    expression[Subtract]("-"),
    expression[Multiply]("*"),
    expression[Divide]("/"),
    expression[IntegralDivide]("div"),
    expression[Remainder]("%"),

    // string functions
    expression[Ascii]("ascii"),
    expression[Chr]("char", true),
    expression[Chr]("chr"),
    expression[Base64]("base64"),
    expression[BitLength]("bit_length"),
    expression[Length]("char_length", true),
    expression[Length]("character_length", true),
    expression[ConcatWs]("concat_ws"),
    expression[Decode]("decode"),
    expression[Elt]("elt"),
    expression[Encode]("encode"),
    expression[FindInSet]("find_in_set"),
    expression[FormatNumber]("format_number"),
    expression[FormatString]("format_string"),
    expression[GetJsonObject]("get_json_object"),
    expression[InitCap]("initcap"),
    expression[StringInstr]("instr"),
    expression[Lower]("lcase", true),
    expression[Length]("length"),
    expression[Levenshtein]("levenshtein"),
    expression[Like]("like"),
    expression[Lower]("lower"),
    expression[OctetLength]("octet_length"),
    expression[StringLocate]("locate"),
    expression[StringLPad]("lpad"),
    expression[StringTrimLeft]("ltrim"),
    expression[JsonTuple]("json_tuple"),
    expression[ParseUrl]("parse_url"),
    expression[StringLocate]("position", true),
    expression[StringLocate]("charindex", true),
    expression[FormatString]("printf", true),
    expression[RegExpExtract]("regexp_extract"),
    expression[RegExpReplace]("regexp_replace"),
    expression[RLike]("regexp_like", true),
    expression[StringRepeat]("repeat"),
    expression[StringReplace]("replace"),
    expression[Overlay]("overlay"),
    expression[RLike]("rlike"),
    expression[StringRPad]("rpad"),
    expression[StringTrimRight]("rtrim"),
    expression[Sentences]("sentences"),
    expression[SoundEx]("soundex"),
    expression[StringSpace]("space"),
    expression[StringSplit]("split"),
    expression[Substring]("substr", true),
    expression[Substring]("substring"),
    expression[Left]("left"),
    expression[Right]("right"),
    expression[SubstringIndex]("substring_index"),
    expression[StringTranslate]("translate"),
    expression[StringTrim]("trim"),
    expression[Upper]("ucase", true),
    expression[UnBase64]("unbase64"),
    expression[Unhex]("unhex"),
    expression[Upper]("upper"),
    expression[XPathList]("xpath"),
    expression[XPathBoolean]("xpath_boolean"),
    expression[XPathDouble]("xpath_double"),
    expression[XPathDouble]("xpath_number", true),
    expression[XPathFloat]("xpath_float"),
    expression[XPathInt]("xpath_int"),
    expression[XPathLong]("xpath_long"),
    expression[XPathShort]("xpath_short"),
    expression[XPathString]("xpath_string"),

    // datetime functions
    expression[AddMonths]("add_months"),
    expression[DateDiff]("datediff"),
    expression[DateAdd]("date_add"),
    expression[DateFormatClass]("date_format"),
    expression[DateSub]("date_sub"),
    expression[DayOfMonth]("day", true),
    expression[DayOfYear]("dayofyear"),
    expression[DayOfMonth]("dayofmonth"),
    expression[FromUnixTime]("from_unixtime"),
    expression[FromUTCTimestamp]("from_utc_timestamp"),
    expression[Hour]("hour"),
    expression[LastDay]("last_day"),
    expression[Minute]("minute"),
    expression[Month]("month"),
    expression[MonthsBetween]("months_between"),
    expression[NextDay]("next_day"),
    expression[Now]("now"),
    expression[Quarter]("quarter"),
    expression[Second]("second"),
    expression[ParseToTimestamp]("to_timestamp"),
    expression[ParseToDate]("to_date"),
    expression[ToUnixTimestamp]("to_unix_timestamp"),
    expression[ToUTCTimestamp]("to_utc_timestamp"),
    expression[TruncDate]("trunc"),
    expression[TruncTimestamp]("date_trunc"),
    expression[UnixTimestamp]("unix_timestamp"),
    expression[DayOfWeek]("dayofweek"),
    expression[WeekDay]("weekday"),
    expression[WeekOfYear]("weekofyear"),
    expression[Year]("year"),
    expression[TimeWindow]("window"),
    expression[MakeDate]("make_date"),
    expression[MakeTimestamp]("make_timestamp"),
    expression[MakeInterval]("make_interval"),
    expression[DatePart]("date_part"),
    expression[Extract]("extract"),

    // collection functions
    expression[CreateArray]("array"),
    expression[ArrayContains]("array_contains"),
    expression[ArraysOverlap]("arrays_overlap"),
    expression[ArrayIntersect]("array_intersect"),
    expression[ArrayJoin]("array_join"),
    expression[ArrayPosition]("array_position"),
    expression[ArraySort]("array_sort"),
    expression[ArrayExcept]("array_except"),
    expression[ArrayUnion]("array_union"),
    expression[CreateMap]("map"),
    expression[CreateNamedStruct]("named_struct"),
    expression[ElementAt]("element_at"),
    expression[MapFromArrays]("map_from_arrays"),
    expression[MapKeys]("map_keys"),
    expression[MapValues]("map_values"),
    expression[MapEntries]("map_entries"),
    expression[MapFromEntries]("map_from_entries"),
    expression[MapConcat]("map_concat"),
    expression[Size]("size"),
    expression[Slice]("slice"),
    expression[Size]("cardinality", true),
    expression[ArraysZip]("arrays_zip"),
    expression[SortArray]("sort_array"),
    expression[ArrayMin]("array_min"),
    expression[ArrayMax]("array_max"),
    expression[Reverse]("reverse"),
    expression[Concat]("concat"),
    expression[Flatten]("flatten"),
    expression[Sequence]("sequence"),
    expression[ArrayRepeat]("array_repeat"),
    expression[ArrayRemove]("array_remove"),
    expression[ArrayDistinct]("array_distinct"),
    expression[ArrayTransform]("transform"),
    expression[MapFilter]("map_filter"),
    expression[ArrayFilter]("filter"),
    expression[ArrayExists]("exists"),
    expression[ArrayForAll]("forall"),
    expression[ArrayAggregate]("aggregate"),
    expression[ArrayAggregate]("reduce"),
    expression[TransformValues]("transform_values"),
    expression[TransformKeys]("transform_keys"),
    expression[MapZipWith]("map_zip_with"),
    expression[ZipWith]("zip_with"),

    // misc functions
    expression[AssertTrue]("assert_true"),
    expression[Crc32]("crc32"),
    expression[Md5]("md5"),
    expression[Murmur3Hash]("hash"),
    expression[XxHash64]("xxhash64"),
    expression[Sha1]("sha", true),
    expression[Sha1]("sha1"),
    expression[Sha2]("sha2"),
    expression[TypeOf]("typeof"),

    // predicates
    expression[And]("and"),
    expression[In]("in"),
    expression[Not]("not"),
    expression[Or]("or"),

    // comparison operators
    expression[EqualNullSafe]("<=>"),
    expression[EqualTo]("="),
    expression[EqualTo]("=="),
    expression[GreaterThan](">"),
    expression[GreaterThanOrEqual](">="),
    expression[LessThan]("<"),
    expression[LessThanOrEqual]("<="),
    expression[Not]("!"),

    // bitwise
    expression[BitwiseAnd]("&"),
    expression[BitwiseNot]("~"),
    expression[BitwiseOr]("|"),
    expression[BitwiseXor]("^"),
    expression[BitwiseCount]("bit_count"),

    // json
    expression[StructsToJson]("to_json"),
    expression[JsonToStructs]("from_json"),
    expression[SchemaOfJson]("schema_of_json"),

    // cast
    expression[Cast]("cast"),
    // We don't need to define `castAlias` since they will use the same `Cast` expression.

    // csv
    expression[CsvToStructs]("from_csv"),
    expression[SchemaOfCsv]("schema_of_csv"),
    expression[StructsToCsv]("to_csv"),

    // Special expressions that are not built-in expressions.
    expression[AttributeReference]("col"),
    expression[Literal]("lit")
  )
}
