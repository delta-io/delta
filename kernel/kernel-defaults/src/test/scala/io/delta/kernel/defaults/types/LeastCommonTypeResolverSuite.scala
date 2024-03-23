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
package io.delta.kernel.defaults.types

import scala.collection.JavaConverters._
import io.delta.kernel.defaults.internal.types.LeastCommonTypeResolver
import io.delta.kernel.defaults.utils.TestUtils
import io.delta.kernel.types._
import org.scalatest.funsuite.AnyFunSuite
import scala.collection.JavaConverters._

class LeastCommonTypeResolverSuite extends AnyFunSuite with TestUtils {

  test("test incompatible least common type") {
    Seq(BinaryType.BINARY,
      BooleanType.BOOLEAN,
      DateType.DATE,
      TimestampType.TIMESTAMP).foreach(numberIncompatibleType => {
      var typeListToList: Seq[DataType] = Seq(numberIncompatibleType)
      Seq(ByteType.BYTE,
        ShortType.SHORT,
        IntegerType.INTEGER,
        LongType.LONG,
        FloatType.FLOAT,
        DoubleType.DOUBLE).foreach(numberType => {
        typeListToList = typeListToList :+ numberType
        val javaTypeList : java.util.List[DataType] = typeListToList.asJava
        val ex = intercept[IllegalStateException] {
          LeastCommonTypeResolver.resolveLeastCommonType(javaTypeList)
        }
      })
    })
  }

  test("test least common type for number") {
    var numberTypes: Seq[DataType] = Seq()
    Seq(ByteType.BYTE,
      ShortType.SHORT,
      IntegerType.INTEGER,
      LongType.LONG,
      FloatType.FLOAT,
      DoubleType.DOUBLE).foreach(numberType => {
      numberTypes = numberTypes :+ numberType
      val javaTypeList : java.util.List[DataType] = numberTypes.asJava
      val leastCommonType: DataType =
        LeastCommonTypeResolver.resolveLeastCommonType(javaTypeList)
      assert(numberType == leastCommonType)
    })
  }

  test("test least common type for string and number") {
    var longLeastCommonTypes: Seq[DataType] = Seq(StringType.STRING)
    Seq(ByteType.BYTE,
      ShortType.SHORT,
      IntegerType.INTEGER,
      LongType.LONG).foreach(numberType => {
      longLeastCommonTypes = longLeastCommonTypes :+ numberType
      val javaTypeList : java.util.List[DataType] = longLeastCommonTypes.asJava
      val leastCommonType: DataType =
        LeastCommonTypeResolver.resolveLeastCommonType(javaTypeList)
      assert(LongType.LONG == leastCommonType)
    })

    var doubleLeastCommonTypes: Seq[DataType] = Seq(StringType.STRING)
    Seq(FloatType.FLOAT,
      DoubleType.DOUBLE).foreach(numberType => {
      doubleLeastCommonTypes = doubleLeastCommonTypes :+ numberType
      val javaTypeList : java.util.List[DataType] = doubleLeastCommonTypes.asJava
      val leastCommonType: DataType =
        LeastCommonTypeResolver.resolveLeastCommonType(javaTypeList)
      assert(DoubleType.DOUBLE == leastCommonType)
    })
  }
}
