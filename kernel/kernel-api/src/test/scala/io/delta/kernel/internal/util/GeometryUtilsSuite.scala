/*
 * Copyright (2023) The Delta Lake Project Authors.
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
package io.delta.kernel.internal.util

import java.util.OptionalDouble

import io.delta.kernel.data.PointVal
import io.delta.kernel.expressions.Literal
import io.delta.kernel.internal.util.GeometryUtils.{formatPointWKT, parsePoint}
import io.delta.kernel.types.GeometryType

import org.scalatest.funsuite.AnyFunSuite

class GeometryUtilsSuite extends AnyFunSuite {

  private def checkPoint(
      wkt: String,
      x: Double,
      y: Double,
      z: Option[Double] = None,
      m: Option[Double] = None): Unit = {
    val p = parsePoint(wkt)
    assert(p.getX === x)
    assert(p.getY === y)
    assert(p.getZ.isPresent === z.isDefined)
    z.foreach(v => assert(p.getZ.getAsDouble === v))
    assert(p.getM.isPresent === m.isDefined)
    m.foreach(v => assert(p.getM.getAsDouble === v))
  }

  private def checkInvalid(wkt: String, msgFragment: String): Unit = {
    withClue(s"Expected exception for input: $wkt") {
      val e = intercept[IllegalArgumentException](parsePoint(wkt))
      assert(e.getMessage.contains(msgFragment))
    }
  }

  test("POINT (x y) - basic 2D") {
    checkPoint("POINT (1.0 2.0)", 1.0, 2.0)
    checkPoint("POINT (0.0 0.0)", 0.0, 0.0)
    checkPoint("POINT (-1.5 3.7)", -1.5, 3.7)
  }

  test("POINT Z(x y z) - 3D") {
    checkPoint("POINT Z(1.0 2.0 3.0)", 1.0, 2.0, z = Some(3.0))
    checkPoint("POINT Z(-1.0 -2.0 -3.0)", -1.0, -2.0, z = Some(-3.0))
  }

  test("POINT M(x y m) - measured") {
    checkPoint("POINT M(1.0 2.0 4.0)", 1.0, 2.0, m = Some(4.0))
  }

  test("POINT ZM(x y z m) - 4D") {
    checkPoint("POINT ZM(1.0 2.0 3.0 4.0)", 1.0, 2.0, z = Some(3.0), m = Some(4.0))
    checkPoint("POINT ZM(0.0 0.0 0.0 0.0)", 0.0, 0.0, z = Some(0.0), m = Some(0.0))
  }

  test("whitespace variations") {
    // no space before opening paren
    checkPoint("POINT(1.0 2.0)", 1.0, 2.0)
    checkPoint("POINT Z(1.0 2.0 3.0)", 1.0, 2.0, z = Some(3.0))
    // extra spaces inside parens
    checkPoint("POINT (  1.0   2.0  )", 1.0, 2.0)
    // extra spaces between keyword and modifier
    checkPoint("POINT  ZM(1.0 2.0 3.0 4.0)", 1.0, 2.0, z = Some(3.0), m = Some(4.0))
  }

  test("case insensitivity") {
    checkPoint("point (1.0 2.0)", 1.0, 2.0)
    checkPoint("POINT zm(1.0 2.0 3.0 4.0)", 1.0, 2.0, z = Some(3.0), m = Some(4.0))
    checkPoint("Point Z(1.0 2.0 3.0)", 1.0, 2.0, z = Some(3.0))
  }

  test("null input throws") {
    checkInvalid(null, "cannot be null")
  }

  test("missing parens throws") {
    checkInvalid("POINT 1.0 2.0", "Invalid WKT POINT")
    checkInvalid("POINT", "Invalid WKT POINT")
  }

  test("wrong coordinate count throws") {
    // 2D with 3 coords
    checkInvalid("POINT (1.0 2.0 3.0)", "expects 2 coordinates but got 3")
    // Z with 2 coords
    checkInvalid("POINT Z(1.0 2.0)", "expects 3 coordinates but got 2")
    // ZM with 3 coords
    checkInvalid("POINT ZM(1.0 2.0 3.0)", "expects 4 coordinates but got 3")
  }

  test("non-numeric coordinate throws") {
    checkInvalid("POINT (1.0 foo)", "Invalid coordinate")
    checkInvalid("POINT ZM(1.0 2.0 3.0 bar)", "Invalid coordinate")
  }

  test("formatPointWKT - 2D") {
    val wkt = formatPointWKT(1.0, 2.0, OptionalDouble.empty(), OptionalDouble.empty())
    assert(wkt === "POINT (1.0 2.0)")
    val p = parsePoint(wkt)
    assert(p.getX === 1.0)
    assert(p.getY === 2.0)
    assert(!p.getZ.isPresent)
    assert(!p.getM.isPresent)
  }

  test("formatPointWKT - Z") {
    val wkt = formatPointWKT(1.0, 2.0, OptionalDouble.of(3.0), OptionalDouble.empty())
    assert(wkt === "POINT Z(1.0 2.0 3.0)")
    val p = parsePoint(wkt)
    assert(p.getZ.getAsDouble === 3.0)
    assert(!p.getM.isPresent)
  }

  test("formatPointWKT - M") {
    val wkt = formatPointWKT(1.0, 2.0, OptionalDouble.empty(), OptionalDouble.of(4.0))
    assert(wkt === "POINT M(1.0 2.0 4.0)")
    val p = parsePoint(wkt)
    assert(!p.getZ.isPresent)
    assert(p.getM.getAsDouble === 4.0)
  }

  test("formatPointWKT - ZM") {
    val wkt = formatPointWKT(1.0, 2.0, OptionalDouble.of(3.0), OptionalDouble.of(4.0))
    assert(wkt === "POINT ZM(1.0 2.0 3.0 4.0)")
    val p = parsePoint(wkt)
    assert(p.getZ.getAsDouble === 3.0)
    assert(p.getM.getAsDouble === 4.0)
  }

  test("formatPointWKT - round-trip negative coords") {
    val wkt = formatPointWKT(-180.0, -90.0, OptionalDouble.empty(), OptionalDouble.empty())
    val p = parsePoint(wkt)
    assert(p.getX === -180.0)
    assert(p.getY === -90.0)
  }

  test("Literal.ofPointWKT stores PointVal with GeometryType") {
    val lit = Literal.ofPointWKT("POINT (1.0 2.0)")
    assert(lit.getDataType.isInstanceOf[GeometryType])
    val pv = lit.getValue.asInstanceOf[PointVal]
    assert(pv.getX === 1.0)
    assert(pv.getY === 2.0)
    assert(!pv.getZ.isPresent)
    assert(!pv.getM.isPresent)
  }

  test("Literal.ofPointWKT round-trip ZM") {
    val wkt = formatPointWKT(10.0, 20.0, OptionalDouble.of(30.0), OptionalDouble.of(40.0))
    val lit = Literal.ofPointWKT(wkt)
    val pv = lit.getValue.asInstanceOf[PointVal]
    assert(pv.getX === 10.0)
    assert(pv.getY === 20.0)
    assert(pv.getZ.getAsDouble === 30.0)
    assert(pv.getM.getAsDouble === 40.0)
  }
}
