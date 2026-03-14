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

import io.delta.kernel.internal.util.GeometryUtils.{formatPointWKT, parsePointXY}

import org.scalatest.funsuite.AnyFunSuite

class GeometryUtilsSuite extends AnyFunSuite {

  private def checkXY(wkt: String, x: Double, y: Double): Unit = {
    val xy = parsePointXY(wkt)
    assert(xy(0) === x)
    assert(xy(1) === y)
  }

  private def checkInvalid(wkt: String, msgFragment: String): Unit = {
    withClue(s"Expected exception for input: $wkt") {
      val e = intercept[IllegalArgumentException](parsePointXY(wkt))
      assert(e.getMessage.contains(msgFragment))
    }
  }

  test("POINT (x y) - basic 2D") {
    checkXY("POINT (1.0 2.0)", 1.0, 2.0)
    checkXY("POINT (0.0 0.0)", 0.0, 0.0)
    checkXY("POINT (-1.5 3.7)", -1.5, 3.7)
  }

  test("POINT Z(x y z) - 3D") {
    checkXY("POINT Z(1.0 2.0 3.0)", 1.0, 2.0)
    checkXY("POINT Z(-1.0 -2.0 -3.0)", -1.0, -2.0)
  }

  test("POINT M(x y m) - measured") {
    checkXY("POINT M(1.0 2.0 4.0)", 1.0, 2.0)
  }

  test("POINT ZM(x y z m) - 4D") {
    checkXY("POINT ZM(1.0 2.0 3.0 4.0)", 1.0, 2.0)
    checkXY("POINT ZM(0.0 0.0 0.0 0.0)", 0.0, 0.0)
  }

  test("whitespace variations") {
    checkXY("POINT(1.0 2.0)", 1.0, 2.0)
    checkXY("POINT Z(1.0 2.0 3.0)", 1.0, 2.0)
    checkXY("POINT (  1.0   2.0  )", 1.0, 2.0)
    checkXY("POINT  ZM(1.0 2.0 3.0 4.0)", 1.0, 2.0)
  }

  test("case insensitivity") {
    checkXY("point (1.0 2.0)", 1.0, 2.0)
    checkXY("POINT zm(1.0 2.0 3.0 4.0)", 1.0, 2.0)
    checkXY("Point Z(1.0 2.0 3.0)", 1.0, 2.0)
  }

  test("null input throws") {
    checkInvalid(null, "cannot be null")
  }

  test("missing parens throws") {
    checkInvalid("POINT 1.0 2.0", "Invalid WKT POINT")
    checkInvalid("POINT", "Invalid WKT POINT")
  }

  test("wrong coordinate count throws") {
    checkInvalid("POINT (1.0 2.0 3.0)", "expects 2 coordinates but got 3")
    checkInvalid("POINT Z(1.0 2.0)", "expects 3 coordinates but got 2")
    checkInvalid("POINT ZM(1.0 2.0 3.0)", "expects 4 coordinates but got 3")
  }

  test("non-numeric coordinate throws") {
    checkInvalid("POINT (1.0 foo)", "Invalid coordinate")
    checkInvalid("POINT ZM(1.0 2.0 3.0 bar)", "Invalid coordinate")
  }

  test("formatPointWKT - 2D") {
    val wkt = formatPointWKT(1.0, 2.0, OptionalDouble.empty(), OptionalDouble.empty())
    assert(wkt === "POINT (1.0 2.0)")
    val xy = parsePointXY(wkt)
    assert(xy(0) === 1.0)
    assert(xy(1) === 2.0)
  }

  test("formatPointWKT - Z round-trip") {
    val wkt = formatPointWKT(1.0, 2.0, OptionalDouble.of(3.0), OptionalDouble.empty())
    assert(wkt === "POINT Z(1.0 2.0 3.0)")
    val xy = parsePointXY(wkt)
    assert(xy(0) === 1.0)
    assert(xy(1) === 2.0)
  }

  test("formatPointWKT - M round-trip") {
    val wkt = formatPointWKT(1.0, 2.0, OptionalDouble.empty(), OptionalDouble.of(4.0))
    assert(wkt === "POINT M(1.0 2.0 4.0)")
    val xy = parsePointXY(wkt)
    assert(xy(0) === 1.0)
    assert(xy(1) === 2.0)
  }

  test("formatPointWKT - ZM round-trip") {
    val wkt = formatPointWKT(1.0, 2.0, OptionalDouble.of(3.0), OptionalDouble.of(4.0))
    assert(wkt === "POINT ZM(1.0 2.0 3.0 4.0)")
    val xy = parsePointXY(wkt)
    assert(xy(0) === 1.0)
    assert(xy(1) === 2.0)
  }

  test("formatPointWKT - round-trip negative coords") {
    val wkt = formatPointWKT(-180.0, -90.0, OptionalDouble.empty(), OptionalDouble.empty())
    val xy = parsePointXY(wkt)
    assert(xy(0) === -180.0)
    assert(xy(1) === -90.0)
  }
}
