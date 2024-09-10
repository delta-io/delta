package io.delta.kernel.types

import org.scalatest.funsuite.AnyFunSuite

class StringTypeSuite extends AnyFunSuite {
  test("check equals") {
    Seq(
      (
        StringType.STRING,
        StringType.STRING,
        true
      ),
      (
        StringType.STRING,
        new StringType("sPark.UTF8_bINary"),
        true
      ),
      (
        StringType.STRING,
        new StringType("SPARK.UTF8_LCASE"),
        false
      ),
      (
        new StringType("ICU.UNICODE"),
        new StringType("SPARK.UTF8_LCASE"),
        false
      ),
      (
        new StringType("ICU.UNICODE"),
        new StringType("ICU.UNICODE_CI"),
        false
      ),
      (
        new StringType("ICU.UNICODE_CI"),
        new StringType("icU.uniCODe_Ci"),
        true
      )
    ).foreach {
      case (st1, st2, expResult) =>
        assert(st1.equals(st2) == expResult)
    }
  }
}
