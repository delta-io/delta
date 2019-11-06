package io.delta.hive.test

import org.scalatest.Ignore

// TODO Figure out why running this test will cause other tests fail. Probably due to some unknonw
// dependency conflicts.
@Ignore
class HiveTestSuite extends HiveTest {

  test("basic hive query") {
    runQuery("create database testdb1")
    runQuery("use testdb1")
    runQuery("create table testtbl1 as select 'foo' as key")
    assert(runQuery("select * from testdb1.testtbl1") === Seq("foo"))
  }
}
