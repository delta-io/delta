package io.delta.flink.table

import java.net.URI

import org.scalatest.funsuite.AnyFunSuite

class AbstractKernelTableSuite extends AnyFunSuite {

  test("normalize") {
    assert(AbstractKernelTable.normalize(URI.create("file:/var")).toString == "file:///var/")
    assert(AbstractKernelTable.normalize(URI.create("file:///var")).toString == "file:///var/")
    assert(AbstractKernelTable.normalize(URI.create("s3://host/var")).toString == "s3://host/var/")
  }
}
