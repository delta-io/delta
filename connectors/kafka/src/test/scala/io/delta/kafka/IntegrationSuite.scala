package io.delta.kafka

import io.delta.table.DeltaCatalog
import org.scalatest.funsuite.AnyFunSuite
import org.slf4j.LoggerFactory

class IntegrationSuite extends AnyFunSuite {

  private val LOG = LoggerFactory.getLogger(classOf[IntegrationSuite])

  test("normal test") {
    LOG.info("hello world")
  }
}
