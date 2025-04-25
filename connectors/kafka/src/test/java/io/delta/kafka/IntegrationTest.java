package io.delta.kafka;

import io.delta.table.DeltaCatalog;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IntegrationTest {

  private static final Logger LOG = LoggerFactory.getLogger(DeltaCatalog.class);

  @Test
  void simpleTest() {

    DeltaCatalog catalog = new DeltaCatalog();
    catalog.listNamespaces();

    LOG.info("Hello world");
  }
}
