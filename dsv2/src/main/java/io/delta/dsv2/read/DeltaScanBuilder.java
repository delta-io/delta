package io.delta.dsv2.read;

import io.delta.dsv2.utils.SchemaUtils;
import io.delta.kernel.ResolvedTable;
import io.delta.kernel.engine.Engine;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DeltaScanBuilder implements ScanBuilder {

  private static final Logger logger = LoggerFactory.getLogger(DeltaScanBuilder.class);

  private final ResolvedTable resolvedTable;
  private final Engine tableEngine;
  private io.delta.kernel.ScanBuilder scanBuilder;
  private StructType sparkSchema;

  public DeltaScanBuilder(ResolvedTable resolvedTable, Engine tableEngine) {
    this.resolvedTable = resolvedTable;
    this.tableEngine = tableEngine;
    this.scanBuilder = resolvedTable.getScanBuilder();
    this.sparkSchema = SchemaUtils.convertKernelSchemaToSparkSchema(resolvedTable.getSchema());

    logger.info(
        "Constructed DeltaScanBuilder for "
            + resolvedTable.getPath()
            + " at read "
            + "version "
            + resolvedTable.getVersion());
  }

  @Override
  public Scan build() {
    return new DeltaScan(scanBuilder.build(), tableEngine, sparkSchema);
  }
}
