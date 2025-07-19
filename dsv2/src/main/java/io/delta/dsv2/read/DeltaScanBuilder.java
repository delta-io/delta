package io.delta.dsv2.read;

import io.delta.dsv2.utils.SchemaUtils;
import io.delta.kernel.ResolvedTable;
import io.delta.kernel.engine.Engine;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.types.StructType;

public class DeltaScanBuilder implements ScanBuilder {
  private final ResolvedTable resolvedTable;
  private final Engine tableEngine;
  private final String accessKey;
  private final String secretKey;
  private final String sessionToken;
  private io.delta.kernel.ScanBuilder scanBuilder;
  private StructType sparkSchema;

  SparkSession spark;

  public DeltaScanBuilder(
      ResolvedTable resolvedTable,
      Engine tableEngine,
      String accessKey,
      String secretKey,
      String sessionToken,
      SparkSession spark) {
    this.resolvedTable = resolvedTable;
    this.tableEngine = tableEngine;
    this.accessKey = accessKey;
    this.secretKey = secretKey;
    this.sessionToken = sessionToken;
    this.scanBuilder = resolvedTable.getScanBuilder();
    this.sparkSchema = SchemaUtils.convertKernelSchemaToSparkSchema(resolvedTable.getSchema());
    this.spark = spark;
  }

  @Override
  public Scan build() {
    return new DeltaScan(
        scanBuilder.build(), tableEngine, sparkSchema, accessKey, secretKey, sessionToken, spark);
  }
}
