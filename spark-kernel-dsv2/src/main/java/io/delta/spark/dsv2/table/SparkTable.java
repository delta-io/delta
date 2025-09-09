/*
 * Copyright (2025) The Delta Lake Project Authors.
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
package io.delta.spark.dsv2.table;

import static java.util.Objects.requireNonNull;

import io.delta.kernel.internal.SnapshotImpl;
import io.delta.spark.dsv2.read.SparkScanBuilder;
import io.delta.spark.dsv2.utils.SchemaUtils;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.connector.catalog.*;
import org.apache.spark.sql.connector.expressions.Expressions;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/** DataSource V2 Table implementation for Delta Lake using the Delta Kernel API. */
public class SparkTable implements Table, SupportsRead {

  private static final Set<TableCapability> CAPABILITIES =
      Collections.unmodifiableSet(EnumSet.of(TableCapability.BATCH_READ));

  private final Identifier identifier;
  private final String tablePath;
  // TODO: [delta-io/delta#5029] Add getProperties() in snapshot to avoid using Impl class.
  private final SnapshotImpl snapshot;
  private final Configuration hadoopConf;

  private final StructType schema;
  private final List<String> partColNames;
  private final StructType dataSchema;
  private final StructType partitionSchema;
  private final Column[] columns;
  private final Transform[] partitionTransforms;

  /**
   * Creates a new DeltaKernelTable instance.
   *
   * @param identifier the table identifier
   * @param tablePath the table path of the Delta table
   * @param hadoopConf the Hadoop configuration to use for creating the engine
   */
  public SparkTable(Identifier identifier, String tablePath, Configuration hadoopConf) {
    this.identifier = requireNonNull(identifier, "identifier is null");
    this.tablePath = requireNonNull(tablePath, "snapshot is null");
    this.hadoopConf = requireNonNull(hadoopConf, "hadoop conf is null");
    this.snapshot =
        (SnapshotImpl)
            io.delta.kernel.TableManager.loadSnapshot(tablePath)
                .build(io.delta.kernel.defaults.engine.DefaultEngine.create(hadoopConf));

    this.schema = SchemaUtils.convertKernelSchemaToSparkSchema(snapshot.getSchema());
    this.partColNames =
        Collections.unmodifiableList(new ArrayList<>(snapshot.getPartitionColumnNames()));

    final Set<String> partitionColumnSet = new HashSet<>(partColNames);
    final List<StructField> dataFields = new ArrayList<>();
    final List<StructField> partitionFields = new ArrayList<>();

    for (StructField field : schema.fields()) {
      (partitionColumnSet.contains(field.name()) ? partitionFields : dataFields).add(field);
    }

    this.dataSchema = new StructType(dataFields.toArray(new StructField[0]));
    this.partitionSchema = new StructType(partitionFields.toArray(new StructField[0]));
    this.columns = CatalogV2Util.structTypeToV2Columns(schema);
    this.partitionTransforms =
        partColNames.stream().map(Expressions::identity).toArray(Transform[]::new);
  }

  @Override
  public String name() {
    return identifier.name();
  }

  @Override
  public StructType schema() {
    return schema;
  }

  @Override
  public Column[] columns() {
    return columns;
  }

  @Override
  public Transform[] partitioning() {
    return partitionTransforms;
  }

  @Override
  public Map<String, String> properties() {
    return Collections.unmodifiableMap(new HashMap<>(snapshot.getMetadata().getConfiguration()));
  }

  @Override
  public Set<TableCapability> capabilities() {
    return CAPABILITIES;
  }

  @Override
  public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
    return new SparkScanBuilder(
        name(), tablePath, dataSchema, partitionSchema, snapshot, hadoopConf);
  }

  @Override
  public String toString() {
    return "SparkTable{identifier=" + identifier + '}';
  }
}
