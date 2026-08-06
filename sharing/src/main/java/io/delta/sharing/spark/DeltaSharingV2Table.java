/*
 * Copyright (2021) The Delta Lake Project Authors.
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

package io.delta.sharing.spark;

import static io.delta.spark.internal.v2.utils.ScalaUtils.toJavaOptional;
import static io.delta.spark.internal.v2.utils.StatsUtils.toV2Statistics;

import org.apache.spark.sql.delta.RowTracking$;
import io.delta.spark.internal.v2.shims.CatalogV2UtilShims;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.connector.catalog.Column;
import org.apache.spark.sql.connector.catalog.MetadataColumn;
import org.apache.spark.sql.connector.catalog.SupportsMetadataColumns;
import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.catalog.V2TableWithV1Fallback;
import org.apache.spark.sql.connector.expressions.Expressions;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.read.Statistics;
import org.apache.spark.sql.execution.datasources.FileFormat$;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.jdk.javaapi.CollectionConverters;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;

/**
 * A DSv2 {@link Table} over a Delta Sharing shared table, returned from {@code
 * AbstractDeltaCatalog.loadTable} when {@code spark.sql.delta.sharing.dsv2.enabled} is set and the
 * shared table is a delta format batch snapshot (see {@link
 * DeltaSharingDSV2Utils#resolveBatchSnapshotContext}).
 *
 * <p>The per-table context (sharing client + getMetadata response) is built by the routing gate
 * ({@link DeltaSharingDSV2Utils#resolveBatchSnapshotContext}) and handed to the constructor, so
 * the read reuses it without re-fetching metadata.
 *
 * <p>Scope: delta format batch snapshot reads only. Reads the DSv2 path does not serve fall back to
 * the V1 Delta Sharing connector, unchanged.
 */
public class DeltaSharingV2Table
    implements Table,
        SupportsRead,
        SupportsMetadataColumns,
        V2TableWithV1Fallback {

  private static final Logger LOG = LoggerFactory.getLogger(DeltaSharingV2Table.class);

  // Signals to Spark what features the Table supports.
  // Signals that the table doesn't support streaming, which will trigger fallback to V1.
  private static final Set<TableCapability> CAPABILITIES =
      Collections.unmodifiableSet(EnumSet.of(TableCapability.BATCH_READ));

  private static final String METADATA_COLUMN_NAME = FileFormat$.MODULE$.METADATA_NAME();

  private final CatalogTable catalogTable;
  private final SparkSession spark;

  // The per-table context (sharing client + getMetadata response), resolved by the catalog routing
  // (resolveBatchSnapshotContext) and reused so the read does not re-fetch getMetadata.
  private final DeltaSharingV2TableContext context;

  // The Delta Kernel engine (a DefaultEngine over the session hadoopConf), built once here and
  // reused across scans -- mirroring DeltaV2Table's kernelEngine, rather than rebuilt per scan.
  private final Engine engine;

  /**
   * @param context the context resolved by the catalog routing ({@link
   *     DeltaSharingDSV2Utils#resolveBatchSnapshotContext}), reused so the read does not re-issue
   *     getMetadata.
   */
  public DeltaSharingV2Table(
      CatalogTable catalogTable, SparkSession spark, DeltaSharingV2TableContext context) {
    this.catalogTable = catalogTable;
    this.spark = spark;
    this.context = context;
    // Register the `delta-sharing-log://` FS scheme before snapshotting the session hadoopConf into
    // the engine, so the conf it captures can resolve the synthetic log the scan builds.
    DeltaSharingDataSource.setupFileSystem(spark.sqlContext());
    this.engine = DefaultEngine.create(spark.sessionState().newHadoopConf());
    LOG.info("DSV2-Sharing: DeltaSharingV2Table constructed for {}",
        catalogTable.identifier().unquotedString());
  }

  public CatalogTable getCatalogTable() {
    return catalogTable;
  }


  /**
   * V1 fallback for read capabilities this V2 table does not implement. Because {@link
   * #capabilities()} advertises only {@code BATCH_READ}, a streaming read resolves through this.
   */
  @Override
  public CatalogTable v1Table() {
    return catalogTable;
  }

  @Override
  public String name() {
    return catalogTable.identifier().unquotedString();
  }

  @Override
  public StructType schema() {
    return context.dsMeta().metadata().schema();
  }

  @Override
  public Column[] columns() {
    return CatalogV2UtilShims.structTypeToV2Columns(schema());
  }

  @Override
  public Transform[] partitioning() {
    // Partition column values are injected by the reader (from the synthetic delta log), so this is
    // not load-bearing for read correctness; it reports identity transforms purely for optimizer
    // partition awareness (e.g. dynamic file pruning), mirroring DeltaV2Table.
    return Arrays.stream(context.dsMeta().metadata().partitionSchema().fieldNames())
        .map(Expressions::identity)
        .toArray(Transform[]::new);
  }

  @Override
  public Map<String, String> properties() {
    // Display-only (DESCRIBE EXTENDED / SHOW TBLPROPERTIES); no read path consumes this. Mirrors
    // DeltaV2Table: the shared table's delta.* config plus provider/location/comment.
    return DeltaSharingDSV2Utils.tableProperties(context);
  }

  @Override
  public Set<TableCapability> capabilities() {
    return CAPABILITIES;
  }

  /**
   * Exposes a single {@code _metadata} struct column: the six file-source base metadata fields from
   * {@code FileFormat.BASE_METADATA_FIELDS} (file_path, file_name, file_size, file_block_start,
   * file_block_length, file_modification_time), plus the four row-tracking fields (row_id,
   * base_row_id, default_row_commit_version, row_commit_version) when row tracking is enabled on
   * the shared table. The same SparkScan read path populates them.
   *
   * <p>{@code file_path} is the opaque {@code delta-sharing:///} id from the synthetic log, not the
   * pre-signed URL (held separately in the CachedTableManager id-to-URL map).
   */
  @Override
  public MetadataColumn[] metadataColumns() {
    StructType metadataType = new StructType();
    for (StructField field :
        CollectionConverters.asJava(FileFormat$.MODULE$.BASE_METADATA_FIELDS())) {
      metadataType = metadataType.add(field);
    }
    for (StructField field :
        CollectionConverters.asJava(
            RowTracking$.MODULE$.createMetadataStructFields(
                context.dsMeta().protocol().deltaProtocol(),
                context.dsMeta().metadata().deltaMetadata(),
                /* nullableConstantFields= */ false,
                /* nullableGeneratedFields= */ false))) {
      metadataType = metadataType.add(field);
    }
    final StructType finalMetadataType = metadataType;

    MetadataColumn[] columns = new MetadataColumn[1];
    columns[0] =
        new MetadataColumn() {
          @Override
          public String name() {
            return METADATA_COLUMN_NAME;
          }

          @Override
          public DataType dataType() {
            return finalMetadataType;
          }

          @Override
          public boolean isNullable() {
            return false;
          }
        };
    return columns;
  }

  // Cached lazily on first call, mirroring DeltaV2Table. Inputs (catalogTable, the context's
  // schemas) are immutable for a given DeltaSharingV2Table, so concurrent computation by racing
  // optimizer passes yields the same value -- volatile is enough for visibility, no lock needed.
  private volatile Optional<Statistics> cachedCatalogStats;

  private Optional<Statistics> computeCatalogStats() {
    Optional<Statistics> cached = cachedCatalogStats;
    if (cached != null) {
      return cached;
    }
    Optional<Statistics> computed =
        toJavaOptional(catalogTable.stats())
            .map(
                stats ->
                    toV2Statistics(
                        stats,
                        context.dsMeta().metadata().deltaMetadata().dataSchema(),
                        context.dsMeta().metadata().partitionSchema()));
    cachedCatalogStats = computed;
    return computed;
  }


  @Override
  public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
    LOG.info("DSV2-Sharing: newScanBuilder for {}", name());
    // Time travel is resolved before this point, so no time-travel option reaches here as a scan
    // option. A CDF (readChangeFeed) read is redirected to the V1 sharing path by DeltaAnalysis
    // before planning.
    return new DeltaSharingScanBuilder(spark, context, engine, computeCatalogStats());
  }
}
