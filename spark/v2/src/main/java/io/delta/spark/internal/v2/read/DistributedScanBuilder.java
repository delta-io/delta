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
package io.delta.spark.internal.v2.read;

import static java.util.Objects.requireNonNull;

import io.delta.kernel.PaginatedScan;
import io.delta.kernel.Scan;
import io.delta.kernel.ScanBuilder;
import io.delta.kernel.Snapshot;
import io.delta.kernel.data.Row;
import io.delta.kernel.expressions.Predicate;
import io.delta.kernel.internal.SnapshotImpl;
import io.delta.kernel.internal.snapshot.LogSegment;
import io.delta.kernel.utils.FileStatus;
import io.delta.spark.internal.v2.utils.SchemaUtils;
import java.util.*;
import java.util.stream.Collectors;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.delta.sources.DeltaSQLConf;
import org.apache.spark.sql.delta.stats.*;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Function0;
import scala.Function1;
import scala.Option;
import scala.collection.JavaConverters;

/**
 * Kernel-compatible {@link ScanBuilder} that performs Spark-level data skipping by orchestrating V1
 * shared components on a distributed DataFrame.
 *
 * <p>The pipeline mirrors V1's {@code DataSkippingReaderBase.filesForScan}:
 *
 * <ol>
 *   <li>Extract {@link LogSegment} from Kernel {@link SnapshotImpl} (checkpoint + delta paths)
 *   <li>{@link DefaultStateProvider#fromPaths} — state reconstruction + stats parsing
 *   <li>{@link StatsColumnResolver} + {@link DefaultDeltaScanExecutor} — shared components
 *   <li>{@link DefaultDataSkippingFilterPlanner} — classify filters into partition / data /
 *       ineligible
 *   <li>{@link DefaultDeltaScanExecutor} DF methods — execute the chosen path
 *   <li>Wrap the filtered DataFrame in a {@link DistributedScan} (zero-copy)
 * </ol>
 *
 * @see DistributedScan the Scan produced by this builder (zero-copy Spark Row to Kernel Row)
 */
public class DistributedScanBuilder implements ScanBuilder {

  private final SparkSession spark;
  private final Snapshot snapshot;
  private final int numPartitions;

  // Mutable builder state — set via fluent methods before build()
  private io.delta.kernel.types.StructType readSchema;
  private Expression[] pushdownExpressions;

  /**
   * Creates a ScanBuilder for distributed batch reads.
   *
   * @param spark SparkSession
   * @param snapshot Kernel snapshot (must be SnapshotImpl to access LogSegment)
   * @param numPartitions Number of Spark partitions for state reconstruction
   */
  public DistributedScanBuilder(SparkSession spark, Snapshot snapshot, int numPartitions) {
    this.spark = requireNonNull(spark, "spark");
    this.snapshot = requireNonNull(snapshot, "snapshot");
    this.numPartitions = numPartitions;
    this.readSchema = snapshot.getSchema();
    this.pushdownExpressions = new Expression[0];
  }

  /**
   * Sets the pushdown expressions (partition + data) for the scan.
   *
   * @param expressions Catalyst expressions from Spark's optimizer (non-null)
   * @return this builder for chaining
   */
  public DistributedScanBuilder withPushdownExpressions(Expression[] expressions) {
    this.pushdownExpressions = requireNonNull(expressions, "expressions").clone();
    return this;
  }

  @Override
  public ScanBuilder withFilter(Predicate predicate) {
    return this; // Kernel predicates unused; filtering via V1 pipeline
  }

  @Override
  public ScanBuilder withReadSchema(io.delta.kernel.types.StructType readSchema) {
    this.readSchema = requireNonNull(readSchema, "readSchema");
    return this;
  }

  /** Builds the scan: state reconstruction → filter planning → execution → zero-copy wrap. */
  @Override
  public Scan build() {
    // ---- 1. Extract file paths from LogSegment ----
    if (!(snapshot instanceof SnapshotImpl)) {
      throw new IllegalArgumentException("Snapshot must be SnapshotImpl to access LogSegment");
    }
    SnapshotImpl snapshotImpl = (SnapshotImpl) snapshot;
    LogSegment logSegment = snapshotImpl.getLogSegment();

    String[] checkpointPaths =
        logSegment.getCheckpoints().stream().map(FileStatus::getPath).toArray(String[]::new);
    long checkpointVersion = logSegment.getCheckpointVersionOpt().orElse(-1L);

    List<FileStatus> deltas = logSegment.getDeltas();
    String[] deltaPaths = deltas.stream().map(FileStatus::getPath).toArray(String[]::new);
    long[] deltaVersions =
        deltas.stream()
            .mapToLong(fs -> DefaultStateProvider.extractDeltaVersion(fs.getPath()))
            .toArray();

    // ---- 2. Schema conversion ----
    StructType sparkTableSchema =
        SchemaUtils.convertKernelSchemaToSparkSchema(snapshot.getSchema());
    List<String> partitionColumnNames = snapshot.getPartitionColumnNames();
    StructType partSchema = buildPartitionSchema(sparkTableSchema, partitionColumnNames);
    StructType statsSchema = StatsColumnResolver.buildStatsSchema(sparkTableSchema, false);

    // ---- 3. State reconstruction (encapsulates load + canonicalize) ----
    DefaultStateProvider stateProvider =
        DefaultStateProvider.fromPaths(
            spark,
            checkpointPaths,
            checkpointVersion,
            deltaPaths,
            deltaVersions,
            numPartitions,
            statsSchema);

    // ---- 4. Build shared components (mirrors DataSkippingReaderBase lazy vals) ----
    StatsColumnResolver statsColumnResolver =
        new StatsColumnResolver(functions.col("stats"), statsSchema, sparkTableSchema);
    Function1<StatsColumn, Option<Column>> getStatsColumnOpt =
        statsColumnResolver::getStatsColumnOpt;

    // By-name parameters in Scala compile to Function0 in bytecode.
    // V2 only uses DF methods (getAllFilesAsDF, filterOnPartitionsAsDF,
    // getDataSkippedFilesAsDF) which access `withStats` but never `allFiles`,
    // so we pass a no-op for the latter.
    Function0<Dataset<org.apache.spark.sql.Row>> withStats =
        stateProvider::allAddFilesWithParsedStats;
    @SuppressWarnings("unchecked")
    Function0<Dataset<org.apache.spark.sql.delta.actions.AddFile>> allFilesNoop =
        (Function0<Dataset<org.apache.spark.sql.delta.actions.AddFile>>) () -> null;

    DefaultDeltaScanExecutor executor =
        new DefaultDeltaScanExecutor(spark, withStats, allFilesNoop, getStatsColumnOpt, partSchema);

    scala.collection.immutable.Seq<Expression> filterExprs =
        JavaConverters.asScalaBufferConverter(Arrays.asList(pushdownExpressions))
            .asScala()
            .toList();

    // ---- 5. Early exit: no filters → all files ----
    if (filterExprs.isEmpty()) {
      return new DistributedScan(executor.getAllFilesAsDF(), snapshot, readSchema);
    }

    // ---- 6. PLAN: classify filters (mirrors filesForScan step 1) ----
    boolean useStats =
        (Boolean) spark.sessionState().conf().getConf(DeltaSQLConf.DELTA_STATS_SKIPPING());
    scala.collection.immutable.Seq<String> partColsScala =
        JavaConverters.asScalaBufferConverter(partitionColumnNames).asScala().toList();
    @SuppressWarnings("unchecked")
    scala.collection.immutable.Seq<String> emptySeq =
        (scala.collection.immutable.Seq<String>)
            (scala.collection.immutable.Seq<?>) scala.collection.immutable.Nil$.MODULE$;
    DefaultDataSkippingFilterPlanner planner =
        new DefaultDataSkippingFilterPlanner(
            spark,
            DeltaDataSkippingType.dataSkippingAndPartitionFilteringV1(),
            getStatsColumnOpt,
            partColsScala,
            partSchema,
            useStats,
            emptySeq, // clusteringColumns
            Option.empty(), // protocol
            Option.empty()); // numOfFilesIfKnown
    DataSkippingFilterPlanner.Result plan = planner.plan(filterExprs);

    // ---- 7. EXECUTE: choose path (mirrors filesForScan step 2) ----
    Dataset<org.apache.spark.sql.Row> filteredDF;
    if (plan.dataFilters().isEmpty()) {
      if (!plan.partitionFilters().isEmpty()) {
        filteredDF = executor.filterOnPartitionsAsDF(plan.partitionFilters());
      } else {
        filteredDF = executor.getAllFilesAsDF();
      }
    } else {
      Column partFilterCol = planner.constructPartitionFilters(plan.partitionFilters());
      filteredDF = executor.getDataSkippedFilesAsDF(partFilterCol, plan.verifiedSkippingExpr());
    }

    // ---- 8. Wrap in DistributedScan (zero-copy) ----
    return new DistributedScan(filteredDF, snapshot, readSchema);
  }

  @Override
  public PaginatedScan buildPaginated(long maxFilesPerBatch, Optional<Row> previousPageToken) {
    throw new UnsupportedOperationException(
        "Paginated scan is not yet supported with distributed data skipping");
  }

  // ========================
  // Private helpers
  // ========================

  /** Builds a partition schema from the full table schema and partition column names. */
  private StructType buildPartitionSchema(
      StructType tableSchema, List<String> partitionColumnNames) {
    if (partitionColumnNames.isEmpty()) {
      return new StructType();
    }

    Set<String> partColSet =
        partitionColumnNames.stream().map(String::toLowerCase).collect(Collectors.toSet());

    List<StructField> partFields = new ArrayList<>();
    for (StructField field : tableSchema.fields()) {
      if (partColSet.contains(field.name().toLowerCase())) {
        partFields.add(field);
      }
    }

    // Preserve partition column ordering
    List<StructField> orderedFields = new ArrayList<>();
    for (String partCol : partitionColumnNames) {
      for (StructField field : partFields) {
        if (field.name().equalsIgnoreCase(partCol)) {
          orderedFields.add(field);
          break;
        }
      }
    }

    return new StructType(orderedFields.toArray(new StructField[0]));
  }
}
