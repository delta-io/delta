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
package io.delta.spark.internal.v2.write;

import static io.delta.spark.internal.v2.utils.ScalaUtils.toScalaMap;
import static java.util.Objects.requireNonNull;

import io.delta.kernel.Operation;
import io.delta.kernel.Snapshot;
import io.delta.kernel.Transaction;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.spark.internal.v2.read.DeltaParquetFileFormatV2;
import io.delta.spark.internal.v2.utils.PartitionUtils;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.types.DataTypeUtils;
import org.apache.spark.sql.connector.distributions.Distribution;
import org.apache.spark.sql.connector.distributions.Distributions;
import org.apache.spark.sql.connector.expressions.Expressions;
import org.apache.spark.sql.connector.expressions.SortDirection;
import org.apache.spark.sql.connector.expressions.SortOrder;
import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.RequiresDistributionAndOrdering;
import org.apache.spark.sql.connector.write.Write;
import org.apache.spark.sql.delta.files.DelayedCommitProtocol;
import org.apache.spark.sql.delta.files.DeltaFileFormatWriter;
import org.apache.spark.sql.delta.stats.DeltaJobStatisticsTracker;
import org.apache.spark.sql.execution.datasources.OutputWriterFactory;
import org.apache.spark.sql.execution.datasources.WriteJobDescription;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.util.SerializableConfiguration;
import scala.Option;
import scala.collection.immutable.Seq;

/**
 * DSv2 {@link Write} implementation for Delta tables.
 *
 * <p>Mirrors Spark's {@code FileWrite.toBatch()} pattern: heavy initialization (Kernel Transaction,
 * DelayedCommitProtocol, WriteJobDescription) is deferred to {@link #toBatch()} which is called at
 * the start of physical execution.
 *
 * <p>Implements {@link RequiresDistributionAndOrdering} so that Spark automatically inserts a
 * shuffle + sort by partition columns before the write, ensuring each task receives all rows for a
 * given partition in sorted order.
 *
 * <p>The executor-side write path reuses Spark's standard classes entirely:
 *
 * <ul>
 *   <li>{@link DeltaFileWriterFactory} serializes the description + committer to executors
 *   <li>{@code SingleDirectoryDataWriter} or {@code DynamicPartitionDataSingleWriter} writes rows
 *   <li>{@link DelayedCommitProtocol} tracks files and produces V1 {@code AddFile} actions
 * </ul>
 */
public class DeltaWrite implements Write, RequiresDistributionAndOrdering {

  private final Snapshot snapshot;
  private final String tablePath;
  private final Configuration hadoopConf;
  private final StructType tableSchema;
  private final StructType dataSchema;
  private final StructType partitionSchema;
  private final LogicalWriteInfo info;
  private final List<String> partitionColumnNames;
  private final WriteMode writeMode;
  private final io.delta.kernel.expressions.Predicate replaceWherePredicate;

  DeltaWrite(
      Snapshot snapshot,
      String tablePath,
      Configuration hadoopConf,
      StructType tableSchema,
      StructType dataSchema,
      StructType partitionSchema,
      LogicalWriteInfo info,
      WriteMode writeMode,
      io.delta.kernel.expressions.Predicate replaceWherePredicate) {
    this.snapshot = requireNonNull(snapshot, "snapshot is null");
    this.tablePath = requireNonNull(tablePath, "tablePath is null");
    this.hadoopConf = requireNonNull(hadoopConf, "hadoopConf is null");
    this.tableSchema = requireNonNull(tableSchema, "tableSchema is null");
    this.dataSchema = requireNonNull(dataSchema, "dataSchema is null");
    this.partitionSchema = requireNonNull(partitionSchema, "partitionSchema is null");
    this.info = requireNonNull(info, "info is null");
    this.partitionColumnNames = snapshot.getPartitionColumnNames();
    this.writeMode = requireNonNull(writeMode, "writeMode is null");
    this.replaceWherePredicate = replaceWherePredicate;
  }

  @Override
  public String description() {
    return "Delta";
  }

  @Override
  public Distribution requiredDistribution() {
    if (partitionColumnNames.isEmpty()) {
      return Distributions.unspecified();
    }
    return Distributions.clustered(
        partitionColumnNames.stream()
            .map(Expressions::column)
            .toArray(org.apache.spark.sql.connector.expressions.Expression[]::new));
  }

  @Override
  public boolean distributionStrictlyRequired() {
    return true;
  }

  @Override
  public SortOrder[] requiredOrdering() {
    return partitionColumnNames.stream()
        .map(name -> Expressions.sort(Expressions.column(name), SortDirection.ASCENDING))
        .toArray(SortOrder[]::new);
  }

  @Override
  @SuppressWarnings({"unchecked", "rawtypes"})
  public BatchWrite toBatch() {
    SparkSession spark = SparkSession.active();

    // -- Hadoop conf: merge table metadata configuration --
    // V1 does: newHadoopConfWithOptions(metadata.configuration ++ deltaLog.options)
    // This ensures Delta table properties (e.g. column mapping config) are visible to
    // Parquet writers and the commit protocol.
    Configuration mergedHadoopConf = new Configuration(hadoopConf);
    io.delta.kernel.internal.SnapshotImpl snapshotImpl =
        (io.delta.kernel.internal.SnapshotImpl) snapshot;
    for (java.util.Map.Entry<String, String> e :
        snapshotImpl.getMetadata().getConfiguration().entrySet()) {
      mergedHadoopConf.set(e.getKey(), e.getValue());
    }

    Engine engine = DefaultEngine.create(mergedHadoopConf);

    // -- Schema evolution --
    boolean mergeSchema =
        Boolean.parseBoolean(info.options().getOrDefault("mergeSchema", "false"))
            || (Boolean)
                spark
                    .sessionState()
                    .conf()
                    .getConf(
                        org.apache.spark.sql.delta.sources.DeltaSQLConf
                            .DELTA_SCHEMA_AUTO_MIGRATE());
    boolean overwriteSchema =
        Boolean.parseBoolean(info.options().getOrDefault("overwriteSchema", "false"))
            && (writeMode == WriteMode.TRUNCATE || writeMode == WriteMode.REPLACE_WHERE);

    StructType incomingSchema = info.schema();

    io.delta.kernel.transaction.UpdateTableTransactionBuilder txnBuilder =
        snapshot.buildUpdateTableTransaction("Delta-Spark-V2", Operation.WRITE);

    if (overwriteSchema) {
      io.delta.kernel.types.StructType newKernelSchema =
          io.delta.spark.internal.v2.utils.SchemaUtils.convertSparkSchemaToKernelSchema(
              incomingSchema);
      txnBuilder.withUpdatedSchema(newKernelSchema);
    } else if (mergeSchema && !incomingSchema.equals(tableSchema)) {
      StructType mergedSparkSchema =
          SchemaEvolutionHelper.mergeSchemas(tableSchema, incomingSchema);
      io.delta.kernel.types.StructType mergedKernelSchema =
          io.delta.spark.internal.v2.utils.SchemaUtils.convertSparkSchemaToKernelSchema(
              mergedSparkSchema);
      txnBuilder.withUpdatedSchema(mergedKernelSchema);
    }

    Transaction txn = txnBuilder.build(engine);

    // -- Schema: split into data + partition columns --

    // -- DelayedCommitProtocol (reuse V1) --
    // V1 TransactionalWrite.getCommitter: uses random prefix if randomizeFilePrefixes is
    // enabled or column mapping is active; uses data subdir if configured.
    String jobId = UUID.randomUUID().toString();
    java.util.Map<String, String> tableConfig = snapshotImpl.getMetadata().getConfiguration();
    boolean randomizeFilePrefixes =
        Boolean.parseBoolean(tableConfig.getOrDefault("delta.randomizeFilePrefixes", "false"));
    String columnMappingMode = tableConfig.getOrDefault("delta.columnMapping.mode", "none");
    boolean needsRandomPrefix = randomizeFilePrefixes || !"none".equals(columnMappingMode);

    Option<Object> randomPrefixLengthOpt;
    if (needsRandomPrefix) {
      int prefixLen = Integer.parseInt(tableConfig.getOrDefault("delta.randomPrefixLength", "2"));
      randomPrefixLengthOpt = Option.apply((Object) prefixLen);
    } else {
      randomPrefixLengthOpt = Option.empty();
    }

    Option<String> deltaDataSubdir =
        (Boolean)
                spark
                    .sessionState()
                    .conf()
                    .getConf(
                        org.apache.spark.sql.delta.sources.DeltaSQLConf
                            .WRITE_DATA_FILES_TO_SUBDIR())
            ? Option.apply("data")
            : Option.empty();

    DelayedCommitProtocol committer =
        new DelayedCommitProtocol(jobId, tablePath, randomPrefixLengthOpt, deltaDataSubdir);

    // -- Hadoop Job + OutputWriterFactory (reuse V1 DeltaFileFormatWriterBase) --
    Job job = DeltaFileFormatWriter.createHadoopJob(mergedHadoopConf, tablePath);

    // DeltaParquetFileFormatV2 extends DeltaParquetFileFormatBase → ParquetFileFormat.
    // Adds Delta-specific Parquet config: IcebergCompat timestamp types, DeltaParquetWriteSupport
    // for column mapping, etc.
    DeltaParquetFileFormatV2 fileFormat =
        PartitionUtils.createDeltaParquetFileFormat(
            snapshot, tablePath, /* optimizationsEnabled */ false, Option.empty());

    // -- WRITE_PARTITION_COLUMNS: Iceberg spec and MaterializePartitionColumns require
    // partition columns to be materialized in Parquet data files. --
    // V1: IcebergCompat.isAnyEnabled(metadata) || protocol.isFeatureSupported(...)
    boolean icebergCompatEnabled =
        Boolean.parseBoolean(tableConfig.getOrDefault("delta.enableIcebergCompatV1", "false"))
            || Boolean.parseBoolean(
                tableConfig.getOrDefault("delta.enableIcebergCompatV2", "false"));
    boolean writePartitionColumns =
        icebergCompatEnabled
            || snapshotImpl
                .getProtocol()
                .getWriterFeatures()
                .contains("materializePartitionColumns");

    Map<String, String> writerOptions = new HashMap<>();
    writerOptions.put(
        org.apache.spark.sql.delta.DeltaOptions.WRITE_PARTITION_COLUMNS(),
        Boolean.toString(writePartitionColumns));

    // -- WriteJobDescription: allColumns = dataColumns ++ partitionColumns --
    // All three seqs must be derived from the same toAttributes call so their Attribute
    // expression IDs match (WriteJobDescription asserts AttributeSet equality).
    Seq allColumns = (Seq) DataTypeUtils.toAttributes(info.schema());

    // Split allColumns by partition membership
    Set<String> partColNameSet = new HashSet<>(partitionColumnNames);
    List<Object> dataCols = new ArrayList<>();
    List<Object> partCols = new ArrayList<>();
    scala.collection.Iterator<Attribute> it =
        (scala.collection.Iterator<Attribute>) allColumns.iterator();
    while (it.hasNext()) {
      Attribute attr = it.next();
      if (partColNameSet.contains(attr.name())) {
        partCols.add(attr);
      } else {
        dataCols.add(attr);
      }
    }

    // When writePartitionColumns is true, partition columns are included in the Parquet
    // data schema so they appear in the physical file (V1: outputDataColumns = dataColumns ++
    // partitionColumns). Otherwise only non-partition columns are written.
    Seq partitionColumns = scala.jdk.javaapi.CollectionConverters.asScala(partCols).toList();
    Seq dataColumns;
    if (writePartitionColumns) {
      List<Object> outputDataCols = new ArrayList<>(dataCols);
      outputDataCols.addAll(partCols);
      dataColumns = scala.jdk.javaapi.CollectionConverters.asScala(outputDataCols).toList();
    } else {
      dataColumns = scala.jdk.javaapi.CollectionConverters.asScala(dataCols).toList();
    }

    // prepareWrite needs the output data schema as StructType.
    // V1: outputDataColumns.toStructType (Scala implicit).
    // In Java we reconstruct it from the Attribute sequence.
    org.apache.spark.sql.types.StructField[] outputFields =
        new org.apache.spark.sql.types.StructField[dataColumns.size()];
    for (int i = 0; i < dataColumns.size(); i++) {
      Attribute attr = (Attribute) dataColumns.apply(i);
      outputFields[i] =
          new org.apache.spark.sql.types.StructField(
              attr.name(), attr.dataType(), attr.nullable(), attr.metadata());
    }
    StructType outputDataSchema = new StructType(outputFields);
    OutputWriterFactory factory =
        fileFormat.prepareWrite(spark, job, toScalaMap(writerOptions), outputDataSchema);

    SerializableConfiguration srlHadoopConf = new SerializableConfiguration(job.getConfiguration());

    // BasicWriteJobStatsTracker: file-level metrics (numFiles, numRows, bytes) for Spark UI
    org.apache.spark.sql.execution.datasources.BasicWriteJobStatsTracker basicStatsTracker =
        new org.apache.spark.sql.execution.datasources.BasicWriteJobStatsTracker(
            srlHadoopConf,
            org.apache.spark.sql.execution.datasources.BasicWriteJobStatsTracker$.MODULE$
                .metrics());

    // DeltaJobStatisticsTracker: per-file column-level stats (min/max/nullCount) for data skipping
    scala.Option<DeltaJobStatisticsTracker> deltaStatsTrackerOpt =
        DeltaStatsTrackerHelper.createStatsTracker(
            spark, snapshot, mergedHadoopConf, new Path(tablePath), dataSchema);

    List<org.apache.spark.sql.execution.datasources.WriteJobStatsTracker> trackerList =
        new ArrayList<>();
    if (deltaStatsTrackerOpt.isDefined()) {
      trackerList.add(deltaStatsTrackerOpt.get());
    }
    trackerList.add(basicStatsTracker);
    Seq statsTrackers = scala.jdk.javaapi.CollectionConverters.asScala(trackerList).toList();

    scala.collection.immutable.Map emptyPartLocations =
        scala.collection.immutable.Map$.MODULE$.empty();

    String writeJobUUID = UUID.randomUUID().toString();
    WriteJobDescription desc =
        new WriteJobDescription(
            writeJobUUID,
            srlHadoopConf,
            factory,
            allColumns,
            dataColumns,
            partitionColumns,
            Option.empty(), // bucketSpec
            tablePath,
            emptyPartLocations,
            spark.sessionState().conf().maxRecordsPerFile(),
            spark.sessionState().conf().sessionLocalTimeZone(),
            statsTrackers);

    // V1 DeltaFileFormatWriter sets this after constructing the description
    job.getConfiguration().set("spark.sql.sources.writeJobUUID", writeJobUUID);

    // -- Setup job (no-op for DelayedCommitProtocol) --
    committer.setupJob(job);

    // Kernel data schema for deserializing JSON stats in DeltaBatchWrite
    io.delta.kernel.types.StructType kernelDataSchema =
        io.delta.spark.internal.v2.utils.SchemaUtils.convertSparkSchemaToKernelSchema(dataSchema);

    return new DeltaBatchWrite(
        job,
        desc,
        committer,
        txn,
        engine,
        partitionSchema,
        deltaStatsTrackerOpt,
        kernelDataSchema,
        writeMode,
        replaceWherePredicate,
        snapshot);
  }
}
