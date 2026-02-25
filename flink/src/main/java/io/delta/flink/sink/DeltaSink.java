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

package io.delta.flink.sink;

import io.delta.flink.table.*;
import io.delta.kernel.internal.util.Preconditions;
import io.delta.kernel.types.StructType;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import jdk.jfr.Experimental;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.sink2.*;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.connector.sink2.*;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Flink v2 sink offer different hooks to insert custom topologies into the sink. We will use the
 * following:
 *
 * <ul>
 *   <li>{@link SupportsPreWriteTopology} which redistributes the data to the writers
 *   <li>{@link org.apache.flink.api.connector.sink2.SinkWriter} which writes data/delete files, and
 *       generates the {@link DeltaWriterResult} objects for the files
 *   <li>{@link SupportsPreCommitTopology} which we use to place the {@link
 *       DeltaWriterResultAggregator} which merges the individual {@link
 *       org.apache.flink.api.connector.sink2.SinkWriter}'s {@link DeltaWriterResult}s to a single
 *       {@link DeltaCommittable}
 *   <li>{@link DeltaCommitter} which commits the incoming{@link DeltaCommittable}s to the Iceberg
 *       table
 *   <li>{@link SupportsPostCommitTopology} we could use for incremental compaction later. This is
 *       not implemented yet.
 * </ul>
 *
 * <p>The job graph looks like below:
 *
 * <pre>{@code
 *                            Flink sink
 *               +-----------------------------------------------------------------------------------+
 *               |                                                                                   |
 * +-------+     | +----------+                               +-------------+      +---------------+ |
 * | Map 1 | ==> | | writer 1 |                               | committer 1 | ---> | post commit 1 | |
 * +-------+     | +----------+                               +-------------+      +---------------+ |
 *               |             \                             /                \                      |
 *               |        DeltaWriterResults        DeltaCommittables          \                     |
 *               |               \                         /                    \                    |
 * +-------+     | +----------+   \ +-------------------+ /                      \ +---------------+ |
 * | Map 2 | ==> | | writer 2 | --->| commit aggregator |                          | post commit 2 | |
 * +-------+     | +----------+     +-------------------+                          +---------------+ |
 *               |                                             Commit only on                        |
 *               |                                             a single committer                    |
 *               +-----------------------------------------------------------------------------------+
 * }</pre>
 */
@Experimental
public class DeltaSink
    implements Sink<RowData>,
        SupportsCommitter<DeltaCommittable>,
        SupportsPreCommitTopology<DeltaWriterResult, DeltaCommittable>,
        SupportsPreWriteTopology<RowData>,
        SupportsPostCommitTopology<DeltaCommittable> {

  private static final Logger LOG = LoggerFactory.getLogger(DeltaSink.class);

  private final DeltaTable deltaTable;
  private final DeltaSinkConf conf;

  public DeltaSink(DeltaTable deltaTable, DeltaSinkConf conf) {
    this.deltaTable = deltaTable;
    this.conf = conf;
  }

  public DeltaTable getTable() {
    return deltaTable;
  }

  public DeltaSinkConf getConf() {
    return conf;
  }

  // Deprecated API for backward compatibility. Will not be called
  public SinkWriter<RowData> createWriter(InitContext context) {
    return new DeltaSinkWriter.Builder()
        .withJobId(context.getJobInfo().getJobId().toString())
        .withSubtaskId(context.getTaskInfo().getIndexOfThisSubtask())
        .withAttemptNumber(context.getTaskInfo().getAttemptNumber())
        .withDeltaTable(deltaTable)
        .withConf(conf)
        .build();
  }

  @Override
  public SinkWriter<RowData> createWriter(WriterInitContext context) {
    this.deltaTable.open();
    return new DeltaSinkWriter.Builder()
        .withJobId(context.getJobInfo().getJobId().toString())
        .withSubtaskId(context.getTaskInfo().getIndexOfThisSubtask())
        .withAttemptNumber(context.getTaskInfo().getAttemptNumber())
        .withDeltaTable(deltaTable)
        .withConf(conf)
        .withMetricGroup(context.metricGroup())
        .build();
  }

  @Override
  public Committer<DeltaCommittable> createCommitter(CommitterInitContext context) {
    this.deltaTable.open();
    return new DeltaCommitter.Builder()
        .withJobId(context.getJobInfo().getJobId().toString())
        .withTaskIndex(context.getTaskInfo().getIndexOfThisSubtask())
        .withDeltaTable(deltaTable)
        .withConf(conf)
        .withMetricGroup(context.metricGroup())
        .build();
  }

  /**
   * This method ensures that all rows with the same partitionHash will be sent to the same {@link
   * DeltaSinkWriter}. It makes no promises about how many unique partitionHash's that a {@link
   * DeltaSinkWriter} will handle (it may even be 0).
   *
   * <p>TODO This design may cause imbalanced workload if the data distribution is skewed.
   */
  @Override
  public DataStream<RowData> addPreWriteTopology(DataStream<RowData> inputDataStream) {
    if (deltaTable.getPartitionColumns().isEmpty()) {
      return inputDataStream;
    }
    return inputDataStream.keyBy(
        new PartitionKeySelector(conf.getSinkFlinkSchema(), deltaTable.getPartitionColumns()));
  }

  @Override
  public DataStream<CommittableMessage<DeltaCommittable>> addPreCommitTopology(
      DataStream<CommittableMessage<DeltaWriterResult>> writerResults) {
    TypeInformation<CommittableMessage<DeltaCommittable>> typeInformation =
        CommittableMessageTypeInfo.of(this::getCommittableSerializer);
    String uid = "PreCommit Agg";
    // global forces all output records send to subtask 0 of the downstream committer operator.
    // This is to ensure commit only happen in one committer subtask.
    return writerResults
        .global()
        .transform(uid, typeInformation, new DeltaWriterResultAggregator())
        .uid(uid)
        .setParallelism(1)
        .setMaxParallelism(1)
        // global forces all output records send to subtask 0 of the downstream committer operator.
        // This is to ensure commit only happen in one committer subtask.
        // Once upstream Flink provides the capability of setting committer operator
        // parallelism to 1, this can be removed.
        .global();
  }

  @Override
  public SimpleVersionedSerializer<DeltaWriterResult> getWriteResultSerializer() {
    return new DeltaWriterResult.Serializer();
  }

  @Override
  public SimpleVersionedSerializer<DeltaCommittable> getCommittableSerializer() {
    return new DeltaCommittable.Serializer();
  }

  @Override
  public void addPostCommitTopology(
      DataStream<CommittableMessage<DeltaCommittable>> committables) {}

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private enum TableType {
      hadoop,
      unitycatalog,
      ucpath
    }

    private static final ConfigOption<TableType> TYPE =
        ConfigOptions.key("type").enumType(TableType.class).defaultValue(TableType.hadoop);
    private static final ConfigOption<String> HADOOP_TABLE_PATH =
        ConfigOptions.key("hadoop.table_path").stringType().noDefaultValue();
    private static final ConfigOption<String> UNITYCATALOG_NAME =
        ConfigOptions.key("unitycatalog.name").stringType().noDefaultValue();
    private static final ConfigOption<String> UNITYCATALOG_TABLE_NAME =
        ConfigOptions.key("unitycatalog.table_name").stringType().noDefaultValue();
    private static final ConfigOption<String> UNITYCATALOG_ENDPOINT =
        ConfigOptions.key("unitycatalog.endpoint").stringType().noDefaultValue();
    private static final ConfigOption<String> UNITYCATALOG_TOKEN =
        ConfigOptions.key("unitycatalog.token").stringType().noDefaultValue();
    private static final ConfigOption<String> UNITYCATALOG_OAUTH_URI =
        ConfigOptions.key("unitycatalog.oauth.uri").stringType().noDefaultValue();
    private static final ConfigOption<String> UNITYCATALOG_OAUTH_CLIENT_ID =
        ConfigOptions.key("unitycatalog.oauth.client_id").stringType().noDefaultValue();
    private static final ConfigOption<String> UNITYCATALOG_OAUTH_CLIENT_SECRET =
        ConfigOptions.key("unitycatalog.oauth.client_secret").stringType().noDefaultValue();

    private DeltaTable deltaTable;
    private TableType tableType = TableType.hadoop;
    // For file-based tables
    private String tablePath;
    private RowType flinkSchema;
    private List<String> partitionColNames;
    // For catalog-based tables
    private String catalogName = "main";
    private String tableName;
    // UC configuration
    private URI endpoint;
    private String token;
    private URI oauthUri;
    private String oauthClientId;
    private String oauthClientSecret;

    private Map<String, String> configurations;

    private Builder() {}

    public Builder withDeltaTable(DeltaTable deltaTable) {
      this.deltaTable = deltaTable;
      return this;
    }

    public Builder withTablePath(String tablePath) {
      this.tablePath = tablePath;
      this.tableType = TableType.hadoop;
      return this;
    }

    public Builder withFlinkSchema(RowType flinkSchema) {
      this.flinkSchema = flinkSchema;
      return this;
    }

    public Builder withPartitionColNames(List<String> partitionColNames) {
      this.partitionColNames = partitionColNames;
      return this;
    }

    // For catalog-based tables
    public Builder withTableName(String tableName) {
      this.tableName = tableName;
      this.tableType = TableType.unitycatalog;
      return this;
    }

    public Builder withEndpoint(String catalogEndpoint) {
      this.endpoint = URI.create(catalogEndpoint);
      return this;
    }

    public Builder withToken(String catalogToken) {
      this.token = catalogToken;
      return this;
    }

    public Builder withOauthUri(String oauthUri) {
      this.oauthUri = URI.create(oauthUri);
      return this;
    }

    public Builder withOauthClientId(String oauthClientId) {
      this.oauthClientId = oauthClientId;
      return this;
    }

    public Builder withOauthClientSecret(String oauthClientSecret) {
      this.oauthClientSecret = oauthClientSecret;
      return this;
    }

    public Builder withConfigurations(Map<String, String> configurations) {
      this.configurations = configurations;
      Configuration extract = Configuration.fromMap(configurations);

      // Extract everything from configurations
      tableType = extract.get(TYPE, TableType.hadoop);
      tablePath = extract.get(HADOOP_TABLE_PATH, tablePath);
      catalogName = extract.get(UNITYCATALOG_NAME, "main");
      tableName = extract.get(UNITYCATALOG_TABLE_NAME, tableName);

      String endpoint = extract.get(UNITYCATALOG_ENDPOINT, null);
      if (Objects.nonNull(endpoint)) {
        this.endpoint = URI.create(endpoint);
      }
      token = extract.get(UNITYCATALOG_TOKEN, token);
      String oauthUriStr = extract.get(UNITYCATALOG_OAUTH_URI, null);
      if (Objects.nonNull(oauthUriStr)) {
        oauthUri = URI.create(oauthUriStr);
      }
      this.oauthClientId = extract.get(UNITYCATALOG_OAUTH_CLIENT_ID, null);
      this.oauthClientSecret = extract.get(UNITYCATALOG_OAUTH_CLIENT_SECRET, null);

      return this;
    }

    private DeltaCatalog createUnityCatalog() {
      Preconditions.checkArgument(endpoint != null);
      Preconditions.checkArgument(!(token == null && oauthUri == null));
      if (token != null) {
        return new UnityCatalog(catalogName, endpoint, token);
      } else {
        return new UnityCatalog(catalogName, endpoint, oauthUri, oauthClientId, oauthClientSecret);
      }
    }

    public DeltaSink build() {
      Objects.requireNonNull(flinkSchema, "flinkSchema must not be null");
      if (configurations == null) {
        configurations = Map.of();
      }
      DeltaSinkConf sinkConf = new DeltaSinkConf(flinkSchema, configurations);
      StructType sinkSchema = sinkConf.getSinkSchema();
      if (deltaTable == null) {
        switch (tableType) {
          case hadoop:
            {
              Objects.requireNonNull(tablePath);
              deltaTable =
                  new HadoopTable(
                      URI.create(tablePath), configurations, sinkSchema, partitionColNames);
              break;
            }
          case unitycatalog:
            {
              Objects.requireNonNull(endpoint);
              Objects.requireNonNull(token);
              // TODO Support separated endpoints for catalog and table
              DeltaCatalog restCatalog = createUnityCatalog();
              deltaTable =
                  new CatalogManagedTable(
                      restCatalog,
                      tableName,
                      configurations,
                      sinkSchema,
                      partitionColNames,
                      endpoint,
                      token);
              break;
            }
          case ucpath:
            {
              Objects.requireNonNull(endpoint);
              Objects.requireNonNull(token);
              DeltaCatalog restCatalog = createUnityCatalog();
              deltaTable =
                  new HadoopTable(
                      restCatalog, tableName, configurations, sinkSchema, partitionColNames);
              break;
            }
          default:
            throw new IllegalArgumentException("unreachable");
        }
      }
      deltaTable.open();
      return new DeltaSink(deltaTable, sinkConf);
    }
  }
}
