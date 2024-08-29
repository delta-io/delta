/*
 * Copyright (2024) The Delta Lake Project Authors.
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
package io.delta.kernel.defaults.engine;

import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.internal.coordinatedcommits.CommitCoordinatorProvider;
import io.delta.kernel.defaults.internal.coordinatedcommits.StorageKernelAPIAdapter;
import io.delta.kernel.defaults.internal.json.JsonUtils;
import io.delta.kernel.defaults.internal.logstore.LogStoreProvider;
import io.delta.kernel.engine.CommitCoordinatorClientHandler;
import io.delta.kernel.engine.coordinatedcommits.CommitResponse;
import io.delta.kernel.engine.coordinatedcommits.GetCommitsResponse;
import io.delta.kernel.engine.coordinatedcommits.UpdatedActions;
import io.delta.kernel.engine.coordinatedcommits.actions.AbstractMetadata;
import io.delta.kernel.engine.coordinatedcommits.actions.AbstractProtocol;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.storage.LogStore;
import io.delta.storage.commit.CommitCoordinatorClient;
import io.delta.storage.commit.CommitFailedException;
import io.delta.storage.commit.TableDescriptor;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

/**
 * Default implementation of {@link CommitCoordinatorClientHandler} based on Hadoop APIs which uses
 * commit coordinator defined in delta-storage modules. It takes a Hadoop {@link Configuration}
 * object to interact with the commit coordinator client. The following optional configurations can
 * be set to customize the behavior of the client:
 *
 * <ul>
 *   <li>{@code io.delta.kernel.logStore.<scheme>.impl} - The class name of the custom {@link
 *       LogStore} implementation to use for operations on storage systems with the specified {@code
 *       scheme}. For example, to use a custom {@link LogStore} for S3 storage objects:
 *       <pre>{@code
 * <property>
 *   <name>io.delta.kernel.logStore.s3.impl</name>
 *   <value>com.example.S3LogStore</value>
 * </property>
 *
 * }</pre>
 *       If not set, the default LogStore implementation for the scheme will be used.
 *   <li>{@code delta.enableFastS3AListFrom} - Set to {@code true} to enable fast listing
 *       functionality when using a {@link LogStore} created for S3 storage objects.
 *   <li>{@code io.delta.kernel.commitCoordinatorBuilder.<name>.impl} - The class name of the custom
 *       {@link io.delta.kernel.defaults.internal.coordinatedcommits.CommitCoordinatorBuilder}
 *       implementation to use for building the commit coordinator client.
 * </ul>
 */
public class DefaultCommitCoordinatorClientHandler implements CommitCoordinatorClientHandler {
  private final Configuration hadoopConf;
  private final CommitCoordinatorClient commitCoordinatorClient;

  /**
   * Create an instance of the default {@link DefaultCommitCoordinatorClientHandler} implementation.
   *
   * @param hadoopConf Configuration to use. List of options to customize the behavior of the client
   *     can be found in the class documentation.
   * @param name The identifier or name of the underlying commit coordinator client
   * @param commitCoordinatorConf The configuration settings for the underlying commit coordinator
   *     client which contains the necessary information to create the client such as the endpoint
   *     etc.
   */
  public DefaultCommitCoordinatorClientHandler(
      Configuration hadoopConf, String name, Map<String, String> commitCoordinatorConf) {
    this.hadoopConf = hadoopConf;
    this.commitCoordinatorClient =
        CommitCoordinatorProvider.getCommitCoordinatorClient(
            hadoopConf, name, commitCoordinatorConf);
  }

  @Override
  public Map<String, String> registerTable(
      String logPath,
      long currentVersion,
      AbstractMetadata currentMetadata,
      AbstractProtocol currentProtocol) {
    // TODO: Introduce table identifier concept in Table API in Kernel and plumb the
    //  table identifier into `CommitCoordinatorClient` in all APIs.
    return commitCoordinatorClient.registerTable(
        new Path(logPath),
        Optional.empty(),
        currentVersion,
        StorageKernelAPIAdapter.toStorageAbstractMetadata(currentMetadata),
        StorageKernelAPIAdapter.toStorageAbstractProtocol(currentProtocol));
  }

  @Override
  public CommitResponse commit(
      String logPath,
      Map<String, String> tableConf,
      long commitVersion,
      CloseableIterator<Row> actions,
      UpdatedActions updatedActions)
      throws io.delta.kernel.engine.coordinatedcommits.CommitFailedException {
    Path path = new Path(logPath);
    LogStore logStore = LogStoreProvider.getLogStore(hadoopConf, path.toUri().getScheme());
    try {
      return StorageKernelAPIAdapter.toKernelAPICommitResponse(
          commitCoordinatorClient.commit(
              logStore,
              hadoopConf,
              new TableDescriptor(path, Optional.empty(), tableConf),
              commitVersion,
              new Iterator<String>() {
                @Override
                public boolean hasNext() {
                  return actions.hasNext();
                }

                @Override
                public String next() {
                  return JsonUtils.rowToJson(actions.next());
                }
              },
              StorageKernelAPIAdapter.toStorageUpdatedActions(updatedActions)));
    } catch (CommitFailedException e) {
      throw StorageKernelAPIAdapter.toKernelAPICommitFailedException(e);
    }
  }

  @Override
  public GetCommitsResponse getCommits(
      String logPath, Map<String, String> tableConf, Long startVersion, Long endVersion) {
    TableDescriptor tableDesc = new TableDescriptor(new Path(logPath), Optional.empty(), tableConf);
    return StorageKernelAPIAdapter.toKernelAPIGetCommitsResponse(
        commitCoordinatorClient.getCommits(tableDesc, startVersion, endVersion));
  }

  @Override
  public void backfillToVersion(
      String logPath, Map<String, String> tableConf, long version, Long lastKnownBackfilledVersion)
      throws IOException {
    Path path = new Path(logPath);
    LogStore logStore = LogStoreProvider.getLogStore(hadoopConf, path.toUri().getScheme());
    TableDescriptor tableDesc = new TableDescriptor(path, Optional.empty(), tableConf);
    commitCoordinatorClient.backfillToVersion(
        logStore, hadoopConf, tableDesc, version, lastKnownBackfilledVersion);
  }

  @Override
  public boolean semanticEquals(CommitCoordinatorClientHandler other) {
    return other instanceof DefaultCommitCoordinatorClientHandler
        && commitCoordinatorClient.semanticEquals(
            ((DefaultCommitCoordinatorClientHandler) other).commitCoordinatorClient);
  }
}
