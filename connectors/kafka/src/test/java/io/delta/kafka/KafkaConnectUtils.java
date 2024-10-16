/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package io.delta.kafka;

import static org.apache.kafka.clients.CommonClientConfigs.GROUP_ID_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.KEY_CONVERTER_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.WorkerConfig.CONNECTOR_CLIENT_POLICY_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.WorkerConfig.PLUGIN_DISCOVERY_CONFIG;
import static org.apache.kafka.connect.runtime.distributed.DistributedConfig.CONFIG_STORAGE_REPLICATION_FACTOR_CONFIG;
import static org.apache.kafka.connect.runtime.distributed.DistributedConfig.CONFIG_TOPIC_CONFIG;
import static org.apache.kafka.connect.runtime.distributed.DistributedConfig.OFFSET_STORAGE_REPLICATION_FACTOR_CONFIG;
import static org.apache.kafka.connect.runtime.distributed.DistributedConfig.OFFSET_STORAGE_TOPIC_CONFIG;
import static org.apache.kafka.connect.runtime.distributed.DistributedConfig.STATUS_STORAGE_REPLICATION_FACTOR_CONFIG;
import static org.apache.kafka.connect.runtime.distributed.DistributedConfig.STATUS_STORAGE_TOPIC_CONFIG;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.hc.client5.http.classic.HttpClient;
import org.apache.hc.client5.http.classic.methods.HttpDelete;
import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.core5.http.HttpStatus;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.connect.runtime.isolation.PluginDiscoveryMode;
import org.apache.kafka.connect.util.clusters.EmbeddedConnectCluster;
import org.awaitility.Awaitility;

public class KafkaConnectUtils {

  private static final HttpClient HTTP = HttpClients.createDefault();
  private static final boolean USE_EMBEDDED_CONNECT = true;

  private static EmbeddedConnectCluster connectCluster;

  // JavaBean-style for serialization
  public static class Config {

    private final String name;
    private final Map<String, String> config = Maps.newHashMap();

    public Config(String name) {
      this.name = name;
    }

    public String getName() {
      return name;
    }

    public Map<String, String> getConfig() {
      return config;
    }

    public Config config(String key, Object value) {
      config.put(key, value.toString());
      return this;
    }
  }

  public static void startConnectCluster() {
    if (USE_EMBEDDED_CONNECT) {
      if (connectCluster != null) {
        // cluster is already running
        return;
      }
      String connectClusterName = "connect-cluster-" + UUID.randomUUID().toString();
      Map<String, String> workerProps = new HashMap<>();
      // permit all Kafka client overrides; required for testing different consumer partition
      // assignment strategies
      workerProps.put(CONNECTOR_CLIENT_POLICY_CLASS_CONFIG, "All");
      workerProps.put(PLUGIN_DISCOVERY_CONFIG, PluginDiscoveryMode.ONLY_SCAN.toString());
      workerProps.put("transaction.state.log.replication.factor", "1");
      workerProps.put("transaction.state.log.min.isr", "1");
      workerProps.putIfAbsent(GROUP_ID_CONFIG, "kc");
      workerProps.putIfAbsent(OFFSET_STORAGE_TOPIC_CONFIG, "kc-offsets");
      workerProps.putIfAbsent(OFFSET_STORAGE_REPLICATION_FACTOR_CONFIG, "1");
      workerProps.putIfAbsent(CONFIG_TOPIC_CONFIG, "kc-config");
      workerProps.putIfAbsent(CONFIG_STORAGE_REPLICATION_FACTOR_CONFIG, "1");
      workerProps.putIfAbsent(STATUS_STORAGE_TOPIC_CONFIG, "kc-storage");
      workerProps.putIfAbsent(STATUS_STORAGE_REPLICATION_FACTOR_CONFIG, "1");
      workerProps.putIfAbsent(
          KEY_CONVERTER_CLASS_CONFIG, "org.apache.kafka.connect.json.JsonConverter");
      workerProps.putIfAbsent(
          VALUE_CONVERTER_CLASS_CONFIG, "org.apache.kafka.connect.json.JsonConverter");
      workerProps.putIfAbsent("key.converter.schemas.enable", "false");
      //      workerProps.putIfAbsent("value.converter.schemas.enable", "false");
      // workerProps.put("max.block.ms", "200000000");
      workerProps.putIfAbsent("offset.flush.interval.ms", "500");

      // To create the control topic whenever the KafkaProducer is created, otherwise
      // the KafkaProducer to send events from the Coordinator to the Worker will block
      // saying partitions not found. Iceberg docs advise to set this to true in the
      // production environment or manually create the control topic.
      workerProps.putIfAbsent("auto.create.topics.enable", "true");

      // setup Kafka broker properties
      Properties brokerProps = new Properties();
      brokerProps.put("auto.create.topics.enable", "false");
      brokerProps.put("delete.topic.enable", "true");
      brokerProps.put(PLUGIN_DISCOVERY_CONFIG, PluginDiscoveryMode.ONLY_SCAN.toString());
      brokerProps.put("transaction.state.log.replication.factor", "1");
      brokerProps.put("transaction.state.log.min.isr", "1");
      brokerProps.putIfAbsent(GROUP_ID_CONFIG, "kc");
      brokerProps.putIfAbsent(OFFSET_STORAGE_TOPIC_CONFIG, "kc-offsets");
      brokerProps.putIfAbsent(OFFSET_STORAGE_REPLICATION_FACTOR_CONFIG, "1");
      brokerProps.putIfAbsent(CONFIG_TOPIC_CONFIG, "kc-config");
      brokerProps.putIfAbsent(CONFIG_STORAGE_REPLICATION_FACTOR_CONFIG, "1");
      brokerProps.putIfAbsent(STATUS_STORAGE_TOPIC_CONFIG, "kc-storage");
      brokerProps.putIfAbsent(STATUS_STORAGE_REPLICATION_FACTOR_CONFIG, "1");
      brokerProps.putIfAbsent(
          // KEY_CONVERTER_CLASS_CONFIG, "org.apache.kafka.connect.json.JsonConverter");
          KEY_CONVERTER_CLASS_CONFIG, "org.apache.kafka.connect.json.JsonConverter");
      brokerProps.putIfAbsent(
          VALUE_CONVERTER_CLASS_CONFIG, "org.apache.kafka.connect.json.JsonConverter");
      brokerProps.putIfAbsent("offset.flush.interval.ms", "500");
      //      brokerProps.putIfAbsent("key.converter.schemas.enable", "false");
      //      brokerProps.putIfAbsent("value.converter.schemas.enable", "false");

      // To create the control topic whenever the KafkaProducer is created, otherwise
      // the KafkaProducer to send events from the Coordinator to the Worker will block
      // saying partitions not found. Iceberg docs advise to set this to true in the
      // production environment or manually create the control topic.
      brokerProps.putIfAbsent("auto.create.topics.enable", "true");

      connectCluster =
          new EmbeddedConnectCluster.Builder()
              .name("connect-cluster")
              .numWorkers(2)
              .workerProps(workerProps)
              .brokerProps(brokerProps)
              .build();
      connectCluster.start();
    }
  }

  public static void stopConnectCluster() {
    if (USE_EMBEDDED_CONNECT && connectCluster != null) {
      connectCluster.stop();
      connectCluster = null;
    }
  }

  public static void createControlTopic(Admin admin) {
    try {
      NewTopic newTopic = new NewTopic("control-iceberg", 1, (short) 1);
      CreateTopicsResult result = admin.createTopics(Collections.singleton(newTopic));

      // Wait for the operation to complete
      KafkaFuture<Void> future = result.values().get("control-iceberg");
      future.get();

      System.out.println("Topic 'control-iceberg' created successfully.");
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static void startConnector(Config config) {
    try {
      if (USE_EMBEDDED_CONNECT) {
        connectCluster.configureConnector(config.getName(), config.getConfig());
      } else {

        HttpPost request =
            new HttpPost(String.format("http://localhost:%d/connectors", TestContext.CONNECT_PORT));
        String body = TestContext.MAPPER.writeValueAsString(config);
        request.setHeader("Content-Type", "application/json");
        request.setEntity(new StringEntity(body));
        HTTP.execute(
            request,
            response -> {
              if (response.getCode() != HttpStatus.SC_CREATED) {
                throw new RuntimeException("Failed to start connector: " + response.getCode());
              }
              return null;
            });
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static String getBootstrapServers() {
    if (USE_EMBEDDED_CONNECT) {
      return connectCluster.kafka().bootstrapServers();
    } else {
      return TestContext.BOOTSTRAP_SERVERS;
    }
  }

  public static void ensureConnectorRunning(String name, int numTasks) {
    try {
      if (USE_EMBEDDED_CONNECT) {
        connectCluster
            .assertions()
            .assertConnectorAndExactlyNumTasksAreRunning(
                name, numTasks, "Connector tasks did not start in time.");
      } else {
        HttpGet request =
            new HttpGet(
                String.format(
                    "http://localhost:%d/connectors/%s/status", TestContext.CONNECT_PORT, name));
        Awaitility.await()
            .atMost(120, TimeUnit.SECONDS)
            .until(
                () ->
                    HTTP.execute(
                        request,
                        response -> {
                          if (response.getCode() == HttpStatus.SC_OK) {
                            JsonNode root =
                                TestContext.MAPPER.readTree(response.getEntity().getContent());
                            String connectorState = root.get("connector").get("state").asText();
                            ArrayNode taskNodes = (ArrayNode) root.get("tasks");
                            List<String> taskStates = Lists.newArrayList();
                            taskNodes.forEach(node -> taskStates.add(node.get("state").asText()));
                            return "RUNNING".equals(connectorState)
                                && taskStates.stream().allMatch("RUNNING"::equals);
                          }
                          return false;
                        }));
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static void stopConnector(String name) {
    try {
      if (USE_EMBEDDED_CONNECT) {
        if (connectCluster != null) {
          connectCluster.stopConnector(name);
        }
      } else {
        HttpDelete request =
            new HttpDelete(
                String.format("http://localhost:%d/connectors/%s", TestContext.CONNECT_PORT, name));
        HTTP.execute(request, response -> null);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private KafkaConnectUtils() {}
}
