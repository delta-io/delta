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

import static org.apache.kafka.connect.runtime.WorkerConfig.CONNECTOR_CLIENT_POLICY_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.WorkerConfig.PLUGIN_DISCOVERY_CONFIG;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
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

  public static void startConnector(Config config) {
    try {
      if (USE_EMBEDDED_CONNECT) {
        Map<String, String> workerProps = new HashMap<>();
        // permit all Kafka client overrides; required for testing different consumer partition
        // assignment strategies
        workerProps.put(CONNECTOR_CLIENT_POLICY_CLASS_CONFIG, "All");
        workerProps.put(PLUGIN_DISCOVERY_CONFIG, PluginDiscoveryMode.ONLY_SCAN.toString());

        // setup Kafka broker properties
        Properties brokerProps = new Properties();
        brokerProps.put("auto.create.topics.enable", "false");
        brokerProps.put("delete.topic.enable", "true");
        brokerProps.put(PLUGIN_DISCOVERY_CONFIG, PluginDiscoveryMode.ONLY_SCAN.toString());

        connectCluster =
            new EmbeddedConnectCluster.Builder()
                .name("connect-cluster")
                .numWorkers(2)
                .workerProps(workerProps)
                .brokerProps(brokerProps)
                .build();
        connectCluster.start();

        connectCluster.configureConnector(config.getName(), config.getConfig());
        connectCluster
            .assertions()
            .assertConnectorAndAtLeastNumTasksAreRunning(
                config.name,
                Integer.parseInt(config.getConfig().get("tasks.max")),
                "Connector tasks did not start in time.");
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
    } catch (IOException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  public static void ensureConnectorRunning(String name) {
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
