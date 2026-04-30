/*
 * Copyright (2026) The Delta Lake Project Authors.
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

package org.apache.spark.sql.delta.catalog.credentials;

import io.unitycatalog.client.ApiClient;
import io.unitycatalog.client.ApiClientBuilder;
import io.unitycatalog.client.auth.TokenProvider;
import io.unitycatalog.client.retry.RetryPolicy;

import java.net.URI;

public class ApiClientFactory {

  private ApiClientFactory() {}

  public static ApiClient createApiClient(
      RetryPolicy retryPolicy, URI uri, TokenProvider tokenProvider) {
    ApiClientBuilder builder =
        ApiClientBuilder.create().uri(uri).tokenProvider(tokenProvider).retryPolicy(retryPolicy);

    String sparkVersion = getSparkVersion();
    String deltaVersion = getDeltaVersion();
    String javaVersion = getJavaVersion();
    String scalaVersion = getScalaVersion();

    builder.addAppVersion("Spark", sparkVersion);
    if (deltaVersion != null) {
      builder.addAppVersion("Delta", deltaVersion);
    }
    if (javaVersion != null) {
      builder.addAppVersion("Java", javaVersion);
    }
    if (scalaVersion != null) {
      builder.addAppVersion("Scala", scalaVersion);
    }

    return builder.build();
  }

  private static String getSparkVersion() {
    try {
      return org.apache.spark.package$.MODULE$.SPARK_VERSION();
    } catch (Exception e) {
      return null;
    }
  }

  static String getDeltaVersion() {
    try {
      Class<?> versionClass = Class.forName("io.delta.Version");
      Object versionObj = versionClass.getMethod("getVersion").invoke(null);
      return versionObj != null ? versionObj.toString() : null;
    } catch (Exception e) {
      try {
        Class<?> packageClass = Class.forName("io.delta.package$");
        Object versionObj =
            packageClass.getMethod("VERSION").invoke(packageClass.getField("MODULE$").get(null));
        return versionObj != null ? versionObj.toString() : null;
      } catch (Exception e2) {
        return null;
      }
    }
  }

  private static String getJavaVersion() {
    try {
      return System.getProperty("java.version");
    } catch (Exception e) {
      return null;
    }
  }

  private static String getScalaVersion() {
    try {
      return scala.util.Properties.versionNumberString();
    } catch (Exception e) {
      return null;
    }
  }
}
