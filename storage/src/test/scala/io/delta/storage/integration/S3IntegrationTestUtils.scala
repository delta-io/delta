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

package io.delta.storage.integration

import org.apache.hadoop.conf.Configuration

private[integration] object S3IntegrationTestUtils {
  private val MultipartSize = 5L * 1024 * 1024

  def configuration(endpointOverride: Option[String] = None): Configuration = {
    val configuration = new Configuration()
    // Keep the multipart boundary small enough for integration tests to exercise conditional
    // CompleteMultipartUpload without uploading Hadoop's default 64 MiB part.
    configuration.setLong("fs.s3a.multipart.size", MultipartSize)
    endpointOverride.orElse(Option(System.getenv("S3_LOG_STORE_TEST_ENDPOINT"))).foreach { endpoint =>
      val accessKey = requiredEnvironmentVariable("S3_LOG_STORE_TEST_ACCESS_KEY")
      val secretKey = requiredEnvironmentVariable("S3_LOG_STORE_TEST_SECRET_KEY")
      configuration.set("fs.s3a.endpoint", endpoint)
      configuration.set("fs.s3a.endpoint.region", "us-east-1")
      configuration.setBoolean("fs.s3a.path.style.access", true)
      configuration.setBoolean("fs.s3a.connection.ssl.enabled", endpoint.startsWith("https://"))
      configuration.set(
        "fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
      configuration.set("fs.s3a.access.key", accessKey)
      configuration.set("fs.s3a.secret.key", secretKey)
    }
    configuration
  }

  private def requiredEnvironmentVariable(name: String): String =
    Option(System.getenv(name)).filter(_.nonEmpty).getOrElse {
      throw new IllegalArgumentException(s"$name must be set when using a custom S3 endpoint")
    }
}
