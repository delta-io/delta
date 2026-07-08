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

package io.sparkuctest;

import io.sparkuctest.mock.GcsCredentialFileSystem;
import io.sparkuctest.mock.GcsTimeBasedCredGenerator;
import java.util.Map;
import java.util.Properties;
import org.junit.jupiter.api.condition.DisabledIf;

/** Verifies GCS vended-credential renewal against the embedded UC server. */
@DisabledIf(
    value = "isUCRemoteConfigured",
    disabledReason =
        "Needs the embedded UC server + in-JVM manual clock; cannot run against a remote server.")
public class GcsCredRenewIT extends BaseCredRenewIT {
  @Override
  protected String scheme() {
    return "gs";
  }

  @Override
  protected Properties serverProperties() {
    Properties props = super.serverProperties();
    props.setProperty("gcs.bucketPath.0", "gs://" + BUCKET_NAME);
    // Issue a fresh credential on each mint so renewal is observable as the clock advances.
    // gcs.jsonKeyFilePath.0 is intentionally omitted: the custom generator is loaded first and the
    // key file is only a fallback for the default service-account generator.
    props.setProperty("gcs.credentialGenerator.0", GcsTimeBasedCredGenerator.class.getName());
    return props;
  }

  @Override
  protected Map<String, String> hadoopFsProps() {
    return Map.of("spark.hadoop.fs.gs.impl", GcsCredentialFileSystem.class.getName());
  }
}
