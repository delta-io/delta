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

import io.sparkuctest.mock.AwsTimeBasedCredGenerator;
import io.sparkuctest.mock.S3CredentialFileSystem;
import java.util.Map;
import java.util.Properties;
import org.junit.jupiter.api.condition.DisabledIf;

/** Verifies S3 (AWS) vended-credential renewal against the embedded UC server. */
@DisabledIf(
    value = "isUCRemoteConfigured",
    disabledReason =
        "Needs the embedded UC server + in-JVM manual clock; cannot run against a remote server.")
public class AwsCredRenewIT extends BaseCredRenewIT {
  @Override
  protected String scheme() {
    return "s3";
  }

  @Override
  protected Properties serverProperties() {
    Properties props = super.serverProperties();
    props.setProperty("s3.bucketPath.0", "s3://" + BUCKET_NAME);
    props.setProperty("s3.accessKey.0", "accessKey0");
    props.setProperty("s3.secretKey.0", "secretKey0");
    props.setProperty("s3.sessionToken.0", "sessionToken0");
    // Issue a fresh credential on each mint so renewal is observable as the clock advances. The
    // base class switches this shared generator to the manual clock for the renewal suites.
    props.setProperty("s3.credentialGenerator.0", AwsTimeBasedCredGenerator.class.getName());
    return props;
  }

  @Override
  protected Map<String, String> hadoopFsProps() {
    return Map.of("spark.hadoop.fs.s3.impl", S3CredentialFileSystem.class.getName());
  }
}
