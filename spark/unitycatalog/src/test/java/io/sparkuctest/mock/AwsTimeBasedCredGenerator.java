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

package io.sparkuctest.mock;

import io.unitycatalog.server.service.credential.aws.AwsCredentialGenerator;
import java.time.Instant;
import software.amazon.awssdk.services.sts.model.Credentials;

/**
 * Server-side AWS credential generator used by both the renewal suites (manual clock, short
 * duration) and the general suites (system clock, long duration); the mode is chosen via {@link
 * TimeBasedCredGenerator#useManualClock}/{@link TimeBasedCredGenerator#useSystemClock}. It stamps
 * each field with the mint timestamp so {@link S3CredentialFileSystem} can parse it back out.
 *
 * <p>Loaded reflectively by the UC server via a no-arg constructor.
 */
public class AwsTimeBasedCredGenerator extends TimeBasedCredGenerator<Credentials>
    implements AwsCredentialGenerator {
  @Override
  protected Credentials newTimeBasedCred(long mintMillis, long durMillis) {
    return Credentials.builder()
        .accessKeyId("accessKeyId" + mintMillis)
        .secretAccessKey("secretAccessKey" + mintMillis)
        .sessionToken("sessionToken" + mintMillis)
        .expiration(Instant.ofEpochMilli(mintMillis + durMillis))
        .build();
  }
}
