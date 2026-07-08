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

import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.hadoop.internal.UCHadoopConfConstants;
import io.unitycatalog.hadoop.internal.auth.AwsVendedTokenProvider;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;

/** Resolves the connector's vended S3 credentials and checks they are currently valid. */
public class S3CredentialFileSystem extends CredentialTestFileSystem<AwsCredentialsProvider> {
  @Override
  protected String scheme() {
    return "s3";
  }

  @Override
  protected AwsCredentialsProvider createProvider() {
    String clazz = getConf().get(UCHadoopConfConstants.S3A_CREDENTIALS_PROVIDER);
    assertThat(clazz).isEqualTo(AwsVendedTokenProvider.class.getName());
    // Constructing the provider also validates that the Hadoop conf the connector wired is usable.
    return new AwsVendedTokenProvider(getConf());
  }

  @Override
  protected long resolveCredentialMintMillis(AwsCredentialsProvider provider) {
    AwsSessionCredentials cred = (AwsSessionCredentials) provider.resolveCredentials();
    long ts = mintMillisOf(cred.accessKeyId(), "accessKeyId");
    // The three fields are stamped with the same mint timestamp; confirm they agree.
    assertThat(cred.secretAccessKey()).isEqualTo("secretAccessKey" + ts);
    assertThat(cred.sessionToken()).isEqualTo("sessionToken" + ts);
    return ts;
  }
}
