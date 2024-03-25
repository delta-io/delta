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

package io.delta.storage.utils;

import org.apache.hadoop.conf.Configuration;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.identity.spi.AwsCredentialsIdentity;
import software.amazon.awssdk.identity.spi.ResolveIdentityRequest;

import java.util.concurrent.CompletableFuture;

public class ReflectionsUtilsSuiteHelper {
    // this class only purpose to test DynamoDBLogStore logic to create AWS credentials provider with reflection.
    public static class TestOnlyAWSCredentialsProviderWithHadoopConf implements AwsCredentialsProvider {

        public TestOnlyAWSCredentialsProviderWithHadoopConf(Configuration hadoopConf) {}

        @Override
        public AwsCredentials resolveCredentials() {
            return null;
        }

        @Override
        public Class<AwsCredentialsIdentity> identityType() {
            return AwsCredentialsProvider.super.identityType();
        }

        @Override
        public CompletableFuture<AwsCredentialsIdentity> resolveIdentity(ResolveIdentityRequest request) {
            return AwsCredentialsProvider.super.resolveIdentity(request);
        }
    }

    // this class only purpose to test DynamoDBLogStore logic to create AWS credentials provider with reflection.
    public static class TestOnlyAWSCredentialsProviderWithUnexpectedConstructor implements AwsCredentialsProvider {

        public TestOnlyAWSCredentialsProviderWithUnexpectedConstructor(String hadoopConf) {}

        @Override
        public AwsCredentials resolveCredentials() {
            return null;
        }

        @Override
        public Class<AwsCredentialsIdentity> identityType() {
            return AwsCredentialsProvider.super.identityType();
        }

        @Override
        public CompletableFuture<AwsCredentialsIdentity> resolveIdentity(ResolveIdentityRequest request) {
            return AwsCredentialsProvider.super.resolveIdentity(request);
        }
    }
}
