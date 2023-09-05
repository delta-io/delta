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

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import org.apache.hadoop.conf.Configuration;

public class ReflectionsUtilsSuiteHelper {
    // this class only purpose to test DynamoDBLogStore logic to create AWS credentials provider with reflection.
    public static class TestOnlyAWSCredentialsProviderWithHadoopConf implements AWSCredentialsProvider {

        public TestOnlyAWSCredentialsProviderWithHadoopConf(Configuration hadoopConf) {}

        @Override
        public AWSCredentials getCredentials() {
            return null;
        }

        @Override
        public void refresh() {

        }
    }

    // this class only purpose to test DynamoDBLogStore logic to create AWS credentials provider with reflection.
    public static class TestOnlyAWSCredentialsProviderWithUnexpectedConstructor implements AWSCredentialsProvider {

        public TestOnlyAWSCredentialsProviderWithUnexpectedConstructor(String hadoopConf) {}

        @Override
        public AWSCredentials getCredentials() {
            return null;
        }

        @Override
        public void refresh() {

        }
    }
}
