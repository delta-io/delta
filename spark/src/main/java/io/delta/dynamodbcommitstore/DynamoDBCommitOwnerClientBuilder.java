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

package io.delta.dynamodbcommitstore;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import org.apache.spark.sql.delta.managedcommit.CommitOwnerBuilder;
import org.apache.spark.sql.delta.managedcommit.CommitOwnerClient;
import org.apache.spark.sql.SparkSession;
import scala.collection.immutable.Map;

import java.lang.reflect.InvocationTargetException;

public class DynamoDBCommitOwnerClientBuilder implements CommitOwnerBuilder {

    private final long BACKFILL_BATCH_SIZE = 1L;

    @Override
    public String getName() {
        return "dynamodb";
    }

    /**
     * Key for the name of the DynamoDB table which stores all the unbackfilled
     * commits for this owner. The value of this key is stored in the `conf`
     * which is passed to the `build` method.
     */
    private static final String MANAGED_COMMITS_TABLE_NAME_KEY = "managedCommitsTableName";
    /**
     * Key for the endpoint of the DynamoDB service. The value of this key is stored in the
     * `conf` which is passed to the `build` method.
     */
    private static final String DYNAMO_DB_ENDPOINT_KEY = "dynamoDBEndpoint";

    /**
     * The AWS credentials provider chain to use when creating the DynamoDB client.
     * This has temporarily been hardcoded until we have a way to read from sparkSession.
     */
    private static final String AWS_CREDENTIALS_PROVIDER =
            "com.amazonaws.auth.DefaultAWSCredentialsProviderChain";

    // TODO: update this interface so that it can take a sparkSession.
    @Override
    public CommitOwnerClient build(SparkSession spark, Map<String, String> conf) {
        String managedCommitsTableName = conf.get(MANAGED_COMMITS_TABLE_NAME_KEY).getOrElse(() -> {
            throw new RuntimeException(MANAGED_COMMITS_TABLE_NAME_KEY + " not found");
        });
        String dynamoDBEndpoint = conf.get(DYNAMO_DB_ENDPOINT_KEY).getOrElse(() -> {
            throw new RuntimeException(DYNAMO_DB_ENDPOINT_KEY + " not found");
        });
        try {
            AmazonDynamoDBClient client =
                    createAmazonDDBClient(dynamoDBEndpoint, AWS_CREDENTIALS_PROVIDER);
            return new DynamoDBCommitOwnerClient(
                    managedCommitsTableName, dynamoDBEndpoint, client, BACKFILL_BATCH_SIZE);
        } catch (Exception e) {
            throw new RuntimeException("Failed to create DynamoDB client", e);
        }
    }

    private AmazonDynamoDBClient createAmazonDDBClient(
            String endpoint,
            String credentialProviderName
    ) throws NoSuchMethodException,
            ClassNotFoundException,
            InvocationTargetException,
            InstantiationException,
            IllegalAccessException {
        Class<?> awsCredentialsProviderClass = Class.forName(credentialProviderName);
        AWSCredentialsProvider awsCredentialsProvider =
                (AWSCredentialsProvider) awsCredentialsProviderClass.getConstructor().newInstance();
        AmazonDynamoDBClient client = new AmazonDynamoDBClient(awsCredentialsProvider);
        client.setEndpoint(endpoint);
        return client;
    }
}
