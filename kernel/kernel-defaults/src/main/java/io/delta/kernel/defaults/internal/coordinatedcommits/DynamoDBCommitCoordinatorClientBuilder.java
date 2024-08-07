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

package io.delta.kernel.defaults.internal.coordinatedcommits;

import java.io.IOException;
import java.util.Map;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import io.delta.storage.commit.CommitCoordinatorClient;
import org.apache.hadoop.conf.Configuration;

public class DynamoDBCommitCoordinatorClientBuilder extends CommitCoordinatorBuilder {

    private final long BACKFILL_BATCH_SIZE = 10L;
    private final Configuration initHadoopConf;

    public DynamoDBCommitCoordinatorClientBuilder(Configuration initHadoopConf) {
        super(initHadoopConf);
        this.initHadoopConf = initHadoopConf;
    }

    @Override
    public String getName() {
        return "dynamodb";
    }

    /**
     * Key for the name of the DynamoDB table which stores all the unbackfilled
     * commits for this owner. The value of this key is stored in the `conf`
     * which is passed to the `build` method.
     */
    private static final String COORDINATED_COMMITS_TABLE_NAME_KEY = "dynamoDBTableName";
    /**
     * The endpoint of the DynamoDB service. The value of this key is stored in the
     * `conf` which is passed to the `build` method.
     */
    private static final String DYNAMO_DB_ENDPOINT_KEY = "dynamoDBEndpoint";

    @Override
    public CommitCoordinatorClient build(Map<String, String> conf) {
        String coordinatedCommitsTableName = conf.get(COORDINATED_COMMITS_TABLE_NAME_KEY);
        if (coordinatedCommitsTableName == null) {
            throw new RuntimeException(COORDINATED_COMMITS_TABLE_NAME_KEY + " not found");
        }
        String dynamoDBEndpoint = conf.get(DYNAMO_DB_ENDPOINT_KEY);
        if (dynamoDBEndpoint == null) {
            throw new RuntimeException(DYNAMO_DB_ENDPOINT_KEY + " not found");
        }
        String awsCredentialsProviderName = "com.amazonaws.auth.DefaultAWSCredentialsProviderChain";
        int readCapacityUnits = 5;
        int writeCapacityUnits = 5;
        boolean skipPathCheck = false;
        try {
            AmazonDynamoDB ddbClient = createAmazonDDBClient(
                    dynamoDBEndpoint,
                    awsCredentialsProviderName,
                    initHadoopConf
            );
            return getDynamoDBCommitCoordinatorClient(
                    coordinatedCommitsTableName,
                    dynamoDBEndpoint,
                    ddbClient,
                    BACKFILL_BATCH_SIZE,
                    readCapacityUnits,
                    writeCapacityUnits,
                    skipPathCheck
            );
        } catch (Exception e) {
            throw new RuntimeException("Failed to create DynamoDB client", e);
        }
    }

    protected DynamoDBCommitCoordinatorClient getDynamoDBCommitCoordinatorClient(
            String coordinatedCommitsTableName,
            String dynamoDBEndpoint,
            AmazonDynamoDB ddbClient,
            long backfillBatchSize,
            int readCapacityUnits,
            int writeCapacityUnits,
            boolean skipPathCheck
    ) throws IOException {
        return new DynamoDBCommitCoordinatorClient(
                coordinatedCommitsTableName,
                dynamoDBEndpoint,
                ddbClient,
                backfillBatchSize,
                readCapacityUnits,
                writeCapacityUnits,
                skipPathCheck
        );
    }

    protected AmazonDynamoDB createAmazonDDBClient(
            String endpoint,
            String credentialProviderName,
            Configuration hadoopConf
    ) throws ReflectiveOperationException {
        AWSCredentialsProvider awsCredentialsProvider =
                ReflectionUtils.createAwsCredentialsProvider(credentialProviderName, hadoopConf);
        AmazonDynamoDBClient client = new AmazonDynamoDBClient(awsCredentialsProvider);
        client.setEndpoint(endpoint);
        return client;
    }
}
