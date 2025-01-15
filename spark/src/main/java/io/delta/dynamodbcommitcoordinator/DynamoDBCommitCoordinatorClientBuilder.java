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

package io.delta.dynamodbcommitcoordinator;

import org.apache.spark.sql.delta.coordinatedcommits.CommitCoordinatorBuilder;
import org.apache.spark.sql.delta.sources.DeltaSQLConf;
import io.delta.storage.commit.CommitCoordinatorClient;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.SparkSession;
import scala.collection.immutable.Map;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClientBuilder;

import java.io.IOException;
import java.net.URI;

public class DynamoDBCommitCoordinatorClientBuilder implements CommitCoordinatorBuilder {

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
    private static final String COORDINATED_COMMITS_TABLE_NAME_KEY = "dynamoDBTableName";
    /**
     * The endpoint of the DynamoDB service. The value of this key is stored in the
     * `conf` which is passed to the `build` method.
     */
    private static final String DYNAMO_DB_ENDPOINT_KEY = "dynamoDBEndpoint";

    @Override
    public CommitCoordinatorClient build(SparkSession spark, Map<String, String> conf) {
        String coordinatedCommitsTableName = conf.get(COORDINATED_COMMITS_TABLE_NAME_KEY).getOrElse(() -> {
            throw new RuntimeException(COORDINATED_COMMITS_TABLE_NAME_KEY + " not found");
        });
        String dynamoDBEndpoint = conf.get(DYNAMO_DB_ENDPOINT_KEY).getOrElse(() -> {
            throw new RuntimeException(DYNAMO_DB_ENDPOINT_KEY + " not found");
        });
        String awsCredentialsProviderName =
                spark.conf().get(DeltaSQLConf.COORDINATED_COMMITS_DDB_AWS_CREDENTIALS_PROVIDER_NAME());
        int readCapacityUnits = Integer.parseInt(
                spark.conf().get(DeltaSQLConf.COORDINATED_COMMITS_DDB_READ_CAPACITY_UNITS().key()));
        int writeCapacityUnits = Integer.parseInt(
                spark.conf().get(DeltaSQLConf.COORDINATED_COMMITS_DDB_WRITE_CAPACITY_UNITS().key()));
        boolean skipPathCheck = Boolean.parseBoolean(
                spark.conf().get(DeltaSQLConf.COORDINATED_COMMITS_DDB_SKIP_PATH_CHECK().key()));
        try {
            DynamoDbClient ddbClient = createAmazonDDBClient(
                    dynamoDBEndpoint,
                    awsCredentialsProviderName,
                    spark.sessionState().newHadoopConf()
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
            DynamoDbClient ddbClient,
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

    protected DynamoDbClient createAmazonDDBClient(
            String endpoint,
            String credentialProviderName,
            Configuration hadoopConf
    ) throws ReflectiveOperationException {
        AwsCredentialsProvider awsCredentialsProvider =
                ReflectionUtils.createAwsCredentialsProvider(credentialProviderName, hadoopConf);

        DynamoDbClientBuilder builder = DynamoDbClient.builder()
                .credentialsProvider(awsCredentialsProvider)
                .httpClient(ApacheHttpClient.builder().build());

        if (endpoint != null) {
            builder.endpointOverride(URI.create(endpoint));
        }

        return builder.build();
    }
}
