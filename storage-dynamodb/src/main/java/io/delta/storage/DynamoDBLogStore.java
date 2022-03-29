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

package io.delta.storage;

import org.apache.hadoop.fs.Path;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException;
import com.amazonaws.services.dynamodbv2.model.DescribeTableResult;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.amazonaws.services.dynamodbv2.model.ExpectedAttributeValue;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.amazonaws.services.dynamodbv2.model.ResourceInUseException;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.auth.AWSCredentialsProvider;

import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TODO
 */
public class DynamoDBLogStore extends BaseExternalLogStore {
    private static final Logger LOG = LoggerFactory.getLogger(DynamoDBLogStore.class);

    private final AmazonDynamoDBClient client;
    private final String tableName;
    private final String credentialsProviderName;
    private final String regionName;
    private final String endpoint;

    public DynamoDBLogStore(Configuration hadoopConf) throws IOException {
        super(hadoopConf);
        tableName = getParam(hadoopConf, "tableName", "delta_log");
        credentialsProviderName = getParam(
            hadoopConf,
            "credentials.provider",
            "com.amazonaws.auth.DefaultAWSCredentialsProviderChain"
        );
        regionName = getParam(hadoopConf, "region", "us-east-1");
        endpoint = getParam(hadoopConf, "endpoint", null);
        client = getClient();
        tryEnsureTableExists(hadoopConf);
    }

    @Override
    protected void putExternalEntry(
            ExternalCommitEntry entry,
            boolean overwrite) throws IOException {
        try {
            LOG.debug(String.format("putItem %s, overwrite: %s", entry, overwrite));
            client.putItem(createPutItemRequest(entry, tableName, overwrite));
        } catch (ConditionalCheckFailedException e) {
            LOG.debug(e.toString());
            throw new java.nio.file.FileAlreadyExistsException(
                entry.absoluteJsonPath().toString()
            );
        }
    }

    @Override
    protected Optional<ExternalCommitEntry> getExternalEntry(
            Path absoluteTablePath,
            Path absoluteJsonPath) {
        final Map<String, AttributeValue> attributes = new ConcurrentHashMap<>();
        attributes.put("tablePath", new AttributeValue(absoluteTablePath.toString()));
        attributes.put("fileName", new AttributeValue(absoluteJsonPath.toString()));

        Map<String, AttributeValue> item = client.getItem(
            new GetItemRequest(tableName, attributes).withConsistentRead(true)
        ).getItem();

        return item != null ? Optional.of(dbResultToCommitEntry(item)) : Optional.empty();
    }

    @Override
    protected Optional<ExternalCommitEntry> getLatestExternalEntry(Path tablePath) {
        final Map<String, Condition> conditions = new ConcurrentHashMap<>();
        conditions.put(
            "tablePath",
            new Condition()
                .withComparisonOperator(ComparisonOperator.EQ)
                .withAttributeValueList(new AttributeValue(tablePath.toString()))
        );

        final List<Map<String,AttributeValue>> items = client.query(
            new QueryRequest(tableName)
                .withConsistentRead(true)
                .withScanIndexForward(false)
                .withLimit(1)
                .withKeyConditions(conditions)
        ).getItems();

        if (items.isEmpty()) {
            return Optional.empty();
        } else {
            return Optional.of(dbResultToCommitEntry(items.get(0)));
        }
    }

    /**
     * Map a DBB query result item to an {@link ExternalCommitEntry}.
     */
    private ExternalCommitEntry dbResultToCommitEntry(Map<String, AttributeValue> item) {
        final AttributeValue commitTimeAttr = item.get("commitTime");
        return new ExternalCommitEntry(
            new Path(item.get("tablePath").getS()),
            item.get("fileName").getS(),
            item.get("tempPath").getS(),
            item.get("complete").getS() == "true",
            commitTimeAttr != null ? Long.parseLong(commitTimeAttr.getN()) : null
        );
    }

    private PutItemRequest createPutItemRequest(
            ExternalCommitEntry entry,
            String tableName,
            boolean overwrite) {

        final Map<String, AttributeValue> attributes = new ConcurrentHashMap<>();
        attributes.put("tablePath", new AttributeValue(entry.tablePath.toString()));
        attributes.put("fileName", new AttributeValue(entry.fileName));
        attributes.put("tempPath", new AttributeValue(entry.tempPath));
        attributes.put(
            "complete",
            new AttributeValue().withS(Boolean.toString(entry.complete))
        );

        if (entry.complete) {
            attributes.put("commitTime", new AttributeValue().withN(entry.commitTime.toString()));
        }

        final PutItemRequest pr = new PutItemRequest(tableName, attributes);

        if(!overwrite) {
            Map<String, ExpectedAttributeValue> expected = new ConcurrentHashMap<>();
            expected.put("fileName", new ExpectedAttributeValue(false));
            pr.withExpected(expected);
        }
        return pr;
    }

    private void tryEnsureTableExists(Configuration hadoopConf) {
        int retries = 0;
        boolean created = false;
        while(retries < 20) {
            String status = "CREATING";
            try {
                // https://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/dynamodbv2/model/TableDescription.html#getTableStatus--
                DescribeTableResult result = client.describeTable(tableName);
                TableDescription descr = result.getTable();
                status = descr.getTableStatus();
            } catch (ResourceNotFoundException e) {
                long rcu = Long.parseLong(getParam(hadoopConf, "provisionedThroughput.rcu", "5"));
                long wcu = Long.parseLong(getParam(hadoopConf, "provisionedThroughput.wcu", "5"));

                LOG.info(
                    "DynamoDB table `{}` in region `{}` does not exist."
                    + "Creating it now with provisioned throughput of {} RCUs and {} WCUs.",
                    tableName, regionName, rcu, wcu);
                try {
                    client.createTable(
                        // attributeDefinitions
                        java.util.Arrays.asList(
                            new AttributeDefinition("tablePath", ScalarAttributeType.S),
                            new AttributeDefinition("fileName", ScalarAttributeType.S)
                        ),
                        tableName,
                        // keySchema
                        Arrays.asList(
                            new KeySchemaElement("tablePath", KeyType.HASH),
                            new KeySchemaElement("fileName", KeyType.RANGE)
                        ),
                        new ProvisionedThroughput(rcu, wcu)
                    );
                    created = true;
                } catch (ResourceInUseException e3) {
                    // race condition - table just created by concurrent process
                }
            }
            if (status.equals("ACTIVE")) {
                if(created) {
                    LOG.info("Successfully created DynamoDB table `{}`", tableName);
                } else {
                    LOG.info("Table `{}` already exists", tableName);
                }
                break;
            } else if(status.equals("CREATING")) {
                retries += 1;
                LOG.info("Waiting for `{}` table creation", tableName);
                try {
                    Thread.sleep(1000);
                } catch(InterruptedException interrupted) {}
            } else {
                LOG.error("table `{}` status: {}", tableName, status);
                break;  // TODO - raise exception?
            }
        };
    }

    private AmazonDynamoDBClient getClient() throws java.io.IOException {
        try {
            final AWSCredentialsProvider auth =
                (AWSCredentialsProvider)Class.forName(credentialsProviderName)
                .getConstructor().newInstance();
            final AmazonDynamoDBClient client = new AmazonDynamoDBClient(auth);
            client.setRegion(Region.getRegion(Regions.fromName(regionName)));
            if (endpoint != null) {
                client.setEndpoint(endpoint);
            }
            return client;
        } catch(
            ClassNotFoundException
            | InstantiationException
            | NoSuchMethodException
            | IllegalAccessException
            | java.lang.reflect.InvocationTargetException e
        ) {
            throw new java.io.IOException(e);
        }
    }

    protected String getParam(Configuration config, String name, String defaultValue) {
        return config.get(
            String.format("spark.delta.DynamoDBLogStore.%s", name),
            defaultValue
        );
    }
}
