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

import io.delta.storage.utils.ReflectionUtils;
import org.apache.hadoop.fs.Path;

import java.io.InterruptedIOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A concrete implementation of {@link BaseExternalLogStore} that uses an external DynamoDB table
 * to provide the mutual exclusion during calls to `putExternalEntry`.
 *
 * DynamoDB entries are of form
 * - key
 * -- tablePath (HASH, STRING)
 * -- filename (RANGE, STRING)
 *
 * - attributes
 * -- tempPath (STRING, relative to _delta_log)
 * -- complete (STRING, representing boolean, "true" or "false")
 * -- commitTime (NUMBER, epoch seconds)
 */
public class S3DynamoDBLogStore extends BaseExternalLogStore {
    private static final Logger LOG = LoggerFactory.getLogger(S3DynamoDBLogStore.class);

    /**
     * Configuration keys for the DynamoDB client.
     *
     * Keys are either of the form $SPARK_CONF_PREFIX.$CONF or $BASE_CONF_PREFIX.$CONF,
     * e.g. spark.io.delta.storage.S3DynamoDBLogStore.ddb.tableName
     * or io.delta.storage.S3DynamoDBLogStore.ddb.tableName
     */
    public static final String SPARK_CONF_PREFIX = "spark.io.delta.storage.S3DynamoDBLogStore";
    public static final String BASE_CONF_PREFIX = "io.delta.storage.S3DynamoDBLogStore";
    public static final String READ_RETRIES = "read.retries";
    public static final String DDB_CLIENT_TABLE = "ddb.tableName";
    public static final String DDB_CLIENT_REGION = "ddb.region";
    public static final String DDB_CLIENT_CREDENTIALS_PROVIDER = "credentials.provider";
    public static final String DDB_CREATE_TABLE_RCU = "provisionedThroughput.rcu";
    public static final String DDB_CREATE_TABLE_WCU = "provisionedThroughput.wcu";

    // WARNING: setting this value too low can cause data loss. Defaults to a duration of 1 day.
    public static final String TTL_SECONDS = "ddb.ttl";

    /**
     * DynamoDB table attribute keys
     */
    private static final String ATTR_TABLE_PATH = "tablePath";
    private static final String ATTR_FILE_NAME = "fileName";
    private static final String ATTR_TEMP_PATH = "tempPath";
    private static final String ATTR_COMPLETE = "complete";
    private static final String ATTR_EXPIRE_TIME = "expireTime";

    /**
     * Member fields
     */
    private final AmazonDynamoDB client;
    private final String tableName;
    private final String credentialsProviderName;
    private final String regionName;
    private final long expirationDelaySeconds;

    public S3DynamoDBLogStore(Configuration hadoopConf) throws IOException {
        super(hadoopConf);

        tableName = getParam(hadoopConf, DDB_CLIENT_TABLE, "delta_log");
        credentialsProviderName = getParam(
            hadoopConf,
            DDB_CLIENT_CREDENTIALS_PROVIDER,
            "com.amazonaws.auth.DefaultAWSCredentialsProviderChain"
        );
        regionName = getParam(hadoopConf, DDB_CLIENT_REGION, "us-east-1");

        final String ttl = getParam(hadoopConf, TTL_SECONDS, null);
        expirationDelaySeconds = ttl == null ?
            BaseExternalLogStore.DEFAULT_EXTERNAL_ENTRY_EXPIRATION_DELAY_SECONDS :
            Long.parseLong(ttl);
        if (expirationDelaySeconds < 0) {
            throw new IllegalArgumentException(
                String.format(
                    "Can't use negative `%s` value of %s", TTL_SECONDS, expirationDelaySeconds));
        }

        LOG.info("using tableName {}", tableName);
        LOG.info("using credentialsProviderName {}", credentialsProviderName);
        LOG.info("using regionName {}", regionName);
        LOG.info("using ttl (seconds) {}", expirationDelaySeconds);

        client = getClient();
        tryEnsureTableExists(hadoopConf);
    }

    @Override
    public CloseableIterator<String> read(Path path, Configuration hadoopConf) throws IOException {
        // With many concurrent readers/writers, there's a chance that concurrent 'recovery'
        // operations occur on the same file, i.e. the same temp file T(N) is copied into the target
        // N.json file more than once. Though data loss will *NOT* occur, readers of N.json may
        // receive a RemoteFileChangedException from S3 as the ETag of N.json was changed. This is
        // safe to retry, so we do so here.
        final int maxRetries = Integer.parseInt(
            getParam(
                hadoopConf,
                READ_RETRIES,
                Integer.toString(RetryableCloseableIterator.DEFAULT_MAX_RETRIES)
            )
        );

        return new RetryableCloseableIterator(() -> super.read(path, hadoopConf), maxRetries);
    }

    @Override
    protected long getExpirationDelaySeconds() {
        return expirationDelaySeconds;
    }

    @Override
    protected void putExternalEntry(
            ExternalCommitEntry entry,
            boolean overwrite) throws IOException {
        try {
            LOG.debug(String.format("putItem %s, overwrite: %s", entry, overwrite));
            client.putItem(createPutItemRequest(entry, overwrite));
        } catch (ConditionalCheckFailedException e) {
            LOG.debug(e.toString());
            throw new java.nio.file.FileAlreadyExistsException(
                entry.absoluteFilePath().toString()
            );
        }
    }

    @Override
    protected Optional<ExternalCommitEntry> getExternalEntry(
            String tablePath,
            String fileName) {
        final Map<String, AttributeValue> attributes = new ConcurrentHashMap<>();
        attributes.put(ATTR_TABLE_PATH, new AttributeValue(tablePath));
        attributes.put(ATTR_FILE_NAME, new AttributeValue(fileName));

        Map<String, AttributeValue> item = client.getItem(
            new GetItemRequest(tableName, attributes).withConsistentRead(true)
        ).getItem();

        return item != null ? Optional.of(dbResultToCommitEntry(item)) : Optional.empty();
    }

    @Override
    protected Optional<ExternalCommitEntry> getLatestExternalEntry(Path tablePath) {
        final Map<String, Condition> conditions = new ConcurrentHashMap<>();
        conditions.put(
            ATTR_TABLE_PATH,
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
        final AttributeValue expireTimeAttr = item.get(ATTR_EXPIRE_TIME);
        return new ExternalCommitEntry(
            new Path(item.get(ATTR_TABLE_PATH).getS()),
            item.get(ATTR_FILE_NAME).getS(),
            item.get(ATTR_TEMP_PATH).getS(),
            item.get(ATTR_COMPLETE).getS().equals("true"),
            expireTimeAttr != null ? Long.parseLong(expireTimeAttr.getN()) : null
        );
    }

    private PutItemRequest createPutItemRequest(ExternalCommitEntry entry, boolean overwrite) {
        final Map<String, AttributeValue> attributes = new ConcurrentHashMap<>();
        attributes.put(ATTR_TABLE_PATH, new AttributeValue(entry.tablePath.toString()));
        attributes.put(ATTR_FILE_NAME, new AttributeValue(entry.fileName));
        attributes.put(ATTR_TEMP_PATH, new AttributeValue(entry.tempPath));
        attributes.put(
            ATTR_COMPLETE,
            new AttributeValue().withS(Boolean.toString(entry.complete))
        );

        if (entry.expireTime != null) {
            attributes.put(
                ATTR_EXPIRE_TIME,
                new AttributeValue().withN(entry.expireTime.toString())
            );
        }

        final PutItemRequest pr = new PutItemRequest(tableName, attributes);

        if (!overwrite) {
            Map<String, ExpectedAttributeValue> expected = new ConcurrentHashMap<>();
            expected.put(ATTR_FILE_NAME, new ExpectedAttributeValue(false));
            pr.withExpected(expected);
        }

        return pr;
    }

    private void tryEnsureTableExists(Configuration hadoopConf) throws IOException {
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
                final long rcu = Long.parseLong(getParam(hadoopConf, DDB_CREATE_TABLE_RCU, "5"));
                final long wcu = Long.parseLong(getParam(hadoopConf, DDB_CREATE_TABLE_WCU, "5"));

                LOG.info(
                    "DynamoDB table `{}` in region `{}` does not exist. " +
                    "Creating it now with provisioned throughput of {} RCUs and {} WCUs.",
                    tableName, regionName, rcu, wcu);
                try {
                    client.createTable(
                        // attributeDefinitions
                        java.util.Arrays.asList(
                            new AttributeDefinition(ATTR_TABLE_PATH, ScalarAttributeType.S),
                            new AttributeDefinition(ATTR_FILE_NAME, ScalarAttributeType.S)
                        ),
                        tableName,
                        // keySchema
                        Arrays.asList(
                            new KeySchemaElement(ATTR_TABLE_PATH, KeyType.HASH),
                            new KeySchemaElement(ATTR_FILE_NAME, KeyType.RANGE)
                        ),
                        new ProvisionedThroughput(rcu, wcu)
                    );
                    created = true;
                } catch (ResourceInUseException e3) {
                    // race condition - table just created by concurrent process
                }
            }
            if (status.equals("ACTIVE")) {
                if (created) {
                    LOG.info("Successfully created DynamoDB table `{}`", tableName);
                } else {
                    LOG.info("Table `{}` already exists", tableName);
                }
                break;
            } else if (status.equals("CREATING")) {
                retries += 1;
                LOG.info("Waiting for `{}` table creation", tableName);
                try {
                    Thread.sleep(1000);
                } catch(InterruptedException e) {
                    throw new InterruptedIOException(e.getMessage());
                }
            } else {
                LOG.error("table `{}` status: {}", tableName, status);
                break;  // TODO - raise exception?
            }
        };
    }

    /**
     * Create the Amazon DynamoDB client.
     */
    protected AmazonDynamoDB getClient() throws java.io.IOException {
        try {
            final AWSCredentialsProvider awsCredentialsProvider =
                    ReflectionUtils.createAwsCredentialsProvider(credentialsProviderName, initHadoopConf());
            return AmazonDynamoDBClientBuilder.standard()
                .withCredentials(awsCredentialsProvider)
                .withRegion(regionName)
                .build();
        } catch (ReflectiveOperationException e) {
            throw new java.io.IOException(e);
        }
    }

    /**
     * Get the hadoopConf param $name that is prefixed either with $SPARK_CONF_PREFIX or
     * $BASE_CONF_PREFIX.
     *
     * If two parameters exist, one for each prefix, then an IllegalArgumentException is thrown.
     *
     * If no parameters exist, then the $defaultValue is returned.
     */
    protected static String getParam(Configuration hadoopConf, String name, String defaultValue) {
        final String sparkPrefixKey = String.format("%s.%s", SPARK_CONF_PREFIX, name);
        final String basePrefixKey = String.format("%s.%s", BASE_CONF_PREFIX, name);

        final String sparkPrefixVal = hadoopConf.get(sparkPrefixKey);
        final String basePrefixVal = hadoopConf.get(basePrefixKey);

        if (sparkPrefixVal != null &&
            basePrefixVal != null &&
            !sparkPrefixVal.equals(basePrefixVal)) {
            throw new IllegalArgumentException(
                String.format(
                    "Configuration properties `%s=%s` and `%s=%s` have different values. " +
                    "Please set only one.",
                    sparkPrefixKey, sparkPrefixVal, basePrefixKey, basePrefixVal
                )
            );
        }

        if (sparkPrefixVal != null) return sparkPrefixVal;
        if (basePrefixVal != null) return basePrefixVal;
        return defaultValue;
    }
}
