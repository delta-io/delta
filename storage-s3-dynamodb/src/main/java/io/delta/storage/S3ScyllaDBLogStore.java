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

import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * A concrete implementation of {@link BaseS3DynamoDBLogStore} that uses an external ScyllaDB table
 * to provide the mutual exclusion during calls to `putExternalEntry`.
 * <p>
 * ScyllaDB entries are of form
 * - key
 * -- tablePath (HASH, STRING)
 * -- filename (RANGE, STRING)
 * <p>
 * - attributes
 * -- tempPath (STRING, relative to _delta_log)
 * -- complete (STRING, representing boolean, "true" or "false")
 * -- commitTime (NUMBER, epoch seconds)
 */
public class S3ScyllaDBLogStore extends BaseS3DynamoDBLogStore {
    private static final Logger LOG = LoggerFactory.getLogger(S3ScyllaDBLogStore.class);

    /**
     * Configuration keys for the ScyllaDB client.
     */
    public static final String SCYLLA_DB_CONF_PREFIX = "S3ScyllaDBLogStore";
    public static final String DDB_CLIENT_ENDPOINT = "ddb.endpoint";


    /**
     * Member fields
     */
    private final String endpoint;

    public S3ScyllaDBLogStore(Configuration hadoopConf) throws IOException {
        super(hadoopConf);
        endpoint = getParam(hadoopConf, DDB_CLIENT_ENDPOINT, null);
        LOG.info("using endpoint {}", endpoint);
        initClient(hadoopConf);
    }

    @Override
    protected AmazonDynamoDB getClient() throws IOException {
        try {
            return AmazonDynamoDBClientBuilder.standard().withCredentials(getAwsCredentialsProvider())
                    .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(endpoint,""))
                    .build();
        } catch (ReflectiveOperationException e) {
            throw new IOException(e);
        }
    }

    @Override
    protected ProvisionedThroughput getProvisionedThroughput(Configuration hadoopConf) {
        //ScyllaDD ignores the ProvisionedThroughput setting,  yet we still require it for table creation through the DynamoDB API
        //see: https://opensource.docs.scylladb.com/stable/alternator/compatibility.html#provisioning
        return new ProvisionedThroughput();
    }

    @Override
    protected String getConfPrefix() {
        return SCYLLA_DB_CONF_PREFIX;
    }

}
