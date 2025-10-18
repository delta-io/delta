/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.delta.flink.internal.table;

import java.io.File;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class to discover and process Hadoop configuration on Flink cluster.
 *
 * <p>
 * This class was backport from Flink's flink-hadoop-fs module, and it contains a subset of methods
 * comparing to the original class. We kept only needed methods.
 * <p>
 * The reason for backport this to connector code was that original implementation requires various
 * additional hadoop classes to be loaded on the classpath which required adding additional hadoop
 * dependencies to the project.
 */
public class HadoopUtils {

    private static final Logger LOG = LoggerFactory.getLogger(HadoopUtils.class);
    /**
     * The prefixes that Flink adds to the Hadoop config.
     */
    private static final String[] FLINK_CONFIG_PREFIXES = {"flink.hadoop."};

    /**
     * Creates Hadoop configuration object, for that it looks for Hadoop config in env variables and
     * Flink cluster configuration (deprecated). The order of loaded configuration is as such:
     * <ul>
     *     <li>HADOOP_HOME environment</li>
     *     <li>hdfs-default.xml pointed by deprecated flink config option `fs.hdfs.hdfsdefault`
     *     (deprecated)
     *     </li>
     *     <li>HADOOP_CONF_DIR environment</li>
     *     <li>Properties from Flink cluster config prefixed with `flink.hadoop`</li>
     * </ul>
     *
     * @param flinkConfiguration Flink cluster configuration.
     * @return Hadoop's configuration object.
     */
    @SuppressWarnings("deprecation")
    public static Configuration getHadoopConfiguration(
        org.apache.flink.configuration.Configuration flinkConfiguration) {

        // Instantiate an HdfsConfiguration to load the hdfs-site.xml and hdfs-default.xml
        // from the classpath

        Configuration result = new Configuration();
        boolean foundHadoopConfiguration = false;

        // We need to load both core-site.xml and hdfs-site.xml to determine the default fs path and
        // the hdfs configuration.
        // The properties of a newly added resource will override the ones in previous resources, so
        // a configuration
        // file with higher priority should be added later.

        // Approach 1: HADOOP_HOME environment variables
        String[] possibleHadoopConfPaths = new String[2];

        final String hadoopHome = System.getenv("HADOOP_HOME");
        if (hadoopHome != null) {
            LOG.debug("Searching Hadoop configuration files in HADOOP_HOME: {}", hadoopHome);
            possibleHadoopConfPaths[0] = hadoopHome + "/conf";
            possibleHadoopConfPaths[1] = hadoopHome + "/etc/hadoop"; // hadoop 2.2
        }

        for (String possibleHadoopConfPath : possibleHadoopConfPaths) {
            if (possibleHadoopConfPath != null) {
                foundHadoopConfiguration = addHadoopConfIfFound(result, possibleHadoopConfPath);
            }
        }

        // Approach 2: Flink configuration (deprecated)
        final String hdfsDefaultPath =
            flinkConfiguration.getString(ConfigConstants.HDFS_DEFAULT_CONFIG, null);
        if (hdfsDefaultPath != null) {
            result.addResource(new org.apache.hadoop.fs.Path(hdfsDefaultPath));
            LOG.debug(
                "Using hdfs-default configuration-file path from Flink config: {}",
                hdfsDefaultPath);
            foundHadoopConfiguration = true;
        }

        final String hdfsSitePath =
            flinkConfiguration.getString(ConfigConstants.HDFS_SITE_CONFIG, null);
        if (hdfsSitePath != null) {
            result.addResource(new org.apache.hadoop.fs.Path(hdfsSitePath));
            LOG.debug(
                "Using hdfs-site configuration-file path from Flink config: {}", hdfsSitePath);
            foundHadoopConfiguration = true;
        }

        final String hadoopConfigPath =
            flinkConfiguration.getString(ConfigConstants.PATH_HADOOP_CONFIG, null);
        if (hadoopConfigPath != null) {
            LOG.debug("Searching Hadoop configuration files in Flink config: {}", hadoopConfigPath);
            foundHadoopConfiguration =
                addHadoopConfIfFound(result, hadoopConfigPath) || foundHadoopConfiguration;
        }

        // Approach 3: HADOOP_CONF_DIR environment variable
        String hadoopConfDir = System.getenv("HADOOP_CONF_DIR");
        if (hadoopConfDir != null) {
            LOG.debug("Searching Hadoop configuration files in HADOOP_CONF_DIR: {}", hadoopConfDir);
            foundHadoopConfiguration =
                addHadoopConfIfFound(result, hadoopConfDir) || foundHadoopConfiguration;
        }

        // Approach 4: Flink configuration
        // add all configuration key with prefix 'flink.hadoop.' in flink conf to hadoop conf
        for (String key : flinkConfiguration.keySet()) {
            for (String prefix : FLINK_CONFIG_PREFIXES) {
                if (key.startsWith(prefix)) {
                    String newKey = key.substring(prefix.length());
                    String value = flinkConfiguration.getString(key, null);
                    result.set(newKey, value);
                    LOG.debug(
                        "Adding Flink config entry for {} as {}={} to Hadoop config",
                        key,
                        newKey,
                        value);
                    foundHadoopConfiguration = true;
                }
            }
        }

        if (!foundHadoopConfiguration) {
            LOG.warn(
                "Could not find Hadoop configuration via any of the supported methods "
                    + "(Flink configuration, environment variables).");
        }

        return result;
    }

    /**
     * Search Hadoop configuration files in the given path, and add them to the configuration if
     * found.
     */
    private static boolean addHadoopConfIfFound(
            Configuration configuration,
            String possibleHadoopConfPath) {

        boolean foundHadoopConfiguration = false;
        if (new File(possibleHadoopConfPath).exists()) {
            if (new File(possibleHadoopConfPath + "/core-site.xml").exists()) {
                configuration.addResource(
                    new org.apache.hadoop.fs.Path(possibleHadoopConfPath + "/core-site.xml"));
                LOG.debug(
                    "Adding "
                        + possibleHadoopConfPath
                        + "/core-site.xml to hadoop configuration");
                foundHadoopConfiguration = true;
            }
            if (new File(possibleHadoopConfPath + "/hdfs-site.xml").exists()) {
                configuration.addResource(
                    new org.apache.hadoop.fs.Path(possibleHadoopConfPath + "/hdfs-site.xml"));
                LOG.debug(
                    "Adding "
                        + possibleHadoopConfPath
                        + "/hdfs-site.xml to hadoop configuration");
                foundHadoopConfiguration = true;
            }
        }
        return foundHadoopConfiguration;
    }
}
