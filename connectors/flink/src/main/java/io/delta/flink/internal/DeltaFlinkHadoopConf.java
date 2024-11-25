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

package io.delta.flink.internal;

public class DeltaFlinkHadoopConf {
    /**
     * If enabled, DeltaGlobalCommitter will use delta-kernel to get snapshots when committing. This
     * can provide a signficate perfomance benefit in commit heavy workloads over the default
     * delta-standalone implementation.
     */
    public static String DELTA_KERNEL_ENABLED = "io.delta.flink.kernel.enabled";
}
