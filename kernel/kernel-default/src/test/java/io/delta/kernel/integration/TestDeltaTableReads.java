/*
 * Copyright (2023) The Delta Lake Project Authors.
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
package io.delta.kernel.integration;

import org.junit.Test;

import io.delta.kernel.client.DefaultTableClient;
import io.delta.kernel.client.TableClient;
import io.delta.kernel.utils.DefaultKernelTestUtils;

/**
 * Test reading Delta lake tables end to end using the Kernel APIs and default {@link TableClient}
 * implementation ({@link DefaultTableClient})
 */
public class TestDeltaTableReads
{
    @Test
    public void tableWithoutCheckpoint()
        throws Exception
    {

    }

    @Test
    public void partitionedTableWithoutCheckpoint()
        throws Exception
    {
        DefaultKernelTestUtils.getConnectorResourceFilePath("");
    }

    @Test
    public void tableWithCheckpoint()
        throws Exception
    {

    }

    @Test
    public void partitionedTableWithCheckpoint()
        throws Exception
    {

    }

    @Test
    public void tableWithNameColumnMappingMode()
        throws Exception
    {

    }

    @Test
    public void tableWithDeletionVectors()
        throws Exception
    {

    }
}
