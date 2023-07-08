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
package io.delta.kernel.client;

import static io.delta.kernel.utils.DefaultKernelTestUtils.getTestResourceFilePath;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import static org.junit.Assert.assertEquals;
import org.junit.Test;

import io.delta.kernel.fs.FileStatus;
import io.delta.kernel.utils.CloseableIterator;

public class TestDefaultFileSystemClient
{
    @Test
    public void listFrom() throws Exception
    {
        String basePath = getTestResourceFilePath("json-files");
        String listFrom = getTestResourceFilePath("json-files/2.json");

        List<String> actListOutput = new ArrayList<>();
        try (CloseableIterator<FileStatus> files = fsClient().listFrom(listFrom)) {
            while (files.hasNext()) {
                actListOutput.add(files.next().getPath());
            }
        }

        List<String> expListOutput = Arrays.asList(basePath + "/2.json", basePath + "/3.json");

        assertEquals(expListOutput, actListOutput);
    }

    private static DefaultFileSystemClient fsClient()
    {
        return new DefaultFileSystemClient(new Configuration());
    }
}
