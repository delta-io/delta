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
package io.delta.kernel.internal.replay;

import java.util.*;

import static org.junit.Assert.*;
import org.junit.Test;

import io.delta.kernel.internal.fs.Path;
import io.delta.kernel.internal.utils.FileStatus;

public class TestLogReplay {

    @Test
    public void testAssertLogFilesBelongToTable() {
        // Create a test instance of LogReplay
        LogReplay logReplay = new LogReplay(new Path("logPath"), new Path("dataPath"), null, null);

        // Create a list of FileStatus objects representing log files
        List<FileStatus> logFiles = new ArrayList<>();
        logFiles.add(new FileStatus("logPath/logfile1"));
        logFiles.add(new FileStatus("logPath/logfile2"));
        logFiles.add(new FileStatus("logPath/logfile3"));

        // Test that files with the correct log path pass the assertion
        logReplay.assertLogFilesBelongToTable(new Path("logPath"), logFiles);

        // Create a list of FileStatus objects with incorrect log paths
        List<FileStatus> invalidLogFiles = new ArrayList<>();
        invalidLogFiles.add(new FileStatus("invalidPath/logfile1"));
        invalidLogFiles.add(new FileStatus("invalidPath/logfile2"));

        // Test that files with incorrect log paths trigger the assertion
        try {
            logReplay.assertLogFilesBelongToTable(new Path("logPath"), invalidLogFiles);
            fail("Assertion did not throw an exception for invalid log paths.");
        } catch (RuntimeException e) {
            // Expected behavior
        }
    }
}
