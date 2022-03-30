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

package io.delta.storage.internal;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;

public final class ThreadUtils {

    /**
     * Based out of sparks ThreadUtils.runInNewThread
     * Run a piece of code in a new thread and return the result.
     * RuntimeException is thrown to avoid the calling interfaces
     * from handling any Exceptions other than IOExceptions
     */
    public static <T> T runInNewThread(
            String threadName,
            boolean isDaemon,
            Callable<T> body) throws IOException {
        //Using a single element list to hold the exception and result,
        //since T exception or T result cannot be used in static method
        List<Exception> exceptionHolder = new ArrayList<>(1);
        List<T> resultHolder = new ArrayList<>(1);
        Thread thread = new Thread(threadName) {
            @Override
            public void run() {
                try {
                    resultHolder.add(body.call());
                } catch (Exception ex) {
                    exceptionHolder.add(ex);
                }
            }
        };
        thread.setDaemon(isDaemon);
        thread.start();
        try {
            thread.join();
        } catch (InterruptedException e) {
            throw new InterruptedIOException(e.getMessage());
        }
        if (!exceptionHolder.isEmpty()) {
            Exception realException = exceptionHolder.get(0);
            // Remove the part of the stack that shows method calls into this helper method
            // This means drop everything from the top until the stack element
            // ThreadUtils.runInNewThread(), and then drop that as well (hence the + 1 to start index).
            List<StackTraceElement> currentThreadStackTrace =
                    Arrays.asList(Thread.currentThread().getStackTrace());
            //index of the stack element pointing to ThreadUtils.runInNewThread()
            int startIndex = currentThreadStackTrace.stream()
                    .filter(e -> e.getClassName().contains(ThreadUtils.class.getSimpleName()))
                    .map(e -> currentThreadStackTrace.indexOf(e))
                    .findFirst()
                    .orElse(-1);
            int endIndex = currentThreadStackTrace.size();
            List<StackTraceElement> baseStackTrace;
            if (startIndex == -1 || startIndex >= endIndex) {
                baseStackTrace = Collections.emptyList();
            } else {
                //startIndex + 1 to drop the stack element ThreadUtils.runInNewThread()
                baseStackTrace =
                        currentThreadStackTrace.subList(startIndex + 1, currentThreadStackTrace.size());
            }
            // Remove the part of the new thread stack that shows methods call from this helper method
            // This means take everything from the top until the stack element
            StackTraceElement[] realExceptionStackTrace = realException.getStackTrace();
            List<StackTraceElement> extraStackTrace = new ArrayList<>();
            for (StackTraceElement st : realExceptionStackTrace) {
                if (!st.getClassName().contains(ThreadUtils.class.getSimpleName())) {
                    extraStackTrace.add(st);
                }
            }
            // Combine the two stack traces, with a place holder just specifying that there
            // was a helper method used, without any further details of the helper
            List<StackTraceElement> placeHolderStackElem = Arrays.asList(
                    new StackTraceElement(
                            String.format(
                                    "... run in separate thread using %s",
                                    ThreadUtils.class.getSimpleName(),
                                    " static method runInNewThread"), //Providing the helper class info.
                            "", //method name containing the execution point, not required here.
                            "",   //filename containing the execution point, not required here.
                            -1) //source line number also not required. -1 indicates unavailable.
            );

            List<StackTraceElement> finalStackTrace = new ArrayList<>();
            finalStackTrace.addAll(extraStackTrace);
            finalStackTrace.addAll(placeHolderStackElem);
            finalStackTrace.addAll(baseStackTrace);

            realException.setStackTrace(finalStackTrace.toArray(new StackTraceElement[0]));
            if (realException instanceof org.apache.hadoop.fs.FileAlreadyExistsException) {
                throw (org.apache.hadoop.fs.FileAlreadyExistsException) realException;
            } else if (realException instanceof IOException) {
                throw (IOException) realException;
            } else {
                //Throwing RuntimeException to avoid the calling interfaces from throwing Exception
                throw new RuntimeException(realException);
            }
        } else {
            return resultHolder.get(0);
        }
    }
}
