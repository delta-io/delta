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

import io.delta.storage.GCSLogStore;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.stream.Stream;

public class ThreadUtils {

    /**
     * Converted sparks ThreadUtils.runInNewThread to java here
     * Run a piece of code in a new thread and return the result. Exception in the new thread is
     * thrown in the caller thread with an adjusted stack trace that removes references to this
     * method for clarity. The exception stack traces will be like the following
     *
     * SomeException: exception-message
     *   at CallerClass.body-method (sourcefile.java)
     *   at ... run in separate thread using org.apache.spark.util.ThreadUtils ... ()
     *   at CallerClass.caller-method (sourcefile.java)
     *   ...
     */
    public static <T> T runInNewThread(
            String threadName,
            boolean isDaemon,
            Callable<T> body) throws Exception {
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
        if (!exceptionHolder.isEmpty()) {
            Exception realException = exceptionHolder.get(0);
            // Remove the part of the stack that shows method calls into this helper method
            // This means drop everything from the top until the stack element
            // ThreadUtils.runInNewThread(), and then drop that as well (hence the `drop(1)`).
            Stream<StackTraceElement> baseStackTrace = Arrays.stream(
                    Thread.currentThread().getStackTrace()).dropWhile(t ->
                    !t.getClassName().contains(GCSLogStore.class.getSimpleName())).skip(1);
            // Remove the part of the new thread stack that shows methods call from this helper method
            Stream<StackTraceElement> extraStackTrace = Arrays.stream(
                    realException.getStackTrace()).takeWhile(e ->
                    !e.getClassName().contains(GCSLogStore.class.getSimpleName()));
            // Combine the two stack traces, with a place holder just specifying that there
            // was a helper method used, without any further details of the helper
            Stream<StackTraceElement> placeHolderStackElem = Stream.of(
                    new StackTraceElement(
                            String.format(
                                    "... run in separate thread using $s",
                                    GCSLogStore.class.getSimpleName(),
                                    " static method runInNewThread"),
                            "",
                            "",
                            -1)
            );
            StackTraceElement[] finalStackTrace = Stream.concat(
                    Stream.concat(baseStackTrace,
                            placeHolderStackElem),
                    baseStackTrace).toArray(StackTraceElement[]::new);
            realException.setStackTrace(finalStackTrace);
            throw realException;
        } else {
            return resultHolder.get(0);
        }
    }
}
