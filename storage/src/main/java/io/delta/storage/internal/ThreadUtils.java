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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;

public final class ThreadUtils {

    /**
     * Based on Apache Spark's ThreadUtils.runInNewThread
     * Run a piece of code in a new thread and return the result.
     */
    public static <T> T runInNewThread(
            String threadName,
            boolean isDaemon,
            Callable<T> body) throws Throwable {
        // Using a single-element list to hold the throwable and result,
        // since values used in static method must be final
        List<Throwable> exceptionHolder = new ArrayList<>(1);
        List<T> resultHolder = new ArrayList<>(1);
        Thread thread = new Thread(threadName) {
            @Override
            public void run() {
                try {
                    resultHolder.add(body.call());
                } catch (Throwable t) {
                    exceptionHolder.add(t);
                }
            }
        };
        thread.setDaemon(isDaemon);
        thread.start();
        thread.join();

        if (!exceptionHolder.isEmpty()) {
            Throwable realException = exceptionHolder.get(0);

            // Remove the part of the stack that shows method calls into this helper method
            // This means drop everything from the top until the stack element
            // ThreadUtils.runInNewThread(), and then drop that as well (hence the `drop(1)`).
            List<StackTraceElement> baseStackTrace = new ArrayList<>();
            boolean shouldDrop = true;
            for (StackTraceElement st : Thread.currentThread().getStackTrace()) {
                if (!shouldDrop) {
                    baseStackTrace.add(st);
                } else if (st.getClassName().contains(ThreadUtils.class.getSimpleName())){
                    shouldDrop = false;
                }
            }

            // Remove the part of the new thread stack that shows methods call from this helper
            // method. This means take everything from the top until the stack element
            List<StackTraceElement> extraStackTrace = new ArrayList<>();
            for (StackTraceElement st : realException.getStackTrace()) {
                if (!st.getClassName().contains(ThreadUtils.class.getSimpleName())) {
                    extraStackTrace.add(st);
                } else {
                    break;
                }
            }

            // Combine the two stack traces, with a placeholder just specifying that there
            // was a helper method used, without any further details of the helper
            StackTraceElement placeHolderStackElem = new StackTraceElement(
                String.format( // Providing the helper class info.
                    "... run in separate thread using %s static method runInNewThread",
                    ThreadUtils.class.getSimpleName()
                ),
                " ", // method name containing the execution point, not required here.
                "", // filename containing the execution point, not required here.
                -1); // source line number also not required. -1 indicates unavailable.
            List<StackTraceElement> finalStackTrace = new ArrayList<>();
            finalStackTrace.addAll(extraStackTrace);
            finalStackTrace.add(placeHolderStackElem);
            finalStackTrace.addAll(baseStackTrace);

            // Update the stack trace and rethrow the exception in the caller thread
            realException.setStackTrace(
                finalStackTrace.toArray(new StackTraceElement[0])
            );
            throw realException;
        } else {
            return resultHolder.get(0);
        }
    }
}
