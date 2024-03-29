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
package io.delta.kernel.internal.checkpoints;

public abstract class CheckpointFormat implements Comparable<CheckpointFormat> {
    public String name;
    public int ordinal;

    @Override
    public int compareTo(CheckpointFormat other) {
        return Integer.compare(ordinal, other.ordinal);
    }

    public abstract boolean usesSidecars();
}

class SingleFormat extends CheckpointFormat {
    SingleFormat() {
        name = "Single";
        ordinal = 0;
    }

    @Override
    public boolean usesSidecars() {
        return true;
    }
}

class MultipartFormat extends CheckpointFormat {
    MultipartFormat() {
        name = "Multipart";
        ordinal = 1;
    }

    @Override
    public boolean usesSidecars() {
        return false;
    }
}

class V2Format extends CheckpointFormat {
    V2Format() {
        name = "V2";
        ordinal = 2;
    }

    @Override
    public boolean usesSidecars() {
        return true;
    }
}
