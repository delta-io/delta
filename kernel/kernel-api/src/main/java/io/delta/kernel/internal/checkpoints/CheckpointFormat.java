package io.delta.kernel.internal.checkpoints;

public abstract class CheckpointFormat implements Comparable<CheckpointFormat> {
    public String name;
    public int ordinal;

    @Override
    public int compareTo(CheckpointFormat other) {
        return Integer.compare(ordinal, other.ordinal);
    }

    abstract public boolean usesSidecars();
}

class SingleFormat extends CheckpointFormat {
    public SingleFormat() {
        name = "Single";
        ordinal = 0;
    }

    @Override
    public boolean usesSidecars() {
        return true;
    }
}

class MultipartFormat extends CheckpointFormat {
    public MultipartFormat() {
        name = "Multipart";
        ordinal = 1;
    }

    @Override
    public boolean usesSidecars() {
        return false;
    }
}

class V2Format extends CheckpointFormat {
    public V2Format() {
        name = "V2";
        ordinal = 2;
    }

    @Override
    public boolean usesSidecars() {
        return true;
    }
}