package io.delta.kernel.types;

import java.util.Locale;

public abstract class DataType {

    public static DataType createPrimitive(String typeName) {
        if (typeName.equals(IntegerType.INSTANCE.typeName())) return IntegerType.INSTANCE;
        if (typeName.equals(LongType.INSTANCE.typeName())) return LongType.INSTANCE;
        if (typeName.equals(StringType.INSTANCE.typeName())) return StringType.INSTANCE;
        if (typeName.equals(BooleanType.INSTANCE.typeName())) return BooleanType.INSTANCE;

        throw new IllegalArgumentException(
            String.format("Can't create primitive for type type %s", typeName)
        );
    }

    public String typeName() {
       String name = this.getClass().getSimpleName();
       if (name.endsWith("Type")) {
           name = name.substring(0, name.length() - 4);
       }
       return name.toLowerCase(Locale.ROOT);
    }
    public boolean equivalent(DataType dt) {
        return this.equals(dt);
    }

    @Override
    public String toString() {
        return typeName();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DataType that = (DataType) o;
        return typeName().equals(that.typeName());
    }
}

