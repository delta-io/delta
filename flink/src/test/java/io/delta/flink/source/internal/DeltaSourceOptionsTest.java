package io.delta.flink.source.internal;

import java.lang.reflect.Field;
import java.util.HashSet;
import java.util.Set;
import static java.lang.reflect.Modifier.isPublic;
import static java.lang.reflect.Modifier.isStatic;

import io.delta.flink.source.internal.builder.DeltaConfigOption;
import org.apache.flink.configuration.ConfigOption;
import org.junit.Test;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;

public class DeltaSourceOptionsTest {

    /**
     * This test checks if all ConfigOption fields from DeltaSourceOptions class were added to
     * {@link DeltaSourceOptions#USER_FACING_SOURCE_OPTIONS} or
     * {@link DeltaSourceOptions#INNER_SOURCE_OPTIONS} map.
     * <p>
     * This tests uses Java Reflection to get all static, public fields of type {@link ConfigOption}
     * from {@link DeltaSourceOptions}.
     */
    @Test
    public void testAllOptionsAreCategorized() {
        Field[] declaredFields = DeltaSourceOptions.class.getDeclaredFields();
        Set<Field> configOptionFields = new HashSet<>();
        for (Field field : declaredFields) {
            if (isPublicStatic(field) && isConfigOptionField(field)) {
                configOptionFields.add(field);
            }
        }

        assertThat(
            "Probably not all ConfigOption Fields were added to DeltaSourceOptions "
                + "VALID_SOURCE_OPTIONS or INNER_SOURCE_OPTIONS map",
            configOptionFields.size(),
            equalTo(
                DeltaSourceOptions.USER_FACING_SOURCE_OPTIONS.size()
                + DeltaSourceOptions.INNER_SOURCE_OPTIONS.size()));
    }

    private boolean isConfigOptionField(Field field) {
        return field.getType().equals(DeltaConfigOption.class);
    }

    private boolean isPublicStatic(Field field) {
        return isStatic(field.getModifiers()) && isPublic(field.getModifiers());
    }
}
