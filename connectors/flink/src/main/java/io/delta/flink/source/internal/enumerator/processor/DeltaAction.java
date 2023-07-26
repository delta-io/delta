package io.delta.flink.source.internal.enumerator.processor;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import io.delta.standalone.actions.Action;
import io.delta.standalone.actions.AddFile;
import io.delta.standalone.actions.Metadata;
import io.delta.standalone.actions.Protocol;
import io.delta.standalone.actions.RemoveFile;

/**
 * An Enum representing Delta {@link Action} class types.
 *
 * <p>
 * This Enum can be used for example to build switch statement based on Delta Action type.
 */
enum DeltaAction {

    ADD(AddFile.class),
    REMOVE(RemoveFile.class),
    METADATA(Metadata.class),
    PROTOCOL(Protocol.class),
    OTHER(null);

    private static final Map<Class<?>, DeltaAction> LOOKUP_MAP;

    static {
        Map<Class<?>, DeltaAction> tmpMap = new HashMap<>();
        for (DeltaAction action : DeltaAction.values()) {
            tmpMap.put(action.deltaActionClass, action);
        }
        LOOKUP_MAP = Collections.unmodifiableMap(tmpMap);
    }

    private final Class<?> deltaActionClass;

    DeltaAction(Class<?> deltaActionClass) {
        this.deltaActionClass = deltaActionClass;
    }

    /**
     * @param deltaActionName A concrete implementation of {@link Action} interface that we would
     *                        like to map to {@link DeltaAction} instance.
     * @return mapped instance of {@link DeltaAction} Enum.
     */
    public static DeltaAction instanceFrom(Class<? extends Action> deltaActionName) {
        return LOOKUP_MAP.getOrDefault(deltaActionName, OTHER);
    }
}
