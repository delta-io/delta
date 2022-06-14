package io.delta.flink.utils;

public enum FailoverType {

    /**
     * Indicates that no failover should take place.
     */
    NONE,

    /**
     * Indicates that failover was caused by Task Manager failure
     */
    TASK_MANAGER,

    /**
     * Indicates that failover was caused by Job Manager failure
     */
    JOB_MANAGER
}
