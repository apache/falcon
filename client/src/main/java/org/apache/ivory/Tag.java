package org.apache.ivory;

import org.apache.ivory.entity.v0.EntityType;

public enum Tag {
    DEFAULT(EntityType.PROCESS), RETENTION(EntityType.FEED), REPLICATION(EntityType.FEED), LATE1(EntityType.PROCESS);

    private final EntityType entityType;

    private Tag(EntityType entityType) {
        this.entityType = entityType;
    }

    public EntityType getType() {
        return entityType;
    }
}
