package org.apache.ivory.service;

import org.apache.ivory.entity.v0.Entity;

public interface ConfigurationChangeListener {

    void onAdd(Entity entity);

    void onRemove(Entity entity);

    void onChange(Entity oldEntity, Entity newEntity);
}
