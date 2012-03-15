package org.apache.ivory.service;

import org.apache.ivory.IvoryException;
import org.apache.ivory.entity.v0.Entity;

public interface ConfigurationChangeListener {

    void onAdd(Entity entity) throws IvoryException;

    void onRemove(Entity entity) throws IvoryException;

    void onChange(Entity oldEntity, Entity newEntity) throws IvoryException;
}
