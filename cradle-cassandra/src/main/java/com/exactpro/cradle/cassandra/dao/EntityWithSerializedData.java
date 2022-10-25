package com.exactpro.cradle.cassandra.dao;

import com.exactpro.cradle.serialization.SerializedEntityData;

public class EntityWithSerializedData<T extends CradleEntity>{

    private final SerializedEntityData serializedEntityData;
    private final T entity;

    public EntityWithSerializedData(SerializedEntityData serializedEntityData, T entity) {
        this.serializedEntityData = serializedEntityData;
        this.entity = entity;
    }

    public SerializedEntityData getSerializedEntityData() {
        return serializedEntityData;
    }

    public T getEntity() {
        return entity;
    }
}
