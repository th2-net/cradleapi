package com.exactpro.cradle.cassandra.dao.loader.impl;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

import com.datastax.oss.driver.api.mapper.annotations.DaoFactory;
import com.datastax.oss.driver.api.mapper.annotations.Mapper;
import com.exactpro.cradle.cassandra.dao.loader.MapperToDaoConverted;

public class DefaultMapperToDaoConverter implements MapperToDaoConverted {
    @Override
    public Map<Class<?>, Class<?>> convert(Class<?> mapper) {
        if (mapper.getAnnotation(Mapper.class) == null) {
            throw new IllegalStateException();
        }

        Map<Class<?>, Class<?>> dao = new HashMap<>();


        for (Method method : mapper.getMethods()) {
            if (method.getAnnotation(DaoFactory.class) == null) {
                continue;
            }

            Class<?> implClass = null;

            try {
                implClass = Class.forName(method.getReturnType().getTypeName() + "Impl__MapperGenerated");
            } catch (ClassNotFoundException e) {}


            dao.put(method.getReturnType(), implClass);
        }

        return dao;
    }
}
