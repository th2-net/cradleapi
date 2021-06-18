/*
 * Copyright 2020-2021 Exactpro (Exactpro Systems Limited)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.exactpro.cradle.cassandra.dao.loader.impl;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

import com.datastax.oss.driver.api.mapper.annotations.DaoFactory;
import com.datastax.oss.driver.api.mapper.annotations.Mapper;
import com.exactpro.cradle.cassandra.dao.loader.MapperToDaoConverted;

public class DefaultMapperToDaoConverter implements MapperToDaoConverted {
    @Override
    public Map<Class<?>, Class<?>> convert(Class<?> mapper) throws ClassNotFoundException {
        if (mapper.getAnnotation(Mapper.class) == null) {
            throw new IllegalStateException();
        }

        Map<Class<?>, Class<?>> dao = new HashMap<>();


        for (Method method : mapper.getMethods()) {
            if (method.getAnnotation(DaoFactory.class) == null) {
                continue;
            }

            Class<?> implClass = Class.forName(method.getReturnType().getTypeName() + "Impl__MapperGenerated");

            dao.put(method.getReturnType(), implClass);
        }

        return dao;
    }
}
