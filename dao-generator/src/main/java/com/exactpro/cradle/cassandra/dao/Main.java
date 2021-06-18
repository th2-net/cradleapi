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

package com.exactpro.cradle.cassandra.dao;

import java.io.File;
import java.io.IOException;
import java.util.Map.Entry;

import com.exactpro.cradle.cassandra.dao.generator.impl.DefaultDaoGenerator;
import com.exactpro.cradle.cassandra.dao.generator.impl.DefaultMapperGenerator;
import com.exactpro.cradle.cassandra.dao.loader.impl.DefaultDaoMapperLoader;
import com.exactpro.cradle.cassandra.dao.loader.impl.DefaultMapperToDaoConverter;
import com.squareup.javapoet.JavaFile;

public class Main {
    public static void main(String[] args) throws ClassNotFoundException {
        DefaultMapperToDaoConverter converter = new DefaultMapperToDaoConverter();
        DefaultDaoGenerator daoGenerator = new DefaultDaoGenerator();
        DefaultMapperGenerator mapperGenerator = new DefaultMapperGenerator();

        File directory = new File("../cradle-cassandra/src/main/generated");

        for (Class<?> mapper : new DefaultDaoMapperLoader().loadAllDaoMapper()) {
            for (Entry<Class<?>, Class<?>> entry : converter.convert(mapper).entrySet()) {
                writeTo(daoGenerator.generate(entry.getKey(), entry.getValue()), directory);
            }
            writeTo(mapperGenerator.generate(mapper, null), directory);
        }
    }

    private static void writeTo(JavaFile[] files, File directory) {
        if (files == null) {
            return;
        }

        for (JavaFile file : files) {

            try {
                file.writeTo(directory);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

}
