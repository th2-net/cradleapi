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

import java.io.*;
import java.net.URL;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Scanner;

import com.datastax.oss.driver.api.mapper.annotations.Mapper;
import com.exactpro.cradle.cassandra.dao.loader.DaoMapperLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultDaoMapperLoader implements DaoMapperLoader {

    private static final String DEFAULT_PATH_FOR_RESOURCE = "META-INF/dao";
    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultDaoMapperLoader.class.getName());

    @Override
    public Class<?>[] loadAllDaoMapper() {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        try {
            Enumeration<URL> resources = classLoader.getResources(DEFAULT_PATH_FOR_RESOURCE);

            List<Class<?>> list = new ArrayList<>();
            while (resources.hasMoreElements()) {
                URL url = resources.nextElement();

                try (InputStream inputStream = url.openStream()) {
                    try (BufferedInputStream bufferedInputStream = new BufferedInputStream(inputStream)) {
                        try (Scanner sc = new Scanner(bufferedInputStream)) {
                            while (sc.hasNextLine()) {
                                try {
                                    Class<?> classCandidate = classLoader.loadClass(sc.nextLine());
                                    if (classCandidate.getAnnotation(Mapper.class) != null) {
                                        list.add(classCandidate);
                                    }
                                } catch (ClassNotFoundException e) {
                                    e.printStackTrace();
                                }
                            }
                        }
                    }
                }
            }

            return list.toArray(new Class[0]);
        } catch (IOException e) {
            LOGGER.error(e.getMessage());
            return new Class[0];
        }
    }
}
