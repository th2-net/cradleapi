package com.exactpro.cradle.cassandra.dao.loader.impl;

import java.io.*;
import java.net.URL;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Scanner;

import com.datastax.oss.driver.api.mapper.annotations.Mapper;
import com.exactpro.cradle.cassandra.dao.loader.DaoMapperLoader;

public class DefaultDaoMapperLoader implements DaoMapperLoader {

    private static final String DEFAULT_PATH_FOR_RESOURCE = "META-INF/dao";

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
            return new Class[0];
        }
    }
}
