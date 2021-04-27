package com.exactpro.cradle.cassandra.dao;

import java.io.File;
import java.io.IOException;
import java.util.Map.Entry;

import com.exactpro.cradle.cassandra.dao.generator.Generator;
import com.exactpro.cradle.cassandra.dao.generator.impl.DefaultDaoGenerator;
import com.exactpro.cradle.cassandra.dao.generator.impl.DefaultMapperGenerator;
import com.exactpro.cradle.cassandra.dao.loader.impl.DefaultDaoMapperLoader;
import com.exactpro.cradle.cassandra.dao.loader.impl.DefaultMapperToDaoConverter;
import com.squareup.javapoet.JavaFile;

public class Main {

    public static void main(String[] args) {
        DefaultMapperToDaoConverter converter = new DefaultMapperToDaoConverter();
        DefaultDaoGenerator daoGenerator = new DefaultDaoGenerator();
        DefaultMapperGenerator mapperGenerator = new DefaultMapperGenerator();

        File directory = new File("./cradle-cassandra/src/main/generated");

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
