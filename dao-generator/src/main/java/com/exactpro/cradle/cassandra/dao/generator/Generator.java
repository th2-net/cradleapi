package com.exactpro.cradle.cassandra.dao.generator;

import com.squareup.javapoet.JavaFile;

public interface Generator {

    JavaFile[] generate(Class<?> interfaceClass, Class<?> implClass);

}
