package com.exactpro.cradle.cassandra.dao.loader;

import java.util.Map;

public interface MapperToDaoConverted {

    Map<Class<?>, Class<?>> convert(Class<?> mapper);

}
