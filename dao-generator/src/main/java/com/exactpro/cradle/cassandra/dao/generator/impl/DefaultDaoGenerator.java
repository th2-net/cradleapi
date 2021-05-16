package com.exactpro.cradle.cassandra.dao.generator.impl;

import java.lang.reflect.Method;
import java.util.concurrent.CompletableFuture;

import javax.lang.model.element.Modifier;

import com.datastax.oss.driver.api.mapper.MapperContext;
import com.datastax.oss.driver.api.mapper.annotations.Dao;
import com.exactpro.cradle.cassandra.dao.retry.AbstractRetryDao;
import com.squareup.javapoet.JavaFile;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.TypeSpec;
import com.squareup.javapoet.TypeSpec.Builder;

public class DefaultDaoGenerator extends AbstractJavaGenerator {
    @Override
    public JavaFile[] generate(Class<?> interfaceClass, Class<?> implClass) {
        if (interfaceClass.getAnnotation(Dao.class) == null || implClass == null) {
            throw new IllegalStateException();
        }

        Builder typeBuilder = TypeSpec.classBuilder(getGeneratedImplClass(interfaceClass.getSimpleName()))
                .superclass(AbstractRetryDao.class)
                .addSuperinterface(interfaceClass)
                .addModifiers(Modifier.PUBLIC)
                .addField(interfaceClass, "dao", Modifier.PRIVATE)
                .addMethod(MethodSpec.constructorBuilder()
                        .addParameter(MapperContext.class, "context")
                        .addParameter(interfaceClass, "dao")
                        .addModifiers(Modifier.PUBLIC)
                        .addCode("super(context);\nthis.dao = dao;")
                        .build());

        for (Method method : interfaceClass.getMethods()) {
            typeBuilder.addMethod(override(method, this::getMethodCode).build());
        }

        return new JavaFile[] { JavaFile.builder(interfaceClass.getPackage().getName(), typeBuilder.build()).build() };
    }

    private String getMethodCode(Class<?> cls) {
        return "return " + (CompletableFuture.class.isAssignableFrom(cls) ? "async" : "blocking") + "Request(\"%1$s\", () -> dao.%1$s(%2$s));";
    }
}
