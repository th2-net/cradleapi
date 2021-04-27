package com.exactpro.cradle.cassandra.dao.generator.impl;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.lang.model.element.Modifier;

import com.datastax.oss.driver.api.mapper.MapperContext;
import com.datastax.oss.driver.api.mapper.annotations.Mapper;
import com.exactpro.cradle.cassandra.dao.generator.Generator;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.JavaFile;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.TypeSpec;
import com.squareup.javapoet.TypeSpec.Builder;

public class DefaultMapperGenerator extends AbstractJavaGenerator {

    @Override
    public JavaFile[] generate(Class<?> cls, Class<?> implClass) {
        Mapper mapperAnnotation = cls.getAnnotation(Mapper.class);

        if (mapperAnnotation == null) {
            throw new IllegalStateException();
        }

        Class<?> builder;

        String builderName = mapperAnnotation.builderName();
        if (builderName.isEmpty()) {
            builderName = cls.getTypeName() + "Builder";
        }

        try {
            builder = Class.forName(builderName);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            return null;
        }

        Builder mapperBuilder = TypeSpec.classBuilder(cls.getSimpleName() + "Impl__RetryGeneratedWithRetry")
                .addSuperinterface(cls)
                .addModifiers(Modifier.PUBLIC)
                .addField(cls, "delegate")
                .addField(MapperContext.class, "context")
                .addMethod(MethodSpec.constructorBuilder()
                        .addParameter(MapperContext.class, "context")
                        .addParameter(cls, "delegate")
                        .addCode("this.context = context;\nthis.delegate = delegate;")
                        .build());

        for (Method method : cls.getMethods()) {
            mapperBuilder.addMethod(override(method, returnType ->  "return new " + getGeneratedImplClass(returnType.getTypeName()) + "(context, delegate.%1$s(%2$s));").build());
        }

        ClassName defaultMapperContext = ClassName.get("com.datastax.oss.driver.internal.mapper", "DefaultMapperContext");

        Builder mapperBuilderBuilder = TypeSpec.classBuilder(cls.getSimpleName() + "BuilderWithRetry")
                .superclass(builder)
                .addModifiers(Modifier.PUBLIC)
                .addMethod(MethodSpec
                        .methodBuilder("build")
                        .returns(cls)
                        .addModifiers(Modifier.PUBLIC)
                        .addAnnotation(Override.class)
                        .addCode(defaultMapperContext.canonicalName() + " context = new " + defaultMapperContext.canonicalName() + "(session, defaultKeyspaceId, customState);\n"
                                + "return new " + cls.getTypeName() + "Impl__RetryGeneratedWithRetry(context, new CassandraDataMapperImpl__MapperGenerated(context));")
                        .build());

        for (Constructor<?> constructor : builder.getConstructors()) {
            mapperBuilderBuilder.addMethod(override(constructor, "super(%2$s);").build());
        }

        return new JavaFile[] {
                JavaFile.builder(cls.getPackageName(), mapperBuilder.build()).build(),
                JavaFile.builder(cls.getPackageName(), mapperBuilderBuilder.build()).build()
        };
    }

}
