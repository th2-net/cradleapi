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

package com.exactpro.cradle.cassandra.dao.generator.impl;

import java.lang.reflect.Constructor;
import java.lang.reflect.Executable;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import javax.lang.model.element.Modifier;

import com.exactpro.cradle.cassandra.dao.generator.Generator;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.JavaFile;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.TypeSpec;

public abstract class AbstractJavaGenerator implements Generator {

    protected String getCassandraImplClass(String name) {
        return name + "Impl__MapperGenerated";
    }

    protected String getGeneratedImplClass(String name) {
        return name + "Impl__RetryGenerated";
    }

    protected JavaFile buildFile(String packageName, TypeSpec typeSpec, List<ClassName> imports) {
        JavaFile.Builder file = JavaFile.builder(packageName, typeSpec);
        for (ClassName anImport : imports) {
            file.addStaticImport(anImport);
        }
        return file.build();
    }

    protected MethodSpec.Builder override(Method method, Function<Class<?>, String> methodCode) {
        MethodSpec.Builder methodBuilder = MethodSpec
                .methodBuilder(method.getName())
                .addAnnotation(Override.class)
                .returns(method.getGenericReturnType());

        List<String> arguments = buildSpec(method, methodBuilder);

        return methodBuilder.addCode(String.format(methodCode.apply(method.getReturnType()), arguments.toArray()));
    }

    protected MethodSpec.Builder override(Constructor<?> constructor, String code) {
        MethodSpec.Builder constructorBuilder = MethodSpec
                .constructorBuilder();
        List<String> arguments = buildSpec(constructor, constructorBuilder);
        return constructorBuilder.addCode(String.format(code, arguments.toArray()));
    }

    private List<String> buildSpec(Executable method, MethodSpec.Builder methodBuilder) {
        if (java.lang.reflect.Modifier.isPublic(method.getModifiers())) {
            methodBuilder.addModifiers(Modifier.PUBLIC);
        } else if (java.lang.reflect.Modifier.isProtected(method.getModifiers())) {
            methodBuilder.addModifiers(Modifier.PROTECTED);
        } else if (java.lang.reflect.Modifier.isPrivate(method.getModifiers())) {
            methodBuilder.addModifiers(Modifier.PRIVATE);
        }

        List<String> arguments = new ArrayList<>();
        arguments.add(method.getName());

        StringBuilder parameters = new StringBuilder();

        for (Parameter parameter : method.getParameters()) {

            methodBuilder.addParameter(parameter.getParameterizedType(), parameter.getName());

            if (parameter.isVarArgs()) {
                methodBuilder.varargs(true);
            }

            parameters.append(parameter.getName()).append(',');

            arguments.add(parameter.getName());
        }

        if (parameters.length() > 0) {
            if (parameters.charAt(parameters.length() - 1) == ',') {
                parameters.deleteCharAt(parameters.length() - 1);
            }
        }

        arguments.add(1, parameters.toString());

        return arguments;
    }

}
