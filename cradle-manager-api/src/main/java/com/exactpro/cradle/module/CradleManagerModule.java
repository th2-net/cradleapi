/*
 * Copyright 2020-2022 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.cradle.module;

import com.exactpro.cradle.config.CradleConfidentialConfiguration;
import com.exactpro.cradle.config.CradleNonConfidentialConfiguration;
import com.exactpro.th2.common.ConfigurationProvider;
import com.exactpro.th2.common.Module;
import com.exactpro.th2.common.schema.configuration.Configuration;

import java.util.Objects;

public abstract class CradleManagerModule implements Module {

    private static final String CRADLE_CONFIDENTIAL_ID = "cradle_confidential";
    private static final String CRADLE_NON_CONFIDENTIAL_ID = "cradle_non_confidential";

    private final CradleConfidentialConfiguration confidentialConfiguration;
    private final CradleNonConfidentialConfiguration nonConfidentialConfiguration;

    public CradleManagerModule(CradleConfidentialConfiguration confidentialConfiguration, CradleNonConfidentialConfiguration nonConfidentialConfiguration) {
        this.confidentialConfiguration = confidentialConfiguration;
        this.nonConfidentialConfiguration = nonConfidentialConfiguration;
    }

    @SuppressWarnings("unchecked")
    public static <C extends Configuration> C loadConfiguration(ConfigurationProvider configurationProvider, Class<C> aClass) {
        if (aClass.equals(CradleConfidentialConfiguration.class)) {
            return (C) configurationProvider.loadConfiguration(CRADLE_CONFIDENTIAL_ID, CradleConfidentialConfiguration.class);
        } else if (aClass.equals(CradleNonConfidentialConfiguration.class)) {
            return (C) configurationProvider.loadConfiguration(CRADLE_NON_CONFIDENTIAL_ID, CradleNonConfidentialConfiguration.class);
        } else {
            throw new IllegalArgumentException("CradleManager doesn't support the " + aClass + " configuration class");
        }
    }

    @SuppressWarnings("unchecked")
    public <C extends Configuration> C loadConfiguration(Class<C> aClass) {
        if (Objects.equals(aClass, CradleConfidentialConfiguration.class)) {
            return (C) confidentialConfiguration;
        } else if (Objects.equals(aClass, CradleNonConfidentialConfiguration.class)) {
            return (C) nonConfidentialConfiguration;
        } else {
            throw new IllegalArgumentException("CradleManager doesn't support the " + aClass + " configuration class");
        }
    }
}
