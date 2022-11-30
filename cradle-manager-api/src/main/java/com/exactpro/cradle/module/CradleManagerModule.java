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

public abstract class CradleManagerModule implements Module {

    private ConfigurationProvider configurationProvider;

    private static final String CRADLE_CONFIDENTIAL_ID = "cradle_confidential";
    private static final String CRADLE_NON_CONFIDENTIAL_ID = "cradle_non_confidential";

    @SuppressWarnings("unchecked")
    public static  <C extends Configuration> C loadConfiguration(ConfigurationProvider configurationProvider, Class<C> aClass) {
        if (aClass.equals(CradleConfidentialConfiguration.class)) {
            return (C) configurationProvider.loadConfiguration(CRADLE_CONFIDENTIAL_ID, CradleConfidentialConfiguration.class);
        } else if (aClass.equals(CradleNonConfidentialConfiguration.class)) {
            return (C) configurationProvider.loadConfiguration(CRADLE_NON_CONFIDENTIAL_ID, CradleNonConfidentialConfiguration.class);
        } else {
            throw new IllegalArgumentException("CradleManager doesn't support " + aClass);
        }
    }

    public <C extends Configuration> C loadConfiguration(Class<C> aClass) {
        return loadConfiguration(configurationProvider, aClass);
    }

    public void initConfigurationProvider(ConfigurationProvider configurationProvider) {
        if (this.configurationProvider == null) {
            this.configurationProvider = configurationProvider;
        }
    }
}
