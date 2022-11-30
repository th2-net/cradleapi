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

import com.exactpro.cradle.CradleManager;
import com.exactpro.th2.common.ConfigurationProvider;
import com.exactpro.th2.common.Module;
import com.exactpro.th2.common.ModuleFactory;
import com.google.auto.service.AutoService;

@AutoService(ModuleFactory.class)
public class DummyCradleManagerFactoryImpl implements ModuleFactory {
    @Override
    public Class<? extends Module> getModuleType() {
        return CradleManager.class;
    }

    @Override
    public Module create(ConfigurationProvider configurationProvider) {
        DummyCradleManager dummyCradleManager = new DummyCradleManager();
        dummyCradleManager.initConfigurationProvider(configurationProvider);
        return dummyCradleManager;
    }
}
