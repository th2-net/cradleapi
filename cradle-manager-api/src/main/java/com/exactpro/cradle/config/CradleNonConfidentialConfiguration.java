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

package com.exactpro.cradle.config;

import com.exactpro.th2.common.schema.configuration.Configuration;

import java.util.Objects;

public class CradleNonConfidentialConfiguration extends Configuration {
    private Long timeout = 5000L;
    private Long pageSize = 5000L;
    private Long cradleMaxEventBatchSize = 1024 * 1024L;
    private Long cradleMaxMessageBatchSize = 1024 * 1024L;
    private Boolean prepareStorage = false;

    public CradleNonConfidentialConfiguration() {
    }

    public CradleNonConfidentialConfiguration(Long timeout, Long pageSize, Long cradleMaxEventBatchSize, Long cradleMaxMessageBatchSize, Boolean prepareStorage) {
        this.timeout = timeout;
        this.pageSize = pageSize;
        this.cradleMaxEventBatchSize = cradleMaxEventBatchSize;
        this.cradleMaxMessageBatchSize = cradleMaxMessageBatchSize;
        this.prepareStorage = prepareStorage;
    }

    public Long getTimeout() {
        return timeout;
    }

    public void setTimeout(Long timeout) {
        this.timeout = timeout;
    }

    public Long getPageSize() {
        return pageSize;
    }

    public void setPageSize(Long pageSize) {
        this.pageSize = pageSize;
    }

    public Long getCradleMaxEventBatchSize() {
        return cradleMaxEventBatchSize;
    }

    public void setCradleMaxEventBatchSize(Long cradleMaxEventBatchSize) {
        this.cradleMaxEventBatchSize = cradleMaxEventBatchSize;
    }

    public Long getCradleMaxMessageBatchSize() {
        return cradleMaxMessageBatchSize;
    }

    public void setCradleMaxMessageBatchSize(Long cradleMaxMessageBatchSize) {
        this.cradleMaxMessageBatchSize = cradleMaxMessageBatchSize;
    }

    public Boolean getPrepareStorage() {
        return prepareStorage;
    }

    public void setPrepareStorage(Boolean prepareStorage) {
        this.prepareStorage = prepareStorage;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CradleNonConfidentialConfiguration that = (CradleNonConfidentialConfiguration) o;
        return Objects.equals(timeout, that.timeout) && Objects.equals(pageSize, that.pageSize) && Objects.equals(cradleMaxEventBatchSize, that.cradleMaxEventBatchSize) && Objects.equals(cradleMaxMessageBatchSize, that.cradleMaxMessageBatchSize) && Objects.equals(prepareStorage, that.prepareStorage);
    }

    @Override
    public int hashCode() {
        return Objects.hash(timeout, pageSize, cradleMaxEventBatchSize, cradleMaxMessageBatchSize, prepareStorage);
    }
}
