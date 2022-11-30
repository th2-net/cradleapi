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
import kotlin.Deprecated;

import java.util.Objects;

@Deprecated(message = "Please use CradleConfidentialConfiguration and CradleNonConfidentialConfiguration")
public class CradleConfiguration extends Configuration {
    private String dataCenter;
    private String host;
    private String keyspace;
    private Integer port;
    private String username;
    private String password;
    private String cradleInstanceName;
    private Long timeout;
    private Long pageSize;
    private Long cradleMaxEventBatchSize;
    private Long cradleMaxMessageBatchSize;
    private Boolean prepareStorage;

    public CradleConfiguration(CradleConfidentialConfiguration confidentialConfiguration,
                               CradleNonConfidentialConfiguration nonConfidentialConfiguration) {
        this(confidentialConfiguration.getDataCenter(),
                confidentialConfiguration.getHost(),
                confidentialConfiguration.getKeyspace(),
                confidentialConfiguration.getPort(),
                confidentialConfiguration.getUsername(),
                confidentialConfiguration.getPassword(),
                confidentialConfiguration.getCradleInstanceName(),
                nonConfidentialConfiguration.getTimeout(),
                nonConfidentialConfiguration.getPageSize(),
                nonConfidentialConfiguration.getCradleMaxEventBatchSize(),
                nonConfidentialConfiguration.getCradleMaxMessageBatchSize(),
                nonConfidentialConfiguration.getPrepareStorage());

    }

    public CradleConfiguration(String dataCenter, String host, String keyspace, Integer port, String username, String password, String cradleInstanceName, Long timeout, Long pageSize, Long cradleMaxEventBatchSize, Long cradleMaxMessageBatchSize, Boolean prepareStorage) {
        this.dataCenter = dataCenter;
        this.host = host;
        this.keyspace = keyspace;
        this.port = port;
        this.username = username;
        this.password = password;
        this.cradleInstanceName = cradleInstanceName;
        this.timeout = timeout;
        this.pageSize = pageSize;
        this.cradleMaxEventBatchSize = cradleMaxEventBatchSize;
        this.cradleMaxMessageBatchSize = cradleMaxMessageBatchSize;
        this.prepareStorage = prepareStorage;
    }

    public String getDataCenter() {
        return dataCenter;
    }

    public void setDataCenter(String dataCenter) {
        this.dataCenter = dataCenter;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public String getKeyspace() {
        return keyspace;
    }

    public void setKeyspace(String keyspace) {
        this.keyspace = keyspace;
    }

    public Integer getPort() {
        return port;
    }

    public void setPort(Integer port) {
        this.port = port;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getCradleInstanceName() {
        return cradleInstanceName;
    }

    public void setCradleInstanceName(String cradleInstanceName) {
        this.cradleInstanceName = cradleInstanceName;
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
        CradleConfiguration that = (CradleConfiguration) o;
        return Objects.equals(dataCenter, that.dataCenter) && Objects.equals(host, that.host) && Objects.equals(keyspace, that.keyspace) && Objects.equals(port, that.port) && Objects.equals(username, that.username) && Objects.equals(password, that.password) && Objects.equals(cradleInstanceName, that.cradleInstanceName) && Objects.equals(timeout, that.timeout) && Objects.equals(pageSize, that.pageSize) && Objects.equals(cradleMaxEventBatchSize, that.cradleMaxEventBatchSize) && Objects.equals(cradleMaxMessageBatchSize, that.cradleMaxMessageBatchSize) && Objects.equals(prepareStorage, that.prepareStorage);
    }

    @Override
    public int hashCode() {
        return Objects.hash(dataCenter, host, keyspace, port, username, password, cradleInstanceName, timeout, pageSize, cradleMaxEventBatchSize, cradleMaxMessageBatchSize, prepareStorage);
    }

    @Override
    public String toString() {
        return "CradleConfiguration{" +
                "dataCenter='" + dataCenter + '\'' +
                ", host='" + host + '\'' +
                ", keyspace='" + keyspace + '\'' +
                ", port=" + port +
                ", username='" + username + '\'' +
                ", password='" + password + '\'' +
                ", cradleInstanceName='" + cradleInstanceName + '\'' +
                ", timeout=" + timeout +
                ", pageSize=" + pageSize +
                ", cradleMaxEventBatchSize=" + cradleMaxEventBatchSize +
                ", cradleMaxMessageBatchSize=" + cradleMaxMessageBatchSize +
                ", prepareStorage=" + prepareStorage +
                '}';
    }
}
