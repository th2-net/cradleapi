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
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

public class CradleConfidentialConfiguration extends Configuration {
    @JsonProperty(required = true)
    private String dataCenter;

    @JsonProperty(required = true)
    private String host;

    @JsonProperty(required = true)
    private String keyspace;

    private Integer port = 0;

    private String username = null;

    private String password = null;

    private String cradleInstanceName = null;

    public CradleConfidentialConfiguration() {
    }

    public CradleConfidentialConfiguration(String dataCenter, String host, String keyspace, Integer port, String username, String password, String cradleInstanceName) {
        this.dataCenter = dataCenter;
        this.host = host;
        this.keyspace = keyspace;
        this.port = port;
        this.username = username;
        this.password = password;
        this.cradleInstanceName = cradleInstanceName;
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

    @Override
    public String toString() {
        return "CradleConfidentialConfiguration{" +
                "dataCenter='" + dataCenter + '\'' +
                ", host='" + host + '\'' +
                ", keyspace='" + keyspace + '\'' +
                ", port=" + port +
                ", username='" + username + '\'' +
                ", password='" + password + '\'' +
                ", cradleInstanceName='" + cradleInstanceName + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CradleConfidentialConfiguration that = (CradleConfidentialConfiguration) o;
        return Objects.equals(dataCenter, that.dataCenter) && Objects.equals(host, that.host) && Objects.equals(keyspace, that.keyspace) && Objects.equals(port, that.port) && Objects.equals(username, that.username) && Objects.equals(password, that.password) && Objects.equals(cradleInstanceName, that.cradleInstanceName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(dataCenter, host, keyspace, port, username, password, cradleInstanceName);
    }
}