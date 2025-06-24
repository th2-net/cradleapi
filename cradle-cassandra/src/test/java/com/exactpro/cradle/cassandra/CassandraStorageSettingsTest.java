/*
 * Copyright 2021-2022 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.cradle.cassandra;

import com.datastax.oss.driver.api.core.cql.Statement;
import com.exactpro.cradle.cassandra.connection.NetworkTopologyStrategy;
import com.exactpro.cradle.cassandra.retries.SelectExecutionPolicy;
import com.exactpro.cradle.cassandra.retries.SelectExecutionVerdict;
import com.exactpro.cradle.utils.CompressionType;
import org.assertj.core.api.Assertions;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collections;

public class CassandraStorageSettingsTest {
    @Test
    public void copy() {
        CassandraStorageSettings defaultSettings = new CassandraStorageSettings();
        defaultSettings.setBookRefreshIntervalMillis(defaultSettings.getBookRefreshIntervalMillis() + 10);
        defaultSettings.setUpdateStatusBeforeStoringEvent(!defaultSettings.isUpdateStatusBeforeStoringEvent());
        defaultSettings.setMaxUncompressedTestEventSize(defaultSettings.getMaxUncompressedTestEventSize() + 10);

        defaultSettings.setNetworkTopologyStrategy(new NetworkTopologyStrategy(Collections.singletonMap("test", 123)));
        defaultSettings.setTimeout(defaultSettings.getTimeout() + 10);
        defaultSettings.setWriteConsistencyLevel(Arrays.stream(CassandraConsistencyLevel.values()).filter(item -> item != defaultSettings.getWriteConsistencyLevel()).findFirst().orElseThrow());
        defaultSettings.setReadConsistencyLevel(Arrays.stream(CassandraConsistencyLevel.values()).filter(item -> item != defaultSettings.getReadConsistencyLevel()).findFirst().orElseThrow());
        defaultSettings.setKeyspace(defaultSettings.getKeyspace() + 10);
        defaultSettings.setKeyspaceReplicationFactor(defaultSettings.getKeyspaceReplicationFactor() + 10);
        defaultSettings.setMaxParallelQueries(defaultSettings.getMaxParallelQueries() + 10);
        defaultSettings.setResultPageSize(defaultSettings.getResultPageSize() + 10);
        defaultSettings.setMaxMessageBatchSize(defaultSettings.getMaxMessageBatchSize() + 10);
        defaultSettings.setMaxUncompressedMessageBatchSize(defaultSettings.getMaxUncompressedMessageBatchSize() + 10);
        defaultSettings.setMaxTestEventBatchSize(defaultSettings.getMaxTestEventBatchSize() + 10);
        defaultSettings.setMaxUncompressedTestEventSize(defaultSettings.getMaxUncompressedTestEventSize() + 10);
        defaultSettings.setSessionsCacheSize(defaultSettings.getSessionsCacheSize() + 10);
        defaultSettings.setPageSessionsCacheSize(defaultSettings.getPageSessionsCacheSize() + 10);
        defaultSettings.setSessionStatisticsCacheSize(defaultSettings.getSessionStatisticsCacheSize() + 10);
        defaultSettings.setScopesCacheSize(defaultSettings.getScopesCacheSize() + 10);
        defaultSettings.setPageScopesCacheSize(defaultSettings.getPageScopesCacheSize() + 10);
        defaultSettings.setPageGroupsCacheSize(defaultSettings.getPageGroupsCacheSize() + 10);
        defaultSettings.setGroupsCacheSize(defaultSettings.getGroupsCacheSize() + 10);
        defaultSettings.setCounterPersistenceInterval(defaultSettings.getCounterPersistenceInterval() + 10);
        defaultSettings.setCounterPersistenceMaxParallelQueries(defaultSettings.getCounterPersistenceMaxParallelQueries() + 10);
        defaultSettings.setComposingServiceThreads(defaultSettings.getComposingServiceThreads() + 10);
        defaultSettings.setMultiRowResultExecutionPolicy(new TestSelectExecutionPolicy());
        defaultSettings.setSingleRowResultExecutionPolicy(new TestSelectExecutionPolicy());
        defaultSettings.setEventBatchDurationMillis(defaultSettings.getEventBatchDurationMillis() + 10);
        defaultSettings.setEventBatchDurationCacheSize(defaultSettings.getEventBatchDurationCacheSize() + 10);
        defaultSettings.setCompressionType(Arrays.stream(CompressionType.values()).filter(item -> item != defaultSettings.getCompressionType()).findFirst().orElseThrow());
        defaultSettings.setInitOperatorsDurationSeconds(defaultSettings.getInitOperatorsDurationSeconds() + 10);

        CassandraStorageSettings copy = new CassandraStorageSettings(defaultSettings);
        Assertions.assertThat(copy)
                .usingRecursiveComparison()
                .isEqualTo(defaultSettings);
    }

    private static class TestSelectExecutionPolicy implements SelectExecutionPolicy {
        @Override
        public SelectExecutionVerdict onError(Statement<?> statement, String queryInfo, Throwable cause, int retryCount) {
            return null;
        }

        @Override
        public SelectExecutionVerdict onNextPage(Statement<?> statement, String queryInfo) {
            return null;
        }
    }
}
