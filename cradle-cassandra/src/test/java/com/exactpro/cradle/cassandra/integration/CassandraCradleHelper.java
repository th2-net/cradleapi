/*
 * Copyright 2020-2023 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.cradle.cassandra.integration;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.exactpro.cradle.cassandra.CassandraConsistencyLevel;
import com.exactpro.cradle.cassandra.CassandraCradleManager;
import com.exactpro.cradle.cassandra.CassandraCradleStorage;
import com.exactpro.cradle.cassandra.CassandraStorageSettings;
import com.exactpro.cradle.cassandra.connection.CassandraConnectionSettings;
import com.exactpro.cradle.utils.CradleStorageException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.CassandraContainer;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;

import java.io.IOException;

public class CassandraCradleHelper {

    private static final Logger logger = LoggerFactory.getLogger(CassandraCradleHelper.class);

    private static String CASSANDRA_IMAGE = "cassandra:3.11.13";
    public static String LOCAL_DATACENTER_NAME = "datacenter1";
    public static String KEYSPACE_NAME = "test_keyspace";
    public static int TIMEOUT = 5000;
    public static int RESULT_PAGE_SIZE = 5;
    public static int PERSISTENCE_INTERVAL = 0;

    private CqlSession session;
    private CassandraConnectionSettings connectionSettings;
    private CassandraCradleStorage storage;
    private CassandraStorageSettings storageSettings;

    private static CassandraContainer<?> cassandra;

    private static CassandraCradleHelper instance;

    @BeforeSuite
    public static void beforeSuite(){
        instance = getInstance();
    }

    @AfterSuite
    public static void afterSuite() {
        cassandra.stop();
    }

    private CassandraCradleHelper() {
        cassandra = new CassandraContainer<>(CASSANDRA_IMAGE);
        setUpEmbeddedCassandra();
        setUpCradle();
    }

    public static synchronized CassandraCradleHelper getInstance() {
        if (instance == null) {
            instance = new CassandraCradleHelper();
        }

        return instance;
    }

    private void setUpEmbeddedCassandra() {
        try {
            cassandra.start();
            session = Connector.connect(cassandra).getSession();
            connectionSettings = new CassandraConnectionSettings(
                    cassandra.getHost(),
                    cassandra.getFirstMappedPort(),
                    LOCAL_DATACENTER_NAME);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            throw e;
        }
    }

    private void setUpCradle() {
        try {
            storageSettings = new CassandraStorageSettings(
                    TIMEOUT,
                    CassandraConsistencyLevel.ONE,
                    CassandraConsistencyLevel.ONE);
            storageSettings.setResultPageSize(RESULT_PAGE_SIZE);
            storageSettings.setKeyspace(KEYSPACE_NAME);
            storageSettings.setCounterPersistenceInterval(PERSISTENCE_INTERVAL);

            CassandraCradleManager manager = new CassandraCradleManager(connectionSettings, storageSettings, true);
            storage = (CassandraCradleStorage) manager.getStorage();
        } catch (CradleStorageException | IOException e) {
            logger.error(e.getMessage(), e);
        }
    }

    public CqlSession getSession() {
        return session;
    }

    public CassandraCradleStorage getStorage() {
        return storage;
    }

    public CassandraStorageSettings getStorageSettings() {
        return storageSettings;
    }
}
