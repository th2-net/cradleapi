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

package com.exactpro.cradle.cassandra;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.exactpro.cradle.cassandra.connection.CassandraConnectionSettings;
import com.exactpro.cradle.utils.CradleStorageException;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.junit.After;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;

import java.io.IOException;

public class CassandraCradleHelper {

    private static final Logger logger = LoggerFactory.getLogger(CassandraCradleHelper.class);

    public static String LOCAL_DATACENTER_NAME = "datacenter1";
    public static String KEYSPACE_NAME = "test_keyspace";
    public static int TIMEOUT = 5000;
    public static int RESULT_PAGE_SIZE = 5;
    public static int PERSISTENCE_INTERVAL = 0;

    private CqlSession session;
    private CassandraConnectionSettings connectionSettings;
    private CassandraCradleStorage storage;
    private CassandraStorageSettings storageSettings;

    private static CassandraCradleHelper instance;

    @BeforeSuite
    public static void beforeSuite() throws IOException, InterruptedException {
        instance = getInstance();
    }

    @AfterSuite
    public static void afterSuite() {
        EmbeddedCassandraServerHelper.stopEmbeddedCassandra();
    }

    private CassandraCradleHelper () throws IOException, InterruptedException {
        setUpEmbeddedCassandra();
        setUpCradle();
    }

    public static synchronized CassandraCradleHelper getInstance () throws IOException, InterruptedException {
            if (instance == null) {
                instance = new CassandraCradleHelper();
            }

            return instance;
    }

    private void setUpEmbeddedCassandra () throws IOException, InterruptedException {
        try {
            EmbeddedCassandraServerHelper.startEmbeddedCassandra();
            session = EmbeddedCassandraServerHelper.getSession();
            connectionSettings = new CassandraConnectionSettings(
                    EmbeddedCassandraServerHelper.getHost(),
                    EmbeddedCassandraServerHelper.getNativeTransportPort(),
                    LOCAL_DATACENTER_NAME);
        } catch (IOException | InterruptedException e) {
            logger.error(e.getMessage(), e);
            throw e;
        }
    }

    private void setUpCradle () {
        try {
            storageSettings = new CassandraStorageSettings(
                    TIMEOUT,
                    ConsistencyLevel.ONE,
                    ConsistencyLevel.ONE);
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

    public CassandraConnectionSettings getConnectionSettings() {
        return connectionSettings;
    }

    public CassandraCradleStorage getStorage() {
        return storage;
    }

    public CassandraStorageSettings getStorageSettings() {
        return storageSettings;
    }
}
