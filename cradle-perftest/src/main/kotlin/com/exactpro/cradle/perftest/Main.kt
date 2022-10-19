/*
 *  Copyright 2022 Exactpro (Exactpro Systems Limited)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.exactpro.cradle.perftest

import com.exactpro.cradle.CradleManager
import com.exactpro.cradle.cassandra.CassandraCradleManager
import com.exactpro.cradle.cassandra.connection.CassandraConnection
import com.exactpro.cradle.cassandra.connection.CassandraConnectionSettings
import com.exactpro.cradle.perftest.cassandra.Connector
import com.exactpro.cradle.perftest.test.MessageGroupSettings
import com.exactpro.cradle.perftest.test.impl.MessageGroupGetTest
import com.exactpro.cradle.perftest.test.impl.MessageGroupStoreTest
import mu.KotlinLogging
import org.testcontainers.containers.CassandraContainer
import org.testcontainers.containers.output.OutputFrame
import org.testcontainers.containers.output.OutputFrame.OutputType.END
import org.testcontainers.containers.output.OutputFrame.OutputType.STDERR
import org.testcontainers.containers.output.OutputFrame.OutputType.STDOUT

class Main {

    companion object {
        private val LOGGER = KotlinLogging.logger { }
        private const val KEY_SPACE = "key_space"
        private const val RESOURCE_MEMORY_GB = 4L
        private const val RESOURCE_CPU = 4L

        @JvmStatic
        fun main(args: Array<String>) {
            LOGGER.info("Starting performance tests")

            //Init phase
            CassandraContainer<Nothing>("cassandra:3.11.13").apply {
                withLogConsumer {
                    when (it.type) {
                        STDOUT -> LOGGER.info { "Cassandra stdout log: ${it.message}" }
                        STDERR -> LOGGER.error { "Cassandra stderr log: ${it.message}" }
                        END -> LOGGER.info { "Cassandra end of log stream: ${it.message}" }
                        else -> LOGGER.info { "Cassandra log: ${it.message}" }
                    }
                }
                withCreateContainerCmdModifier { cmd ->
                    cmd.hostConfig?.apply {
                        withMemory(RESOURCE_MEMORY_GB * 1024 * 1024 * 1024)
                        withCpuCount(RESOURCE_CPU)
                    }
                }
            }.use { cassandra ->
                    cassandra.start()

                    Connector.connect(
                        cassandra.host,
                        cassandra.firstMappedPort,
                        cassandra.username,
                        cassandra.password,
                        cassandra.localDatacenter
                    ).use {
                        it.session.execute(
                            """
                        CREATE KEYSPACE IF NOT EXISTS $KEY_SPACE WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };
                    """.trimIndent()
                        )
                    }

                    try {
                        val settings = CassandraConnectionSettings(
                            cassandra.localDatacenter,
                            cassandra.host,
                            cassandra.firstMappedPort,
                            KEY_SPACE
                        ).apply {
                            username = cassandra.username
                            password = cassandra.password
                        }
                        val connection = CassandraConnection(settings)
                        val manager: CradleManager = CassandraCradleManager(connection)
                        try {
                            manager.init(
                                "cradle",
                                true,
                                (1_024 * 1_024).toLong(),
                                (1_024 * 1_024).toLong()
                            )

                            val testSettings = MessageGroupSettings()
                            MessageGroupStoreTest().use {
                                it.execute(manager.storage, testSettings)
                            }

                            MessageGroupGetTest().use {
                                for(i in 1..5) {
                                    it.execute(manager.storage, testSettings)
                                }
                            }

                        } finally {
                            manager.dispose()
                        }
                    } catch (e: Exception) {
                        LOGGER.error(e) { "Test failure" }
                    }
                    LOGGER.info("Finished performance tests")
                }
        }

        private val OutputFrame.message: String
            get() = utf8String.removeSuffix("\n")
    }
}