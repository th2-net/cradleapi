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
import com.exactpro.cradle.messages.StoredGroupMessageBatch
import com.exactpro.cradle.perftest.cassandra.Connector
import com.exactpro.cradle.perftest.test.MessageGroupSettings
import com.exactpro.cradle.perftest.test.Results
import com.exactpro.cradle.perftest.test.impl.GetGroupParallelTest
import com.exactpro.cradle.perftest.test.impl.GetGroupSplitIntervalTest
import com.exactpro.cradle.perftest.test.impl.GetMessageGroupTest
import com.exactpro.cradle.perftest.test.impl.StoreMessageGroupTest
import mu.KotlinLogging
import org.testcontainers.containers.BindMode
import org.testcontainers.containers.CassandraContainer
import org.testcontainers.containers.output.OutputFrame
import org.testcontainers.containers.output.OutputFrame.OutputType.END
import org.testcontainers.containers.output.OutputFrame.OutputType.STDERR
import org.testcontainers.containers.output.OutputFrame.OutputType.STDOUT
import java.nio.file.Paths
import java.time.Instant
import java.time.ZoneOffset
import java.time.ZonedDateTime
import java.time.temporal.ChronoField
import kotlin.io.path.absolutePathString
import kotlin.io.path.createDirectories
import kotlin.io.path.exists

class Main {

    companion object {
        private val LOGGER = KotlinLogging.logger { }

        private const val CASSANDRA_VERSION = "3.11.13"
        private const val CRADLE_GROUP_A = "group_a"
        private const val CRADLE_GROUP_B = "group_b"
        private const val CRADLE_GROUP_C = "group_c"
        private const val CRADLE_GROUP_D = "group_d"
        private const val CRADLE_GROUP_E = "group_e"

        private const val KEY_SPACE = "key_space"
        private const val RESOURCE_MEMORY_GB = 6L
        private const val RESOURCE_CPU = 6L

        private val LOCAL_VAR_PV = Paths.get("build", "pv", "var", "cassandra")
        private val TARGET_VAR_PV = "/var/lib/cassandra"

        @JvmStatic
        fun main(args: Array<String>) {
            LOGGER.info("Starting performance tests")
            LOCAL_VAR_PV.createDirectories()

            //Init phase
            CassandraContainer<Nothing>("cassandra:$CASSANDRA_VERSION").apply {
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
//                        withMemory(RESOURCE_MEMORY_GB * 1024 * 1024 * 1024)
//                        withCpuCount(RESOURCE_CPU)
                    }
                }
                withEnv("CASSANDRA_NUM_TOKENS", "1")
                withFileSystemBind(LOCAL_VAR_PV.absolutePathString(), TARGET_VAR_PV, BindMode.READ_WRITE)
            }.use { cassandra ->
                cassandra.start()
                try {

                    cassandra.initDBSchema()

                    try {
                        val manager = cassandra.createCradleManager(prepareStorage = false)
                        val results = Results()
                        try {
//                        println(manager.storage.getGroupedMessageBatches(
//                            CRADLE_GROUP_A,
//                            ZonedDateTime.now(ZoneOffset.UTC).withHour(0).toInstant(),
//                            ZonedDateTime.now(ZoneOffset.UTC).withHour(23).toInstant()
//                        ).asSequence()
//                            .map(StoredGroupMessageBatch::getMessageCount)
//                            .sum())

                            val groupNames =
                                listOf(CRADLE_GROUP_A, CRADLE_GROUP_B, CRADLE_GROUP_C, CRADLE_GROUP_D, CRADLE_GROUP_E)
                            val testSettings = MessageGroupSettings(startTime = 1666329437 * 1_000_000_000L)
//                            StoreMessageGroupTest(results).execute(manager.storage, testSettings, groupNames)

                            with(GetMessageGroupTest(results)) {
                                groupNames.forEach { group ->
                                    execute(manager.storage, testSettings, group)
                                }
                            }
//
//                            with(GetGroupSplitIntervalTest(results)) {
//                                groupNames.forEach { group ->
//                                    execute(manager.storage, testSettings, group)
//                                }
//                            }
//
                            with(GetGroupParallelTest(results)) {
                                execute(manager.storage, testSettings, listOf(CRADLE_GROUP_A, CRADLE_GROUP_B))
                                execute(manager.storage, testSettings, listOf(CRADLE_GROUP_C, CRADLE_GROUP_D))
                                execute(manager.storage, testSettings, listOf(CRADLE_GROUP_E, CRADLE_GROUP_A))
                                execute(manager.storage, testSettings, listOf(CRADLE_GROUP_B, CRADLE_GROUP_C))
                                execute(manager.storage, testSettings, listOf(CRADLE_GROUP_D, CRADLE_GROUP_E))
                            }
                        } finally {
                            manager.dispose()
                            LOGGER.info { results }
                        }
                    } catch (e: Exception) {
                        LOGGER.error(e) { "Test failure" }
                    }
                } finally {
                    cassandra.stop()
                }
                LOGGER.info("Finished performance tests")
            }
        }

        private fun CassandraContainer<*>.initDBSchema() {
            Connector.connect(
                host,
                firstMappedPort,
                username,
                password,
                localDatacenter
            ).use {
                it.session.execute(
                    """
                                CREATE KEYSPACE IF NOT EXISTS $KEY_SPACE WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };
                            """.trimIndent()
                )
            }
        }

        private fun CassandraContainer<*>.createCradleManager(
            instanceName: String = "cradle",
            prepareStorage: Boolean = true,
            batchSize: Long = 1_024 * 1_024
        ): CradleManager {
            val settings = CassandraConnectionSettings(
                localDatacenter,
                host,
                firstMappedPort,
                KEY_SPACE
            ).apply {
                username = this@createCradleManager.username
                password = this@createCradleManager.password
            }
            val connection = CassandraConnection(settings)
            return CassandraCradleManager(connection).apply {
                init(
                    instanceName,
                    prepareStorage,
                    batchSize,
                    batchSize
                )
            }
        }

        private val OutputFrame.message: String
            get() = utf8String.removeSuffix("\n")
    }
}