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

package com.exactpro.cradle.perftest.test.impl

import com.exactpro.cradle.CradleStorage
import com.exactpro.cradle.Direction
import com.exactpro.cradle.messages.MessageToStoreBuilder
import com.exactpro.cradle.messages.StoredGroupMessageBatch
import com.exactpro.cradle.perftest.NANOS_IN_SEC
import com.exactpro.cradle.perftest.measure
import com.exactpro.cradle.perftest.test.MessageGroupSettings
import com.exactpro.cradle.perftest.test.PerformanceTest
import com.exactpro.cradle.perftest.toInstant
import mu.KotlinLogging
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong
import kotlin.random.Random

class MessageGroupStoreTest(
    private val parallelism: Int = 5
) : PerformanceTest(), AutoCloseable {


    override fun execute(storage: CradleStorage, settings: MessageGroupSettings) {
        measure {
            LOGGER.info {
                "Execute message group batch store, batches to store ${settings.batchesToProcess}, message to store ${settings.batchesToProcess * settings.batchSize}, settings $settings"
            }

            val queue = ArrayBlockingQueue<RetryContainer<Void>>(parallelism)
            val dryer = Drier(queue).apply { start() }

            generator(storage, settings)
                .take(settings.batchesToProcess.toInt())
                .forEach { groupMessageBatch ->
                    queue.put(RetryContainer {
                        storage.storeGroupedMessageBatchAsync(groupMessageBatch, settings.groupName)
                    })
                }

            dryer.disable()
            dryer.join()
        }.also { duration ->
            LOGGER.info { "Average INSERT ${(settings.numberOfMessages.toDouble() / duration) * NANOS_IN_SEC}, settings $settings" }
        }
    }

    override fun close() {
    }

    private class Drier(
        private val queue: ArrayBlockingQueue<RetryContainer<Void>>
    ) : Thread() {

        @Volatile
        private var enabled = true

        fun disable() {
            enabled = false
        }

        override fun run() {
            LOGGER.info { "Dryer has started" }
            var counter = 0
            try {
                while (enabled) {
                    queue.poll(1, TimeUnit.SECONDS)?.let {
                        it.result()
                        LOGGER.debug { "The ${++counter} batch has stored" }
                    } ?: let {
                        LOGGER.debug { "Wait the next item" }
                    }
                }
            } catch (e: InterruptedException) {
                LOGGER.info { "Stopping test" }
            } finally {
                queue.forEach {
                    it.result()
                    LOGGER.debug { "The ${++counter} batch has stored" }
                }
                LOGGER.info { "Drier has finished" }
            }
        }
    }

    companion object {
        private val LOGGER = KotlinLogging.logger { }
        private val RANDOM = Random(1)
        private val SEQUENCE_COUNTER = ConcurrentHashMap<StreamId, AtomicLong>()

        fun generator(storage: CradleStorage, messageGroupSettings: MessageGroupSettings): Sequence<StoredGroupMessageBatch> {
            val sessionAliases = (1..messageGroupSettings.numberOfStreams).map { "session_$it" }.toList()
            val directions = listOf(Direction.FIRST, Direction.SECOND)
            val timestampCounter = AtomicLong(messageGroupSettings.startTime)

            return generateSequence {
                storage.objectsFactory.createGroupMessageBatch().apply {
                    try {
                        for (i in 1..messageGroupSettings.batchSize) {
                            val sessionAlias = sessionAliases[RANDOM.nextInt(0, sessionAliases.size)]
                            val direction = directions[RANDOM.nextInt(0, directions.size)]

                            addMessage(MessageToStoreBuilder().apply {
                                streamName(sessionAlias)
                                direction(direction)
                                content(ByteArray(messageGroupSettings.messageSize).apply(RANDOM::nextBytes))
                                timestamp(timestampCounter.addAndGet(messageGroupSettings.timeShiftNanos).toInstant())
                                index(SEQUENCE_COUNTER.getOrPut(StreamId(sessionAlias, direction)) {
                                    AtomicLong(System.nanoTime())
                                }.incrementAndGet())
                                //TODO: add properties
                            }.build())
                        }
                    } catch (e: Exception) {
                        LOGGER.error(e) { "Current batch size = $messageCount items, $batchSize bytes" }
                    }
                }
            }
        }

        private data class StreamId(val sessionAlias: String, val direction: Direction)

    }
}