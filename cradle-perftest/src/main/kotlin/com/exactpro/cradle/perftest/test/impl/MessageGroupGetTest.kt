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
import com.exactpro.cradle.perftest.test.MessageGroupSettings
import com.exactpro.cradle.perftest.test.PerformanceTest
import mu.KotlinLogging
import java.time.Instant
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.CompletableFuture
import java.util.concurrent.atomic.AtomicLong
import kotlin.random.Random

class MessageGroupGetTest(
    parallelism: Int = 1000
) : PerformanceTest(), AutoCloseable {
    private val queue = ArrayBlockingQueue<CompletableFuture<Void>>(parallelism)
    private val dryer: Thread = Thread(::dry).apply { start() }

    override fun execute(storage: CradleStorage, settings: MessageGroupSettings) {
        val expectedBatches = settings.numberOfMessages / settings.messageSize
        LOGGER.info { "Execute message group batch store, batches to store $expectedBatches, settings $settings" }

//        storage.getGroupedMessageBatches(settings.groupName)

        var counter = 0
        generator(storage, settings)
            .take(expectedBatches.toInt())
            .forEach { groupMessageBatch ->
                queue.put(storage.storeGroupedMessageBatchAsync(groupMessageBatch, settings.groupName))
                LOGGER.debug { "Store the ${++counter} batch" }
            }
    }

    override fun close() {
        dryer.interrupt()
        dryer.join()
    }

    private fun dry() {
        LOGGER.info { "Dryer has started" }
        var counter = 0
        try {
            while (!Thread.currentThread().isInterrupted) {
                queue.take().getSilent()
                LOGGER.debug { "The ${++counter} batch is stored" }
            }
        } catch (e: InterruptedException) {
            LOGGER.info { "Stopping test" }
        } finally {
            queue.forEach {
                it.getSilent()
                LOGGER.debug { "The ${++counter} batch is stored" }
            }
            LOGGER.info { "Drier has finished" }
        }
    }

    companion object {
        private val LOGGER = KotlinLogging.logger { }
        private val RANDOM = Random(1)
        private val SEQUENCE_COUNTER = AtomicLong(System.nanoTime())

        fun generator(storage: CradleStorage, messageGroupSettings: MessageGroupSettings): Sequence<StoredGroupMessageBatch> {
            val sessionAliases = (1..messageGroupSettings.numberOfStreams).map { "session_$it" }.toList()
            val directions = listOf(Direction.FIRST, Direction.SECOND)

            return generateSequence {
                storage.objectsFactory.createGroupMessageBatch().apply {
                    addMessage(MessageToStoreBuilder().apply {
                        streamName(sessionAliases[RANDOM.nextInt(0, sessionAliases.size)])
                        content(ByteArray(messageGroupSettings.messageSize).apply(RANDOM::nextBytes))
                        timestamp(Instant.now())
                        direction(directions[RANDOM.nextInt(0, directions.size)] )
                        index(SEQUENCE_COUNTER.incrementAndGet())
                        //TODO: add properties
                    }.build())
                }
            }
        }

        private fun CompletableFuture<*>.getSilent() {
            try {
                get()
            } catch (e: RuntimeException) {
                LOGGER.error(e) { "Async storing failure" }
            }
        }
    }
}