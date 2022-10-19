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
import com.exactpro.cradle.messages.StoredGroupMessageBatch
import com.exactpro.cradle.perftest.NANOS_IN_SEC
import com.exactpro.cradle.perftest.measure
import com.exactpro.cradle.perftest.test.MessageGroupSettings
import com.exactpro.cradle.perftest.test.PerformanceTest
import com.exactpro.cradle.perftest.toInstant
import mu.KotlinLogging

class MessageGroupGetTest : PerformanceTest() {

    override fun execute(storage: CradleStorage, settings: MessageGroupSettings) {
        val from = settings.startTime.toInstant()
        val to = (settings.startTime + settings.numberOfMessages * settings.timeShiftNanos).toInstant()

        LOGGER.info { "Execute get message group batch, batches to store ${settings.batchesToProcess}, from $from, to $to, settings $settings" }

        var respond: Iterable<StoredGroupMessageBatch>? = null
        val getIterableDuration = measure {
            var counter = 0
            while (respond == null) {
                try {
                    respond = storage.getGroupedMessageBatches(settings.groupName, from, to)
                } catch (e: Exception) {
                    LOGGER.error(e) { "Getting group message batch iterable is failure, attempt ${++counter}" }
                }
            }
        }.also { duration ->
            LOGGER.info { "Got cradle iterator in ${duration.toDouble() / NANOS_IN_SEC} sec" }
        }

        var numberOfMessage = 0
        val iterateDuration = measure {
            numberOfMessage = requireNotNull(respond)
                .sumOf(StoredGroupMessageBatch::getMessageCount)
        }.also { duration ->
            LOGGER.info { "Average SELECT ${(numberOfMessage.toDouble() / duration) * NANOS_IN_SEC}, actual number of message $numberOfMessage, duration ${duration.toDouble() / NANOS_IN_SEC}, settings $settings" }
        }

        LOGGER.info { "Average GET RESULTS ${(numberOfMessage.toDouble() / (getIterableDuration + iterateDuration)) * NANOS_IN_SEC}, actual number of message $numberOfMessage, duration ${(getIterableDuration + iterateDuration).toDouble() / NANOS_IN_SEC}, settings $settings" }
    }

    override fun close() {
    }

    companion object {
        private val LOGGER = KotlinLogging.logger { }
    }
}