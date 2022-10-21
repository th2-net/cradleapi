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
import com.exactpro.cradle.perftest.measure
import com.exactpro.cradle.perftest.test.MessageGroupSettings
import com.exactpro.cradle.perftest.test.Results
import com.exactpro.cradle.perftest.toInstant
import com.exactpro.cradle.perftest.toSec
import java.util.concurrent.CompletableFuture
import java.util.concurrent.atomic.AtomicLong

class GetGroupParallelTest(private val results: Results) {

    fun execute(storage: CradleStorage, settings: MessageGroupSettings, groupNames: List<String>) {
        val from = settings.startTime.toInstant()
        val to = (settings.startTime + settings.messagesToProcess * settings.timeShiftNanos / 2).toInstant()

        results.add(
            Results.Row(
                "Execute get message group batch by parallel group request",
                group = groupNames.toString(),
                batches = settings.batchesToProcess,
                messages = settings.messagesToProcess,
                from = from,
                to = to
            )
        )

        val numberOfMessage = AtomicLong(0L)
//        val totalDuration = AtomicLong(0L)

        measure {
            val iterableFutureList: List<CompletableFuture<Iterable<StoredGroupMessageBatch>>> =
                groupNames.asSequence()
                    .map { group ->
                        CompletableFuture.supplyAsync {
                            measure {
                                RetryContainer("Getting group message batch iterable (group $group)") {
                                    storage.getGroupedMessageBatches(
                                        group,
                                        from,
                                        to,
                                    )
                                }.result()
                            }.also { (duration, _) ->
                                results.add(
                                    Results.Row(
                                        "Got cradle iterator for group $group",
                                        duration = duration.toSec()
                                    )
                                )
//                                totalDuration.addAndGet(duration)
                            }.result
                        }
                    }.toList()

            iterableFutureList.asSequence()
                .map(CompletableFuture<Iterable<StoredGroupMessageBatch>>::get)
                .mapIndexed { index, respond ->
                    CompletableFuture.supplyAsync {
                        measure {
                            requireNotNull(respond)
                                .sumOf(StoredGroupMessageBatch::getMessageCount)
                        }.also { (duration, sum) ->
                            results.add(
                                Results.Row(
                                    "Average SELECT (iteration $index)",
                                    throughput = sum / duration.toSec(),
                                    duration = duration.toSec(),
                                    messages = numberOfMessage.toLong()
                                )
                            )
                            numberOfMessage.addAndGet(sum.toLong())
//                            totalDuration.addAndGet(duration)
                        }
                    }
                }.forEach(CompletableFuture<*>::get)
        }.also { (duration, _) ->
            results.add(
                Results.Row(
                    "Average GET RESULTS",
                    throughput = numberOfMessage.get() / duration.toSec(),
                    duration = duration.toSec(),
                    messages = numberOfMessage.toLong()
                )
            )
        }

    }
}