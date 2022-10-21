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

class GetGroupSplitIntervalTest(private val results: Results) {

    fun execute(storage: CradleStorage, settings: MessageGroupSettings, group: String, splitFactor: Int = 5) {
        val from = settings.startTime
        val to = (settings.startTime + settings.messagesToProcess * settings.timeShiftNanos)
        val diff = to - from
        val step = diff / splitFactor

        results.add(
            Results.Row(
                "Execute get message group batch by split interval (split factor $splitFactor)",
                group,
                batches = settings.batchesToProcess,
                messages = settings.messagesToProcess,
            )
        )

        val numberOfMessage = AtomicLong(0L)
        val totalDuration = AtomicLong(0L)

        val iterableFutureList: List<CompletableFuture<Iterable<StoredGroupMessageBatch>>> =
            (0 until splitFactor).asSequence()
                .map { iteration ->
                    CompletableFuture.supplyAsync {
                        val intervalFrom = (from + (step * iteration)).toInstant()
                        val intervalTo = (from + (step * (iteration + 1))).toInstant()
                        measure {
                            RetryContainer("Getting group message batch iterable (iteration $iteration)") {
                                storage.getGroupedMessageBatches(
                                    group,
                                    intervalFrom,
                                    intervalTo,
                                )
                            }.result()
                        }.also { (duration, _) ->
                            results.add(
                                Results.Row(
                                    "Got cradle iterator (iteration $iteration)",
                                    group = group,
                                    duration = duration.toSec(),
                                    from = intervalFrom,
                                    to = intervalTo
                                )
                            )
                            totalDuration.addAndGet(duration)
                        }.result
                    }
                }.toList()

        iterableFutureList.asSequence()
            .map(CompletableFuture<Iterable<StoredGroupMessageBatch>>::get)
            .forEachIndexed { index, respond ->
                measure {
                    requireNotNull(respond)
                        .sumOf(StoredGroupMessageBatch::getMessageCount)
                }.also { (duration, sum) ->
                    results.add(
                        Results.Row(
                            "Average SELECT (iteration $index)",
                            group = group,
                            throughput = sum / duration.toSec(),
                            duration = duration.toSec(),
                            messages = sum.toLong()
                        )
                    )
                    numberOfMessage.addAndGet(sum.toLong())
                    totalDuration.addAndGet(duration)
                }
            }

        results.add(
            Results.Row(
                "Average GET RESULTS",
                group = group,
                throughput = numberOfMessage.get() / totalDuration.get().toSec(),
                duration = totalDuration.get().toSec(),
                messages = numberOfMessage.toLong()
            )
        )
    }
}