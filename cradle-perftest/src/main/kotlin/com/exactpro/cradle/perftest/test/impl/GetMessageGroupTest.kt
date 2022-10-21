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

class GetMessageGroupTest(private val results: Results) {

    fun execute(storage: CradleStorage, settings: MessageGroupSettings, group: String) {
        val from = settings.startTime.toInstant()
        val to = (settings.startTime + settings.messagesToProcess * settings.timeShiftNanos).toInstant()

        results.add(Results.Row(
            "Execute get message group batch",
            group,
            batches = settings.batchesToProcess,
            messages = settings.messagesToProcess,
            from = from,
            to = to
        ))

        var respond: Iterable<StoredGroupMessageBatch>? = null
        val getIterableDuration = measure {
            respond = RetryContainer("Getting group message batch iterable") {
                storage.getGroupedMessageBatches(group, from, to)
            }.result()
        }.also { (duration, _) ->
            results.add(Results.Row(
                "Got cradle iterator for group $group",
                duration = duration.toSec()
            ))
        }.duration

        var numberOfMessage = 0
        val iterateDuration = measure {
            numberOfMessage = requireNotNull(respond)
                .sumOf(StoredGroupMessageBatch::getMessageCount)
        }.also { (duration, _) ->
            results.add(Results.Row(
                "Average SELECT",
                group = group,
                throughput = numberOfMessage / duration.toSec(),
                duration = duration.toSec(),
                messages = numberOfMessage.toLong()
            ))
        }.duration

        results.add(Results.Row(
            "Average GET RESULTS",
            group = group,
            throughput = numberOfMessage / (getIterableDuration + iterateDuration).toSec(),
            duration = (getIterableDuration + iterateDuration).toSec(),
            messages = numberOfMessage.toLong()
        ))
    }
}