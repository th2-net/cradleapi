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

package com.exactpro.cradle.perftest.test

import mu.KotlinLogging
import org.apache.commons.lang3.StringUtils
import java.text.DecimalFormat
import java.time.Instant
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

class Results {
    private val lock = ReentrantLock()
    private val list = mutableListOf<Row>()

    fun add(row: Row) = lock.withLock {
        list.add(row)
        LOGGER.info { row }
    }

    override fun toString(): String = lock.withLock {
        StringBuilder().appendLine("comment, group, throughput, duration, batches, messages, from, to").apply {
            list.forEach {
                appendLine(it)
            }
        }.toString()
    }

    data class Row(val comment: String, val group: String? = null, val throughput: Double = Double.NaN, val duration: Double = Double.NaN,  val batches: Long = 0L, val messages: Long = 0L, val from: Instant? = null, val to: Instant? = null) {
        override fun toString(): String = "\"$comment\"," +
                "${if(StringUtils.isBlank(group)) "" else "\"$group\""}," +
                "${if(throughput.isNaN()) "" else NUMBER_FORMAT.format(throughput)}," +
                "${if(duration.isNaN()) "" else NUMBER_FORMAT.format(duration)}," +
                "${if(batches == 0L) "" else batches}," +
                "${if(messages == 0L) "" else messages}," +
                "${from ?: ""}," +
                "${to ?: ""}"
    }

    companion object {
        private val LOGGER = KotlinLogging.logger { }
        private val NUMBER_FORMAT = DecimalFormat("############.###")
    }
}
