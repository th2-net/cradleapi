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

data class MessageGroupSettings(
    private val numberOfMessages: Long = 1_000_000,
    val numberOfStreams: Int = 1,
    val messageSize: Int = 256,
    val batchSize: Int = 1024 * 1024 / (messageSize + 64),
    val timeShiftNanos: Long = 1_000_000_000L,
    val startTime: Long = System.currentTimeMillis() * 1_000_000L
) {
    val batchesToProcess = numberOfMessages / messageSize
    val messagesToProcess = batchesToProcess * messageSize
}
