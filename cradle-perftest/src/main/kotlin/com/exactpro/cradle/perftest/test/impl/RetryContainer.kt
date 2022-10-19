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

import mu.KotlinLogging
import java.util.concurrent.CompletableFuture

class RetryContainer<T> (
    private val func: () -> CompletableFuture<T>
) {
    private val future = func.invoke()

    fun result(): T {
        var currentFuture = future
        var counter = 0
        while (!currentFuture.isSuccess()) {
           currentFuture = func.invoke()
            LOGGER.warn("Try to store again $this, attempt ${++counter}")
        }
        return currentFuture.get()
    }

    private fun CompletableFuture<T>.isSuccess(): Boolean {
        try {
            get()
            return true
        } catch (e: InterruptedException) {
            LOGGER.error(e) { "Async storing failure" }
            Thread.currentThread().interrupt()
        } catch (e: Exception) {
            LOGGER.error(e) { "Async storing failure" }
        }
        return false
    }

    companion object {
        private val LOGGER = KotlinLogging.logger { }
    }
}