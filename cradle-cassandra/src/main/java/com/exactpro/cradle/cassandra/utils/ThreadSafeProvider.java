/*
 * Copyright 2024 Exactpro (Exactpro Systems Limited)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.exactpro.cradle.cassandra.utils;

import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class ThreadSafeProvider<T> {
    private final Lock lock = new ReentrantLock();
    private final Iterator<T> iterator;

    public ThreadSafeProvider(Iterator<T> iterator) {
        this.iterator = iterator;
    }

    public @Nullable T next() {
        lock.lock();
        try {
            return iterator.hasNext() ? iterator.next() : null;
        } finally {
            lock.unlock();
        }
    }
}
