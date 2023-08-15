/*
 * Copyright 2023 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.cradle.cassandra.workers;

import io.prometheus.client.Counter;

import javax.annotation.Nonnull;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class MetricHolder<T extends MetricHolder.LabelHolder> {

    private final ConcurrentMap<T, Counter.Child> map = new ConcurrentHashMap<>();
    private final Counter counter;

    public MetricHolder(@Nonnull Counter counter) {
        this.counter = Objects.requireNonNull(counter, "'counter' can't be null");
    }

    public void inc(T key, double value) {
        map.computeIfAbsent(key, a -> counter.labels(a.getLabels())).inc(value);
    }

    public void inc(T key) {
        inc(key, 1);
    }

    public interface LabelHolder {
        String[] getLabels();
    }
}
