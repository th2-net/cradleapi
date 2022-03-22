/*
 * Copyright 2020-2022 Exactpro (Exactpro Systems Limited)
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
package com.exactpro.cradle;

public class Counter {
    private final long entityCount;
    private final long entitySize;

    public Counter(long entityCount, long entitySize) {
        this.entityCount = entityCount;
        this.entitySize = entitySize;
    }

    public long getEntityCount() {
        return entityCount;
    }

    public long getEntitySize() {
        return entitySize;
    }

    public Counter incrementedBy(Counter value) {
        return new Counter(
                this.getEntityCount() + value.getEntityCount(),
                this.getEntitySize() + value.getEntitySize());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Counter)) return false;

        Counter counter = (Counter) o;

        if (getEntityCount() != counter.getEntityCount()) return false;
        return getEntitySize() == counter.getEntitySize();
    }

    @Override
    public int hashCode() {
        int result = (int) (getEntityCount() ^ (getEntityCount() >>> 32));
        result = 31 * result + (int) (getEntitySize() ^ (getEntitySize() >>> 32));
        return result;
    }
}