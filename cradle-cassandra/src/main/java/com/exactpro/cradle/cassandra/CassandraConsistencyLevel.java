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

package com.exactpro.cradle.cassandra;
import com.datastax.oss.driver.api.core.ConsistencyLevel;

public enum CassandraConsistencyLevel {

    ANY(ConsistencyLevel.ANY),
    ONE(ConsistencyLevel.ONE),
    TWO(ConsistencyLevel.TWO),
    THREE(ConsistencyLevel.THREE),
    QUORUM(ConsistencyLevel.QUORUM),
    ALL(ConsistencyLevel.ALL),
    LOCAL_ONE(ConsistencyLevel.LOCAL_ONE),
    LOCAL_QUORUM(ConsistencyLevel.LOCAL_QUORUM),
    EACH_QUORUM(ConsistencyLevel.EACH_QUORUM),
    SERIAL(ConsistencyLevel.SERIAL),
    LOCAL_SERIAL(ConsistencyLevel.LOCAL_SERIAL);


    private final ConsistencyLevel consistencyLevel;

    CassandraConsistencyLevel(ConsistencyLevel consistencyLevel) {
        this.consistencyLevel = consistencyLevel;
    }

    ConsistencyLevel getValue() {
        return this.consistencyLevel;
    }
}