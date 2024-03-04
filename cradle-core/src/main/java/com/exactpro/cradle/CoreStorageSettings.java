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

package com.exactpro.cradle;

import com.exactpro.cradle.messages.GroupedMessageBatchToStore;
import com.exactpro.cradle.messages.MessageBatchToStore;

@SuppressWarnings("unused")
public class CoreStorageSettings {
    public static final long DEFAULT_BOOK_REFRESH_INTERVAL_MILLIS = 60000;
    public static final int PAGE_ACTION_REJECTION_THRESHOLD_FACTOR = 2;
    public static final boolean DEFAULT_STORE_INDIVIDUAL_MESSAGE_SESSIONS = true;
    private long bookRefreshIntervalMillis = DEFAULT_BOOK_REFRESH_INTERVAL_MILLIS;

    /**
     * if `true`, the {@link CradleStorage#storeGroupedMessageBatch} method converts {@link GroupedMessageBatchToStore} to {@link MessageBatchToStore} and stores them.
     * if `false`, the {@link CradleStorage#storeGroupedMessageBatch} method stores only {@link GroupedMessageBatchToStore},
     * also the {@link CradleStorage#storeMessageBatch} and {@link CradleStorage#storeMessageBatchAsync} methods throws {@link  IllegalStateException}
     */
    private boolean storeIndividualMessageSessions = DEFAULT_STORE_INDIVIDUAL_MESSAGE_SESSIONS;

    public long getBookRefreshIntervalMillis() {
        return bookRefreshIntervalMillis;
    }

    public void setBookRefreshIntervalMillis(long bookRefreshIntervalMillis) {
        this.bookRefreshIntervalMillis = bookRefreshIntervalMillis;
    }

    public long calculatePageActionRejectionThreshold() {
        return this.bookRefreshIntervalMillis * PAGE_ACTION_REJECTION_THRESHOLD_FACTOR;
    }

    public long calculateStoreActionRejectionThreshold() {
        return this.bookRefreshIntervalMillis;
    }

    public boolean isStoreIndividualMessageSessions() {
        return storeIndividualMessageSessions;
    }

    public void setStoreIndividualMessageSessions(boolean storeIndividualMessageSessions) {
        this.storeIndividualMessageSessions = storeIndividualMessageSessions;
    }
}