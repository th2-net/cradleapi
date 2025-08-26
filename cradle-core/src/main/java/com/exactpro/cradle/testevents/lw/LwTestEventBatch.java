/*
 * Copyright 2025 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.cradle.testevents.lw;

import com.exactpro.cradle.messages.StoredMessageId;
import com.exactpro.cradle.testevents.StoredTestEventId;
import com.exactpro.cradle.testevents.TestEvent;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * Interface for batches of test events
 */
public interface LwTestEventBatch extends TestEvent {
    /**
     * @return number of test events currently stored in the batch
     */
    int getTestEventsCount();

    /**
     * Returns test event stored in the batch by ID
     *
     * @param id of test event to get from batch
     * @return test event for given ID, if it is present in the batch, null otherwise
     */
    LwBatchedStoredTestEvent getTestEvent(StoredTestEventId id);

    /**
     * @return collection of test events stored in the batch
     */
    Collection<LwBatchedStoredTestEvent> getTestEvents();

    /**
     * @return collection of root test events stored in the batch
     */
    Collection<LwBatchedStoredTestEvent> getRootTestEvents();

    /**
     * @return map of event IDs stored in the batch and the corresponding message IDs
     */
    Map<StoredTestEventId, Set<StoredMessageId>> getBatchMessages(); //TODO: remove

    boolean hasChildren(StoredTestEventId parentId);

    Collection<LwBatchedStoredTestEvent> getChildren(StoredTestEventId parentId);

    Set<StoredMessageId> getMessages(StoredTestEventId eventId);
}
