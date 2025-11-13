/*
 * Copyright 2022-2025 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.cradle.cassandra.integration.messages;

import com.exactpro.cradle.PageId;
import com.exactpro.cradle.messages.*;
import java.time.Instant;
import java.util.stream.Collectors;

public class MessageTestUtils {

    public static StoredGroupedMessageBatch groupedMessageBatchToStored (PageId pageId, Instant recDate, GroupedMessageBatchToStore batchToStore) {
        return new StoredGroupedMessageBatch (
                batchToStore.getGroup(),
                batchToStore.getMessages().stream().map(
                        el -> new StoredMessageBuilder()
                                .setBookId(el.getBookId())
                                .setSessionAlias(el.getSessionAlias())
                                .setDirection(el.getDirection())
                                .setTimestamp(el.getTimestamp())
                                .setIndex(el.getSequence())
                                .setContent(el.getContentBuffer())
                                .setProtocol(el.getProtocol())
                                .build()).collect(Collectors.toList()),
                pageId,
                recDate);
    }

    public static StoredMessageBatch messageBatchToStored (PageId pageId, Instant recDate, MessageBatchToStore batchToStore) {
        return new StoredMessageBatch(
                batchToStore.getMessages().stream().map(
                        el -> new StoredMessageBuilder()
                                .setBookId(el.getBookId())
                                .setSessionAlias(el.getSessionAlias())
                                .setDirection(el.getDirection())
                                .setTimestamp(el.getTimestamp())
                                .setIndex(el.getSequence())
                                .setContent(el.getContentBuffer())
                                .setProtocol(el.getProtocol())
                                .build()).collect(Collectors.toList()),
                pageId,
                recDate);
    }

    public static StoredMessage messageToStored(StoredMessage storedMessage, PageId pageId) {
        return new StoredMessageBuilder()
                .setBookId(storedMessage.getBookId())
                .setSessionAlias(storedMessage.getSessionAlias())
                .setDirection(storedMessage.getDirection())
                .setTimestamp(storedMessage.getTimestamp())
                .setIndex(storedMessage.getSequence())
                .setContent(storedMessage.getContentBuffer())
                .setProtocol(storedMessage.getProtocol())
                .setPageId(pageId)
                .build();
    }
}
