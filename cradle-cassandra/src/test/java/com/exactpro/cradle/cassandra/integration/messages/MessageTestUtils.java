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
                                .setContent(el.getContent())
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
                                .setContent(el.getContent())
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
                .setContent(storedMessage.getContent())
                .setProtocol(storedMessage.getProtocol())
                .setPageId(pageId)
                .build();
    }
}
