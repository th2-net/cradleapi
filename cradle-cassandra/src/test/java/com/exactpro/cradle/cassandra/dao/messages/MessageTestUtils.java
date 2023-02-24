package com.exactpro.cradle.cassandra.dao.messages;

import com.exactpro.cradle.PageId;
import com.exactpro.cradle.messages.GroupedMessageBatchToStore;
import com.exactpro.cradle.messages.StoredGroupedMessageBatch;
import com.exactpro.cradle.messages.StoredMessageBuilder;

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
}
