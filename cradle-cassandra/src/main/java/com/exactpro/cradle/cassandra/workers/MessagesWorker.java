/*
 * Copyright 2021-2023 Exactpro (Exactpro Systems Limited)
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

import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.core.cql.Row;
import com.exactpro.cradle.BookId;
import com.exactpro.cradle.BookInfo;
import com.exactpro.cradle.Direction;
import com.exactpro.cradle.PageId;
import com.exactpro.cradle.PageInfo;
import com.exactpro.cradle.SessionRecordType;
import com.exactpro.cradle.cassandra.counters.MessageStatisticsCollector;
import com.exactpro.cradle.cassandra.counters.SessionStatisticsCollector;
import com.exactpro.cradle.cassandra.dao.CassandraOperators;
import com.exactpro.cradle.cassandra.dao.cache.CachedPageSession;
import com.exactpro.cradle.cassandra.dao.cache.CachedSession;
import com.exactpro.cradle.cassandra.dao.messages.GroupEntity;
import com.exactpro.cradle.cassandra.dao.messages.GroupedMessageBatchEntity;
import com.exactpro.cradle.cassandra.dao.messages.GroupedMessageBatchOperator;
import com.exactpro.cradle.cassandra.dao.messages.GroupedMessageIteratorProvider;
import com.exactpro.cradle.cassandra.dao.messages.MessageBatchEntity;
import com.exactpro.cradle.cassandra.dao.messages.MessageBatchOperator;
import com.exactpro.cradle.cassandra.dao.messages.MessageBatchesIteratorProvider;
import com.exactpro.cradle.cassandra.dao.messages.MessagesIteratorProvider;
import com.exactpro.cradle.cassandra.dao.messages.PageGroupEntity;
import com.exactpro.cradle.cassandra.dao.messages.PageSessionEntity;
import com.exactpro.cradle.cassandra.dao.messages.SessionEntity;
import com.exactpro.cradle.cassandra.dao.messages.converters.MessageBatchEntityConverter;
import com.exactpro.cradle.cassandra.resultset.CassandraCradleResultSet;
import com.exactpro.cradle.cassandra.utils.GroupedMessageEntityUtils;
import com.exactpro.cradle.cassandra.utils.MessageBatchEntityUtils;
import com.exactpro.cradle.messages.GroupedMessageBatchToStore;
import com.exactpro.cradle.messages.GroupedMessageFilter;
import com.exactpro.cradle.messages.MessageBatchToStore;
import com.exactpro.cradle.messages.MessageFilter;
import com.exactpro.cradle.messages.StoredGroupedMessageBatch;
import com.exactpro.cradle.messages.StoredMessage;
import com.exactpro.cradle.messages.StoredMessageBatch;
import com.exactpro.cradle.messages.StoredMessageId;
import com.exactpro.cradle.resultset.CradleResultSet;
import com.exactpro.cradle.serialization.SerializedMessageMetadata;
import com.exactpro.cradle.utils.CompressException;
import com.exactpro.cradle.utils.CradleStorageException;
import com.exactpro.cradle.utils.TimeUtils;
import io.prometheus.client.Counter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.zip.DataFormatException;

import static com.exactpro.cradle.CradleStorage.EMPTY_MESSAGE_INDEX;
import static com.exactpro.cradle.cassandra.dao.messages.MessageBatchEntity.FIELD_FIRST_MESSAGE_TIME;
import static com.exactpro.cradle.cassandra.dao.messages.MessageBatchEntity.FIELD_LAST_SEQUENCE;
import static com.exactpro.cradle.cassandra.dao.messages.MessageBatchEntity.FIELD_SEQUENCE;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class MessagesWorker extends Worker {
    private static final Logger logger = LoggerFactory.getLogger(MessagesWorker.class);

    private static final MetricHolder<StreamLabel> MESSAGE_READ_METRIC = new MetricHolder<>(
            Counter.build()
                    .name("cradle_message_read_total")
                    .help("Fetched messages")
                    .labelNames(StreamLabel.LABEL_NAMES)
                    .register()
    );
    private static final MetricHolder<StreamLabel> MESSAGE_BATCH_READ_METRIC = new MetricHolder<>(
            Counter.build()
                    .name("cradle_message_batch_read_total")
                    .help("Fetched message batches")
                    .labelNames(StreamLabel.LABEL_NAMES)
                    .register()
    );
    private static final MetricHolder<StreamLabel> MESSAGE_STORE_METRIC = new MetricHolder<>(
            Counter.build()
                    .name("cradle_message_stored_total")
                    .help("Stored messages")
                    .labelNames(StreamLabel.LABEL_NAMES)
                    .register()
    );
    private static final MetricHolder<StreamLabel> MESSAGE_BATCH_STORE_METRIC = new MetricHolder<>(
            Counter.build()
                    .name("cradle_message_batch_stored_total")
                    .help("Stored message batches")
                    .labelNames(StreamLabel.LABEL_NAMES)
                    .register()
    );
    private static final MetricHolder<StreamLabel> MESSAGE_STORE_UNCOMPRESSED_BYTES = new MetricHolder<>(
            Counter.build()
                    .name("cradle_message_stored_uncompressed_bytes_total")
                    .help("Stored uncompressed message bytes")
                    .labelNames(StreamLabel.LABEL_NAMES)
                    .register()
    );
    private static final MetricHolder<StreamLabel> MESSAGE_STORE_COMPRESSED_BYTES = new MetricHolder<>(
            Counter.build()
                    .name("cradle_message_stored_compressed_bytes_total")
                    .help("Stored compressed message bytes")
                    .labelNames(StreamLabel.LABEL_NAMES)
                    .register()
    );

    private final MessageStatisticsCollector messageStatisticsCollector;
    private final SessionStatisticsCollector sessionStatisticsCollector;

    public MessagesWorker(
            WorkerSupplies workerSupplies
            , MessageStatisticsCollector messageStatisticsCollector
            , SessionStatisticsCollector sessionStatisticsCollector
    ) {
        super(workerSupplies);
        this.messageStatisticsCollector = messageStatisticsCollector;
        this.sessionStatisticsCollector = sessionStatisticsCollector;
    }

    public static StoredMessageBatch mapMessageBatchEntity(PageId pageId, MessageBatchEntity entity) {
        try {
            StoredMessageBatch batch = MessageBatchEntityUtils.toStoredMessageBatch(entity, pageId);
            updateMessageReadMetrics(batch);
            return batch;
        } catch (DataFormatException | IOException | CompressException e) {
            throw new CompletionException("Error while converting message batch entity into stored message batch", e);
        }
    }

    public static StoredGroupedMessageBatch mapGroupedMessageBatchEntity(PageId pageId, GroupedMessageBatchEntity entity) {
        try {
            StoredGroupedMessageBatch batch = GroupedMessageEntityUtils.toStoredGroupedMessageBatch(entity, pageId);
            updateMessageReadMetrics(pageId.getBookId(), batch);
            return batch;
        } catch (DataFormatException | IOException | CompressException e) {
            throw new CompletionException("Error while converting message batch entity into stored message batch", e);
        }
    }

    private static void updateMessageReadMetrics(StoredMessageBatch batch) {
        StreamLabel key = new StreamLabel(
                batch.getId().getBookId().getName(),
                batch.getSessionAlias(),
                batch.getDirection().getLabel());
        MESSAGE_READ_METRIC.inc(key, batch.getMessageCount());
        MESSAGE_BATCH_READ_METRIC.inc(key);
    }

    private static void updateMessageReadMetrics(BookId bookId, StoredGroupedMessageBatch batch) {
        StreamLabel key = new StreamLabel(
                bookId.getName(),
                batch.getGroup());
        MESSAGE_READ_METRIC.inc(key, batch.getMessageCount());
        MESSAGE_BATCH_READ_METRIC.inc(key);
    }

    private static void updateMessageWriteMetrics(MessageBatchEntity entity, BookId bookId) {
        StreamLabel key = new StreamLabel(bookId.getName(), entity.getSessionAlias(), entity.getDirection());
        updateMessageWriteMetrics(key, entity.getMessageCount(), entity.getUncompressedContentSize());
        MESSAGE_STORE_COMPRESSED_BYTES.inc(key, entity.getContentSize());
    }

    private static void updateMessageWriteMetrics(StreamLabel key, int count, int uncompressedContentSize) {
        MESSAGE_STORE_METRIC.inc(key, count);
        MESSAGE_BATCH_STORE_METRIC.inc(key);
        MESSAGE_STORE_UNCOMPRESSED_BYTES.inc(key, uncompressedContentSize);
    }

    private static void updateMessageWriteMetrics(GroupedMessageBatchEntity entity, BookId bookId) {
        StreamLabel key = new StreamLabel(bookId.getName(), entity.getGroup());
        MESSAGE_STORE_METRIC.inc(key, entity.getMessageCount());
        MESSAGE_BATCH_STORE_METRIC.inc(key);
        MESSAGE_STORE_UNCOMPRESSED_BYTES.inc(key, entity.getUncompressedContentSize());
        MESSAGE_STORE_COMPRESSED_BYTES.inc(key, entity.getContentSize());
    }

    public CompletableFuture<CradleResultSet<StoredMessageBatch>> getMessageBatches(MessageFilter filter, BookInfo book)
            throws CradleStorageException {
        MessageBatchesIteratorProvider provider =
                new MessageBatchesIteratorProvider("get messages batches filtered by " + filter, filter,
                        getOperators(), book, composingService, selectQueryExecutor,
                        composeReadAttrs(filter.getFetchParameters()));
        return provider.nextIterator()
                .thenApplyAsync(r -> new CassandraCradleResultSet<>(r, provider), composingService);
    }

    public CompletableFuture<CradleResultSet<StoredGroupedMessageBatch>> getGroupedMessageBatches(
            GroupedMessageFilter filter,
            BookInfo book
    )
            throws CradleStorageException {
        GroupedMessageIteratorProvider provider =
                new GroupedMessageIteratorProvider("get messages batches filtered by " + filter, filter,
                        getOperators(), book, composingService, selectQueryExecutor,
                        composeReadAttrs(filter.getFetchParameters()), filter.getOrder());
        return provider.nextIterator()
                .thenApplyAsync(r -> new CassandraCradleResultSet<>(r, provider), composingService);
    }

    public CompletableFuture<CradleResultSet<StoredMessage>> getMessages(MessageFilter filter, BookInfo book)
            throws CradleStorageException {
        MessagesIteratorProvider provider =
                new MessagesIteratorProvider("get messages filtered by " + filter, filter,
                        getOperators(), book, composingService, selectQueryExecutor,
                        composeReadAttrs(filter.getFetchParameters()));
        return provider.nextIterator()
                .thenApplyAsync(r -> new CassandraCradleResultSet<>(r, provider), composingService);
    }

    private CompletableFuture<Row> getNearestTimeAndSequenceBefore(
            PageInfo page,
            MessageBatchOperator mbOperator, String sessionAlias, String direction, LocalDate messageDate,
            LocalTime messageTime, long sequence, Function<BoundStatementBuilder, BoundStatementBuilder> readAttrs
    ) {
        String queryInfo = format("get nearest time and sequence before %s for page '%s'",
                TimeUtils.toInstant(messageDate, messageTime), page.getId().getName());
        return selectQueryExecutor.executeSingleRowResultQuery(
                        () -> mbOperator.getNearestBatchTimeAndSequenceBefore(page.getId().getBookId().getName(), page.getId().getName(), sessionAlias,
                                direction, messageDate, messageTime, sequence, readAttrs), Function.identity(), queryInfo)
                .thenComposeAsync(row ->
                {
                    if (row != null)
                        return CompletableFuture.completedFuture(row);

                    return CompletableFuture.completedFuture(null);
                }, composingService);
    }

    public CompletableFuture<StoredMessageBatch> getMessageBatch(StoredMessageId id, PageId pageId) {
        logger.debug("Getting message batch for message with id '{}'", id);
        BookId bookId = pageId.getBookId();
        BookInfo bookInfo;
        try {
            bookInfo = getBook(bookId);
        } catch (CradleStorageException e) {
            return CompletableFuture.failedFuture(e);
        }

        LocalDateTime ldt = TimeUtils.toLocalTimestamp(id.getTimestamp());
        CassandraOperators operators = getOperators();
        MessageBatchEntityConverter mbEntityConverter = operators.getMessageBatchEntityConverter();
        MessageBatchOperator mbOperator = operators.getMessageBatchOperator();

        return getNearestTimeAndSequenceBefore(bookInfo.getPage(pageId), mbOperator, id.getSessionAlias(),
                id.getDirection().getLabel(), ldt.toLocalDate(), ldt.toLocalTime(), id.getSequence(), readAttrs)
                .thenComposeAsync(row ->
                {
                    if (row == null) {
                        logger.debug("No message batches found by id '{}'", id);
                        return CompletableFuture.completedFuture(null);
                    }
                    return selectQueryExecutor.executeSingleRowResultQuery(
                                    () -> mbOperator.get(pageId.getBookId().getName(), pageId.getName(), id.getSessionAlias(),
                                            id.getDirection().getLabel(), ldt.toLocalDate(),
                                            row.getLocalTime(FIELD_FIRST_MESSAGE_TIME), row.getLong(FIELD_SEQUENCE), readAttrs),
                                    mbEntityConverter::getEntity,
                                    format("get message batch for message with id '%s'", id))
                            .thenApplyAsync(entity ->
                            {
                                if (entity == null)
                                    return null;
                                StoredMessageBatch batch = mapMessageBatchEntity(pageId, entity);
                                logger.debug("Message batch with id '{}' found for message with id '{}'",
                                        batch.getId(), id);
                                return batch;
                            }, composingService);
                }, composingService);
    }

    public CompletableFuture<StoredMessage> getMessage(StoredMessageId id, PageId pageId) {
        return getMessageBatch(id, pageId)
                .thenComposeAsync(batch ->
                {
                    if (batch == null)
                        return CompletableFuture.completedFuture(null);

                    Optional<StoredMessage>
                            found = batch.getMessages().stream().filter(m -> id.equals(m.getId())).findFirst();
                    if (found.isPresent())
                        return CompletableFuture.completedFuture(found.get());

                    logger.debug("There is no message with id '{}' in batch '{}'", id, batch.getId());
                    return CompletableFuture.completedFuture(null);
                }, composingService);
    }

    private CompletableFuture<PageGroupEntity> storePageGroup(GroupedMessageBatchEntity groupedMessageBatchEntity) {
        CassandraOperators operators = getOperators();
        PageGroupEntity pageGroupEntity = new PageGroupEntity(
                groupedMessageBatchEntity.getBook(),
                groupedMessageBatchEntity.getPage(),
                groupedMessageBatchEntity.getGroup());

        if (operators.getPageGroupCache().contains(pageGroupEntity)) {
            logger.debug("Skipped writing group '{}' for page '{}'", pageGroupEntity.getGroup(), pageGroupEntity.getPage());
            return CompletableFuture.completedFuture(null);
        }

        logger.debug("Writing group '{}' for page '{}'", pageGroupEntity.getGroup(), pageGroupEntity.getPage());
        return operators.getPageGroupsOperator().write(pageGroupEntity, writeAttrs)
                .whenCompleteAsync((result, e) -> {
                    if (e == null) {
                        operators.getPageGroupCache().store(pageGroupEntity);
                    }
                }, composingService);
    }

    private CompletableFuture<GroupEntity> storeGroup(GroupedMessageBatchEntity groupedMessageBatchEntity) {
        CassandraOperators operators = getOperators();
        GroupEntity groupEntity = new GroupEntity(groupedMessageBatchEntity.getBook(), groupedMessageBatchEntity.getGroup());

        if (operators.getGroupCache().contains(groupEntity)) {
            logger.debug("Skipped writing group '{}'", groupEntity.getGroup());
            return CompletableFuture.completedFuture(null);
        }

        logger.debug("Writing group '{}'", groupEntity.getGroup());
        return operators.getGroupsOperator().write(groupEntity, writeAttrs)
                .whenCompleteAsync((result, e) -> {
                    if (e == null) {
                        operators.getGroupCache().store(groupEntity);
                    }
                }, composingService);
    }

    public CompletableFuture<PageSessionEntity> storePageSession(PageId pageId, String sessionAlias, Direction direction) {
        CassandraOperators operators = getOperators();
        CachedPageSession cachedPageSession = new CachedPageSession(pageId.toString(),
                sessionAlias, direction.getLabel());
        if (!operators.getPageSessionsCache().store(cachedPageSession)) {
            logger.debug("Skipped writing page/session of page '{}', session alias '{}', direction '{}'", pageId, sessionAlias, direction);
            return CompletableFuture.completedFuture(null);
        }

        logger.debug("Writing page/session of page '{}', session alias '{}', direction '{}'", pageId, sessionAlias, direction);

        return operators.getPageSessionsOperator().write(
                new PageSessionEntity(pageId.getBookId().getName(), pageId.getName(), sessionAlias, direction.getLabel()),
                writeAttrs
        );
    }

    public CompletableFuture<PageSessionEntity> storePageSession(MessageBatchToStore batch, PageId pageId) {
        return storePageSession(pageId, batch.getSessionAlias(), batch.getDirection());
    }

    public CompletableFuture<SessionEntity> storeSession(BookId bookId, String sessionAlias) {
        CassandraOperators operators = getOperators();
        CachedSession cachedSession = new CachedSession(bookId.toString(), sessionAlias);
        if (!operators.getSessionsCache().store(cachedSession)) {
            logger.debug("Skipped writing book/session of book '{}', session alias '{}'", bookId, sessionAlias);
            return CompletableFuture.completedFuture(null);
        }
        logger.debug("Writing book/session of book '{}', session alias '{}'", bookId, sessionAlias);

        return operators.getSessionsOperator().write(new SessionEntity(bookId.toString(), sessionAlias), writeAttrs);
    }

    public CompletableFuture<SessionEntity> storeSession(MessageBatchToStore batch) {
        return storeSession(batch.getId().getBookId(), batch.getSessionAlias());
    }

    public CompletableFuture<Void> storeMessageBatch(MessageBatchToStore batch, PageId pageId) {
        BookId bookId = pageId.getBookId();
        MessageBatchOperator mbOperator = getOperators().getMessageBatchOperator();

        return CompletableFuture.supplyAsync(() -> {
            try {
                return MessageBatchEntityUtils.toSerializedEntity(batch, pageId, settings.getCompressionType(), settings.getMaxUncompressedMessageBatchSize());
            } catch (Exception e) {
                throw new CompletionException(e);
            }
        }, composingService).thenCompose(serializedEntity -> {
            MessageBatchEntity entity = serializedEntity.getEntity();
            List<SerializedMessageMetadata> meta = serializedEntity.getSerializedEntityData().getSerializedEntityMetadata();

            return mbOperator.write(entity, writeAttrs)
                    .thenRunAsync(() -> {
                        messageStatisticsCollector.updateMessageBatchStatistics(bookId,
                                entity.getPage(),
                                entity.getSessionAlias(),
                                entity.getDirection(),
                                meta);
                        sessionStatisticsCollector.updateSessionStatistics(bookId,
                                entity.getPage(),
                                SessionRecordType.SESSION,
                                entity.getSessionAlias(),
                                meta);
                        updateMessageWriteMetrics(entity, bookId);
                    }, composingService);
        });
    }

    public CompletableFuture<Void> storeGroupedMessageBatch(
            GroupedMessageBatchToStore batchToStore,
            PageId pageId,
            boolean storeSessionMetadata
    ) {
        BookId bookId = pageId.getBookId();
        GroupedMessageBatchOperator gmbOperator = getOperators().getGroupedMessageBatchOperator();

        return CompletableFuture.supplyAsync(() -> {
            try {
                return GroupedMessageEntityUtils.toSerializedEntity(
                        batchToStore,
                        pageId,
                        settings.getCompressionType(),
                        settings.getMaxUncompressedMessageBatchSize()
                );
            } catch (Exception e) {
                throw new CompletionException(e);
            }
        }, composingService).thenCompose(serializedEntity -> {
            GroupedMessageBatchEntity entity = serializedEntity.getEntity();
            List<SerializedMessageMetadata> meta = serializedEntity.getSerializedEntityData().getSerializedEntityMetadata();

            CompletableFuture<Void> future = gmbOperator.write(entity, writeAttrs)
                    .thenRunAsync(() -> CompletableFuture.allOf(
                            storePageGroup(entity),
                            storeGroup(entity),
                            CompletableFuture.runAsync(() -> {
                                messageStatisticsCollector.updateMessageBatchStatistics(bookId,
                                        pageId.getName(),
                                        entity.getGroup(),
                                        "",
                                        meta);
                                sessionStatisticsCollector.updateSessionStatistics(bookId,
                                        pageId.getName(),
                                        SessionRecordType.SESSION_GROUP,
                                        entity.getGroup(),
                                        meta);
                                updateMessageWriteMetrics(entity, bookId);
                            }, composingService)
                    ), composingService);
            if (storeSessionMetadata) {
                future = updateSessionStatistics(future, bookId, pageId, batchToStore, meta);
            }
            return future;
        });
    }

    private CompletableFuture<Void> updateSessionStatistics(CompletableFuture<Void> future, BookId bookId, PageId pageId, GroupedMessageBatchToStore batchToStore, List<SerializedMessageMetadata> meta) {
        return future.thenApplyAsync((unused) -> getDirectionToSessionAliases(batchToStore), composingService)
                .thenComposeAsync((sessions) -> storeSessions(pageId, sessions), composingService)
                .thenComposeAsync((sessions) -> storePageSessions(bookId, sessions), composingService)
                .thenApplyAsync((sessions) -> {
                    for (Map.Entry<Direction, Set<String>> entry : sessions.entrySet()) {
                        Direction direction = entry.getKey();
                        Set<String> sessionAliases = entry.getValue();
                        for (String sessionAlias : sessionAliases) {
                            List<SerializedMessageMetadata> streamMetadatas = meta.stream()
                                    .filter((metadata) ->
                                            metadata.getDirection() == direction && Objects.equals(metadata.getSessionAlias(), sessionAlias)
                                    ).collect(Collectors.toList());
                            messageStatisticsCollector.updateMessageBatchStatistics(bookId,
                                    pageId.getName(),
                                    sessionAlias,
                                    direction.getLabel(),
                                    streamMetadatas);
                            sessionStatisticsCollector.updateSessionStatistics(bookId,
                                    pageId.getName(),
                                    SessionRecordType.SESSION,
                                    sessionAlias,
                                    streamMetadatas);
                            updateMessageWriteMetrics(
                                    new StreamLabel(bookId.getName(), sessionAlias, direction.getLabel()),
                                    streamMetadatas.size(),
                                    streamMetadatas.stream()
                                            .mapToInt(SerializedMessageMetadata::getSerializedEntitySize).sum()
                            );
                        }
                    }
                    return null;
                }, composingService);
    }

    private static Map<Direction, Set<String>> getDirectionToSessionAliases(GroupedMessageBatchToStore batch) {
        Map<Direction, Set<String>> sessions = new EnumMap<>(Direction.class);
        for (Direction direction : Direction.values()) {
            sessions.put(direction, new HashSet<>());
        }
        for (StoredMessage message : batch.getMessages()) {
            sessions.get(message.getDirection()).add(message.getSessionAlias());
        }
        return sessions;
    }

    private CompletableFuture<Map<Direction, Set<String>>> storeSessions(PageId pageId, Map<Direction, Set<String>> sessions) {
        return CompletableFuture.allOf(
                sessions.entrySet().stream()
                        .flatMap((entry) -> entry.getValue().stream()
                                .map((sessionAlias) -> storePageSession(pageId, sessionAlias, entry.getKey()))
                        ).toArray(CompletableFuture[]::new)
        ).thenApply((unused) -> sessions);
    }

    private CompletableFuture<Map<Direction, Set<String>>> storePageSessions(BookId bookId, Map<Direction, Set<String>> sessions) {
        return CompletableFuture.allOf(
                sessions.values().stream()
                        .flatMap(Set::stream)
                        .distinct()
                        .map((sessionAlias) -> storeSession(bookId, sessionAlias))
                        .toArray(CompletableFuture[]::new)
        ).thenApply((unused) -> sessions);
    }

    public long getBoundarySequence(String sessionAlias, Direction direction, BookInfo book, boolean first)
            throws CradleStorageException {
        MessageBatchOperator mbOp = getOperators().getMessageBatchOperator();
        PageInfo currentPage = first ? book.getFirstPage() : book.getLastPage();
        Row row = null;

        while (row == null && currentPage != null) {
            String page = currentPage.getId().getName();
            String bookName = book.getId().getName();
            String queryInfo = format("get %s sequence for book '%s' page '%s', session alias '%s', " +
                    "direction '%s'", (first ? "first" : "last"), bookName, page, sessionAlias, direction);
            try {
                row = selectQueryExecutor.executeSingleRowResultQuery(
                        () -> first ? mbOp.getFirstSequence(bookName, page, sessionAlias, direction.getLabel(), readAttrs)
                                : mbOp.getLastSequence(bookName, page, sessionAlias, direction.getLabel(), readAttrs),
                        Function.identity(), queryInfo).get();
            } catch (InterruptedException | ExecutionException e) {
                throw new CradleStorageException("Error occurs while " + queryInfo, e);
            }

            if (row == null)
                currentPage = first ? book.getNextPage(currentPage.getStarted())
                        : book.getPreviousPage(currentPage.getStarted());
        }
        if (row == null) {
            logger.debug("There is no messages yet in book '{}' with session alias '{}' and direction '{}'",
                    book.getId(), sessionAlias, direction);
            return EMPTY_MESSAGE_INDEX;
        }

        return row.getLong(first ? FIELD_SEQUENCE : FIELD_LAST_SEQUENCE);
    }

    private static class StreamLabel implements MetricHolder.LabelHolder {
        private static final String[] LABEL_NAMES = new String[]{BOOK_ID, SESSION_ALIAS_OR_GROUP, DIRECTION};
        private final String bookId;
        private final String sessionAliasOrGroup;
        private final String direction;
        private final int hash;

        private StreamLabel(
                @Nonnull String bookId,
                @Nonnull String sessionAlias,
                @Nonnull String direction
        ) {
            this.bookId = requireNonNull(bookId, "'bookId' can't be null");
            this.sessionAliasOrGroup = requireNonNull(sessionAlias, "'sessionAliasOrGroup' can't be null");
            this.direction = requireNonNull(direction, "'direction' can't be null");
            this.hash = Objects.hash(bookId, sessionAliasOrGroup, direction);
        }

        private StreamLabel(
                @Nonnull String bookId,
                @Nonnull String sessionGroup
        ) {
            this(bookId, sessionGroup, "");
        }

        @Override
        public String[] getLabels() {
            return new String[]{bookId, sessionAliasOrGroup, direction};
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            StreamLabel that = (StreamLabel) o;
            return Objects.equals(bookId, that.bookId)
                    && Objects.equals(sessionAliasOrGroup, that.sessionAliasOrGroup)
                    && Objects.equals(direction, that.direction);
        }

        @Override
        public int hashCode() {
            return hash;
        }
    }
}
