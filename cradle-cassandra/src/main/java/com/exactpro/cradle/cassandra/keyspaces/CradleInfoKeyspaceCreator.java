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

package com.exactpro.cradle.cassandra.keyspaces;

import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import com.datastax.oss.driver.api.querybuilder.schema.CreateTable;
import com.exactpro.cradle.cassandra.CassandraStorageSettings;
import com.exactpro.cradle.cassandra.dao.EntityStatisticsEntity;
import com.exactpro.cradle.cassandra.dao.MessageStatisticsEntity;
import com.exactpro.cradle.cassandra.dao.SessionStatisticsEntity;
import com.exactpro.cradle.cassandra.dao.books.BookEntity;
import com.exactpro.cradle.cassandra.dao.books.PageEntity;
import com.exactpro.cradle.cassandra.dao.books.PageNameEntity;
import com.exactpro.cradle.cassandra.dao.intervals.IntervalEntity;
import com.exactpro.cradle.cassandra.dao.labels.LabelEntity;
import com.exactpro.cradle.cassandra.dao.messages.*;
import com.exactpro.cradle.cassandra.dao.testevents.PageScopeEntity;
import com.exactpro.cradle.cassandra.dao.testevents.ScopeEntity;
import com.exactpro.cradle.cassandra.dao.testevents.TestEventEntity;
import com.exactpro.cradle.cassandra.utils.QueryExecutor;
import com.exactpro.cradle.utils.CradleStorageException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.function.Supplier;

public class CradleInfoKeyspaceCreator extends KeyspaceCreator
{
	private static final Logger logger = LoggerFactory.getLogger(CradleInfoKeyspaceCreator.class);
	private static final String TEST_EVENT_PARENT_INDEX = "test_event_parent_index";

	public CradleInfoKeyspaceCreator(QueryExecutor exec, CassandraStorageSettings settings)
	{
		super(settings.getKeyspace(), exec, settings);
	}

	@Override
	protected void createTables() throws IOException
	{
		createBooks();
		createPages();
		createPagesNames();

		createSessions();
		createScopes();

		createMessages();
		createGroupedMessages();
		createPageSessions();
		createPageGroups();
		createGroups();

		createTestEvents();
		createPageScopes();
		createTestEventParentIndex();

		createLabelsTable();
		createIntervals();

		createMessageStatistics();
		createEntityStatistics();

		createSessionStatistics();

	}

	@Override
	public void createAll() throws IOException, CradleStorageException
	{
		if (getKeyspaceMetadata() != null)
		{
			logger.info("\"Cradle Info\" keyspace '{}' already exists", getKeyspace());
		}
		super.createAll();
	}

	private void createBooks() throws IOException
	{
		String tableName = BookEntity.TABLE_NAME;
		createTable(tableName, () -> SchemaBuilder.createTable(getKeyspace(), tableName).ifNotExists()
				.withPartitionKey(BookEntity.FIELD_NAME, DataTypes.TEXT)
				.withColumn(BookEntity.FIELD_FULLNAME, DataTypes.TEXT)
				.withColumn(BookEntity.FIELD_DESCRIPTION, DataTypes.TEXT)
				.withColumn(BookEntity.FIELD_CREATED, DataTypes.TIMESTAMP)
				.withColumn(BookEntity.FIELD_SCHEMA_VERSION, DataTypes.TEXT));
	}

	private void createPages() throws IOException {
		String tableName = PageEntity.TABLE_NAME;
		createTable(tableName, () -> SchemaBuilder.createTable(getKeyspace(), tableName).ifNotExists()
				.withPartitionKey(PageEntity.FIELD_BOOK, DataTypes.TEXT)
				.withClusteringColumn(PageEntity.FIELD_START_DATE, DataTypes.DATE)
				.withClusteringColumn(PageEntity.FIELD_START_TIME, DataTypes.TIME)
				.withColumn(PageEntity.FIELD_NAME, DataTypes.TEXT)
				.withColumn(PageEntity.FIELD_COMMENT, DataTypes.TEXT)
				.withColumn(PageEntity.FIELD_END_DATE, DataTypes.DATE)
				.withColumn(PageEntity.FIELD_END_TIME, DataTypes.TIME)
				.withColumn(PageEntity.FIELD_UPDATED, DataTypes.TIMESTAMP)
				.withColumn(PageEntity.FIELD_REMOVED, DataTypes.TIMESTAMP));
	}

	private void createPagesNames() throws IOException
	{
		String tableName = PageNameEntity.TABLE_NAME;
		createTable(tableName, () -> SchemaBuilder.createTable(getKeyspace(), tableName).ifNotExists()
				.withPartitionKey(PageNameEntity.FIELD_BOOK, DataTypes.TEXT)
				.withPartitionKey(PageNameEntity.FIELD_NAME, DataTypes.TEXT)
				.withColumn(PageNameEntity.FIELD_START_DATE, DataTypes.DATE)
				.withColumn(PageNameEntity.FIELD_START_TIME, DataTypes.TIME)
				.withColumn(PageNameEntity.FIELD_COMMENT, DataTypes.TEXT)
				.withColumn(PageNameEntity.FIELD_END_DATE, DataTypes.DATE)
				.withColumn(PageNameEntity.FIELD_END_TIME, DataTypes.TIME));
	}

	private void createSessions() throws IOException
	{
		String tableName = SessionEntity.TABLE_NAME;
		createTable(tableName, () -> SchemaBuilder.createTable(getKeyspace(), tableName).ifNotExists()
				.withPartitionKey(SessionEntity.FIELD_BOOK, DataTypes.TEXT)
				.withClusteringColumn(SessionEntity.FIELD_SESSION_ALIAS, DataTypes.TEXT));
	}

	private void createScopes() throws IOException
	{
		String tableName = ScopeEntity.TABLE_NAME;
		createTable(tableName, () -> SchemaBuilder.createTable(getKeyspace(), tableName).ifNotExists()
				.withPartitionKey(ScopeEntity.FIELD_BOOK, DataTypes.TEXT)
				.withClusteringColumn(ScopeEntity.FIELD_SCOPE, DataTypes.TEXT));
	}

	private void createMessages() throws IOException
	{
		String tableName = MessageBatchEntity.TABLE_NAME;
		createTable(tableName, () -> SchemaBuilder.createTable(getKeyspace(), tableName).ifNotExists()
				.withPartitionKey(MessageBatchEntity.FIELD_BOOK, DataTypes.TEXT)
				.withPartitionKey(MessageBatchEntity.FIELD_PAGE, DataTypes.TEXT)
				.withPartitionKey(MessageBatchEntity.FIELD_SESSION_ALIAS, DataTypes.TEXT)
				.withPartitionKey(MessageBatchEntity.FIELD_DIRECTION, DataTypes.TEXT)

				.withClusteringColumn(MessageBatchEntity.FIELD_FIRST_MESSAGE_DATE, DataTypes.DATE)
				.withClusteringColumn(MessageBatchEntity.FIELD_FIRST_MESSAGE_TIME, DataTypes.TIME)
				.withClusteringColumn(MessageBatchEntity.FIELD_SEQUENCE, DataTypes.BIGINT)

				.withColumn(MessageBatchEntity.FIELD_LAST_MESSAGE_DATE, DataTypes.DATE)
				.withColumn(MessageBatchEntity.FIELD_LAST_MESSAGE_TIME, DataTypes.TIME)
				.withColumn(MessageBatchEntity.FIELD_LAST_SEQUENCE, DataTypes.BIGINT)
				.withColumn(MessageBatchEntity.FIELD_MESSAGE_COUNT, DataTypes.INT)
				.withColumn(MessageBatchEntity.FIELD_COMPRESSED, DataTypes.BOOLEAN)
				.withColumn(MessageBatchEntity.FIELD_LABELS, DataTypes.setOf(DataTypes.TEXT))
				.withColumn(MessageBatchEntity.FIELD_CONTENT, DataTypes.BLOB)
				.withColumn(MessageBatchEntity.FIELD_REC_DATE, DataTypes.TIMESTAMP));
	}

	private void createGroupedMessages() throws IOException
	{
		String tableName = GroupedMessageBatchEntity.TABLE_NAME;
		createTable(tableName, () -> SchemaBuilder.createTable(getKeyspace(), tableName).ifNotExists()
				.withPartitionKey(GroupedMessageBatchEntity.FIELD_BOOK, DataTypes.TEXT)
				.withPartitionKey(GroupedMessageBatchEntity.FIELD_PAGE, DataTypes.TEXT)
				.withPartitionKey(GroupedMessageBatchEntity.FIELD_ALIAS_GROUP, DataTypes.TEXT)

				.withClusteringColumn(GroupedMessageBatchEntity.FIELD_FIRST_MESSAGE_DATE, DataTypes.DATE)
				.withClusteringColumn(GroupedMessageBatchEntity.FIELD_FIRST_MESSAGE_TIME, DataTypes.TIME)

				.withColumn(GroupedMessageBatchEntity.FIELD_LAST_MESSAGE_DATE, DataTypes.DATE)
				.withColumn(GroupedMessageBatchEntity.FIELD_LAST_MESSAGE_TIME, DataTypes.TIME)
				.withColumn(GroupedMessageBatchEntity.FIELD_REC_DATE, DataTypes.TIMESTAMP)
				.withColumn(GroupedMessageBatchEntity.FIELD_MESSAGE_COUNT, DataTypes.INT)
				.withColumn(GroupedMessageBatchEntity.FIELD_COMPRESSED, DataTypes.BOOLEAN)
				.withColumn(GroupedMessageBatchEntity.FIELD_LABELS, DataTypes.setOf(DataTypes.TEXT))
				.withColumn(GroupedMessageBatchEntity.FIELD_CONTENT, DataTypes.BLOB));
	}

	private void createPageSessions() throws IOException
	{
		String tableName = PageSessionEntity.TABLE_NAME;
		createTable(tableName, () -> SchemaBuilder.createTable(getKeyspace(), tableName).ifNotExists()
				.withPartitionKey(PageSessionEntity.FIELD_BOOK, DataTypes.TEXT)
				.withPartitionKey(PageSessionEntity.FIELD_PAGE, DataTypes.TEXT)

				.withClusteringColumn(PageSessionEntity.FIELD_SESSION_ALIAS, DataTypes.TEXT)
				.withClusteringColumn(PageSessionEntity.FIELD_DIRECTION, DataTypes.TEXT));
	}

	private void createPageGroups () throws IOException {
		String tableName = PageGroupEntity.TABLE_NAME;

		createTable(tableName, () -> SchemaBuilder.createTable(getKeyspace(), tableName).ifNotExists()
				.withPartitionKey(PageGroupEntity.FIELD_BOOK, DataTypes.TEXT)
				.withPartitionKey(PageGroupEntity.FIELD_PAGE, DataTypes.TEXT)
				.withClusteringColumn(PageGroupEntity.FIELD_GROUP, DataTypes.TEXT));
	}

	private void createGroups () throws IOException {
		String tableName = GroupEntity.TABLE_NAME;

		createTable(tableName, () -> SchemaBuilder.createTable(getKeyspace(), tableName).ifNotExists()
				.withPartitionKey(GroupEntity.FIELD_BOOK, DataTypes.TEXT)
				.withClusteringColumn(GroupEntity.FIELD_GROUP, DataTypes.TEXT));
	}

	private void createTestEvents() throws IOException
	{
		String tableName = TestEventEntity.TABLE_NAME;
		createTable(tableName, () -> SchemaBuilder.createTable(getKeyspace(), tableName).ifNotExists()
				.withPartitionKey(TestEventEntity.FIELD_BOOK, DataTypes.TEXT)
				.withPartitionKey(TestEventEntity.FIELD_PAGE, DataTypes.TEXT)
				.withPartitionKey(TestEventEntity.FIELD_SCOPE, DataTypes.TEXT)

				.withClusteringColumn(TestEventEntity.FIELD_START_DATE, DataTypes.DATE)
				.withClusteringColumn(TestEventEntity.FIELD_START_TIME, DataTypes.TIME)
				.withClusteringColumn(TestEventEntity.FIELD_ID, DataTypes.TEXT)

				.withColumn(TestEventEntity.FIELD_NAME, DataTypes.TEXT)
				.withColumn(TestEventEntity.FIELD_TYPE, DataTypes.TEXT)
				.withColumn(TestEventEntity.FIELD_SUCCESS, DataTypes.BOOLEAN)
				.withColumn(TestEventEntity.FIELD_ROOT, DataTypes.BOOLEAN)
				.withColumn(TestEventEntity.FIELD_PARENT_ID, DataTypes.TEXT)
				.withColumn(TestEventEntity.FIELD_EVENT_BATCH, DataTypes.BOOLEAN)
				.withColumn(TestEventEntity.FIELD_EVENT_COUNT, DataTypes.INT)
				.withColumn(TestEventEntity.FIELD_END_DATE, DataTypes.DATE)
				.withColumn(TestEventEntity.FIELD_END_TIME, DataTypes.TIME)
				.withColumn(TestEventEntity.FIELD_COMPRESSED, DataTypes.BOOLEAN)
				.withColumn(TestEventEntity.FIELD_MESSAGES, DataTypes.BLOB)
				.withColumn(TestEventEntity.FIELD_LABELS, DataTypes.setOf(DataTypes.TEXT))
				.withColumn(TestEventEntity.FIELD_CONTENT, DataTypes.BLOB)
				.withColumn(TestEventEntity.FIELD_REC_DATE, DataTypes.TIMESTAMP));
	}

	private void createPageScopes() throws IOException
	{
		String tableName = PageScopeEntity.TABLE_NAME;
		createTable(tableName, () -> SchemaBuilder.createTable(getKeyspace(), tableName).ifNotExists()
				.withPartitionKey(PageScopeEntity.FIELD_BOOK, DataTypes.TEXT)
				.withPartitionKey(PageScopeEntity.FIELD_PAGE, DataTypes.TEXT)
				.withClusteringColumn(PageScopeEntity.FIELD_SCOPE, DataTypes.TEXT));
	}

	private void createTestEventParentIndex() throws IOException
	{
		createIndex(TEST_EVENT_PARENT_INDEX, TestEventEntity.TABLE_NAME, TestEventEntity.FIELD_PARENT_ID);
	}

	private void createLabelsTable() throws IOException
	{
		String tableName = LabelEntity.TABLE_NAME;
		createTable(tableName, () -> SchemaBuilder.createTable(getKeyspace(), tableName).ifNotExists()
				.withPartitionKey(LabelEntity.FIELD_BOOK, DataTypes.TEXT)
				.withPartitionKey(LabelEntity.FIELD_PAGE, DataTypes.TEXT)
				.withClusteringColumn(LabelEntity.FIELD_NAME, DataTypes.TEXT));
	}

	private void createIntervals() throws IOException
	{
		String tableName = IntervalEntity.TABLE_NAME;
		createTable(tableName, () -> SchemaBuilder.createTable(getKeyspace(), tableName).ifNotExists()
				.withPartitionKey(IntervalEntity.FIELD_BOOK, DataTypes.TEXT)
				.withPartitionKey(IntervalEntity.FIELD_PAGE, DataTypes.TEXT)
				.withPartitionKey(IntervalEntity.FIELD_INTERVAL_START_DATE, DataTypes.DATE)
				.withClusteringColumn(IntervalEntity.FIELD_CRAWLER_NAME, DataTypes.TEXT)
				.withClusteringColumn(IntervalEntity.FIELD_CRAWLER_VERSION, DataTypes.TEXT)
				.withClusteringColumn(IntervalEntity.FIELD_CRAWLER_TYPE, DataTypes.TEXT)
				.withClusteringColumn(IntervalEntity.FIELD_INTERVAL_START_TIME, DataTypes.TIME)
				.withColumn(IntervalEntity.FIELD_INTERVAL_END_DATE, DataTypes.DATE)
				.withColumn(IntervalEntity.FIELD_INTERVAL_END_TIME, DataTypes.TIME)
				.withColumn(IntervalEntity.FIELD_INTERVAL_LAST_UPDATE_DATE, DataTypes.DATE)
				.withColumn(IntervalEntity.FIELD_INTERVAL_LAST_UPDATE_TIME, DataTypes.TIME)
				.withColumn(IntervalEntity.FIELD_RECOVERY_STATE_JSON, DataTypes.TEXT)
				.withColumn(IntervalEntity.FIELD_INTERVAL_PROCESSED, DataTypes.BOOLEAN));
	}

	private void createMessageStatistics() throws IOException
	{
		String tableName = MessageStatisticsEntity.TABLE_NAME;
		createTable(tableName, () -> SchemaBuilder.createTable(getKeyspace(), tableName).ifNotExists()
				.withPartitionKey(MessageStatisticsEntity.FIELD_BOOK, DataTypes.TEXT)
				.withPartitionKey(MessageStatisticsEntity.FIELD_PAGE, DataTypes.TEXT)
				.withPartitionKey(MessageStatisticsEntity.FIELD_SESSION_ALIAS, DataTypes.TEXT)
				.withPartitionKey(MessageStatisticsEntity.FIELD_DIRECTION, DataTypes.TEXT)
				.withPartitionKey(MessageStatisticsEntity.FIELD_FRAME_TYPE, DataTypes.TINYINT)
				.withClusteringColumn(MessageStatisticsEntity.FIELD_FRAME_START, DataTypes.TIMESTAMP)
				.withColumn(MessageStatisticsEntity.FIELD_ENTITY_COUNT, DataTypes.COUNTER)
				.withColumn(MessageStatisticsEntity.FIELD_ENTITY_SIZE, DataTypes.COUNTER));
	}

	private void createEntityStatistics() throws IOException
	{
		String tableName = EntityStatisticsEntity.TABLE_NAME;
		createTable(tableName, () -> SchemaBuilder.createTable(getKeyspace(), tableName).ifNotExists()
				.withPartitionKey(EntityStatisticsEntity.FIELD_BOOK, DataTypes.TEXT)
				.withPartitionKey(EntityStatisticsEntity.FIELD_PAGE, DataTypes.TEXT)
				.withPartitionKey(EntityStatisticsEntity.FIELD_ENTITY_TYPE, DataTypes.TINYINT)
				.withPartitionKey(EntityStatisticsEntity.FIELD_FRAME_TYPE, DataTypes.TINYINT)
				.withClusteringColumn(EntityStatisticsEntity.FIELD_FRAME_START, DataTypes.TIMESTAMP)
				.withColumn(EntityStatisticsEntity.FIELD_ENTITY_COUNT, DataTypes.COUNTER)
				.withColumn(EntityStatisticsEntity.FIELD_ENTITY_SIZE, DataTypes.COUNTER));
	}

	private void createSessionStatistics () throws IOException
	{
		String tableName = SessionStatisticsEntity.TABLE_NAME;
		createTable(tableName, () -> SchemaBuilder.createTable(getKeyspace(), tableName).ifNotExists()
				.withPartitionKey(SessionStatisticsEntity.FIELD_BOOK, DataTypes.TEXT)
				.withPartitionKey(SessionStatisticsEntity.FIELD_PAGE, DataTypes.TEXT)
				.withPartitionKey(SessionStatisticsEntity.FIELD_RECORD_TYPE, DataTypes.TINYINT)
				.withPartitionKey(SessionStatisticsEntity.FIELD_FRAME_TYPE, DataTypes.TINYINT)
				.withClusteringColumn(SessionStatisticsEntity.FIELD_FRAME_START, DataTypes.TIMESTAMP)
				.withClusteringColumn(SessionStatisticsEntity.FIELD_SESSION, DataTypes.TEXT));
	}

	@Override
	protected void createTable(String tableName, Supplier<CreateTable> query) throws IOException {
		super.createTable(tableName, query);
	}

	@Override
	protected void createIndex(String indexName, String tableName, String columnName) throws IOException {
		super.createIndex(indexName, tableName, columnName);
	}

}
