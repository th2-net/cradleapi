/*
 * Copyright 2024 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.cradle.cassandra.dao.books;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.*;
import com.datastax.oss.driver.api.mapper.MapperContext;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.exactpro.cradle.cassandra.dao.BoundStatementBuilderWrapper;

import java.util.Collection;
import java.util.function.Function;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.bindMarker;

public class PageBatchInserter {
    private final CqlSession session;
    private final PreparedStatement pageInsertStatement;
    private final PreparedStatement pageUpdateStatement;
    private final PreparedStatement pageNameInsertStatement;
    private final PreparedStatement pageNameUpdateStatement;

    public PageBatchInserter(MapperContext context) {
        this.session = context.getSession();

        var keyspaceId = context.getKeyspaceId();
        var pageTableId = CqlIdentifier.fromCql(PageEntity.TABLE_NAME);
        var pageNamesTableId = CqlIdentifier.fromCql(PageNameEntity.TABLE_NAME);

        this.pageInsertStatement = session.prepare(QueryBuilder.insertInto(keyspaceId, pageTableId)
                .value(PageEntity.FIELD_BOOK, bindMarker(PageEntity.FIELD_BOOK))
                .value(PageEntity.FIELD_START_DATE, bindMarker(PageEntity.FIELD_START_DATE))
                .value(PageEntity.FIELD_START_TIME, bindMarker(PageEntity.FIELD_START_TIME))
                .value(PageEntity.FIELD_NAME, bindMarker(PageEntity.FIELD_NAME))
                .value(PageEntity.FIELD_COMMENT, bindMarker(PageEntity.FIELD_COMMENT))
                .value(PageEntity.FIELD_END_DATE, bindMarker(PageEntity.FIELD_END_DATE))
                .value(PageEntity.FIELD_END_TIME, bindMarker(PageEntity.FIELD_END_TIME))
                .value(PageEntity.FIELD_UPDATED, bindMarker(PageEntity.FIELD_UPDATED))
                .value(PageEntity.FIELD_REMOVED, bindMarker(PageEntity.FIELD_REMOVED))
                .build()
        );

        this.pageUpdateStatement = session.prepare(QueryBuilder.update(keyspaceId, pageTableId)
                .setColumn(PageEntity.FIELD_NAME, bindMarker(PageEntity.FIELD_NAME))
                .setColumn(PageEntity.FIELD_COMMENT, bindMarker(PageEntity.FIELD_COMMENT))
                .setColumn(PageEntity.FIELD_END_DATE, bindMarker(PageEntity.FIELD_END_DATE))
                .setColumn(PageEntity.FIELD_END_TIME, bindMarker(PageEntity.FIELD_END_TIME))
                .setColumn(PageEntity.FIELD_UPDATED, bindMarker(PageEntity.FIELD_UPDATED))
                .setColumn(PageEntity.FIELD_REMOVED, bindMarker(PageEntity.FIELD_REMOVED))
                .whereColumn(PageEntity.FIELD_BOOK).isEqualTo(bindMarker(PageEntity.FIELD_BOOK))
                .whereColumn(PageEntity.FIELD_START_DATE).isEqualTo(bindMarker(PageEntity.FIELD_START_DATE))
                .whereColumn(PageEntity.FIELD_START_TIME).isEqualTo(bindMarker(PageEntity.FIELD_START_TIME))
                .build()
        );

        this.pageNameInsertStatement = session.prepare(QueryBuilder.insertInto(keyspaceId, pageNamesTableId)
                .value(PageNameEntity.FIELD_BOOK, bindMarker(PageNameEntity.FIELD_BOOK))
                .value(PageNameEntity.FIELD_START_DATE, bindMarker(PageNameEntity.FIELD_START_DATE))
                .value(PageNameEntity.FIELD_START_TIME, bindMarker(PageNameEntity.FIELD_START_TIME))
                .value(PageNameEntity.FIELD_NAME, bindMarker(PageNameEntity.FIELD_NAME))
                .value(PageNameEntity.FIELD_COMMENT, bindMarker(PageNameEntity.FIELD_COMMENT))
                .value(PageNameEntity.FIELD_END_DATE, bindMarker(PageNameEntity.FIELD_END_DATE))
                .value(PageNameEntity.FIELD_END_TIME, bindMarker(PageNameEntity.FIELD_END_TIME))
                .build()
        );

        this.pageNameUpdateStatement = session.prepare(QueryBuilder.update(keyspaceId, pageNamesTableId)
                .setColumn(PageNameEntity.FIELD_START_DATE, bindMarker(PageNameEntity.FIELD_START_DATE))
                .setColumn(PageNameEntity.FIELD_START_TIME, bindMarker(PageNameEntity.FIELD_START_TIME))
                .setColumn(PageNameEntity.FIELD_COMMENT, bindMarker(PageNameEntity.FIELD_COMMENT))
                .setColumn(PageNameEntity.FIELD_END_DATE, bindMarker(PageNameEntity.FIELD_END_DATE))
                .setColumn(PageNameEntity.FIELD_END_TIME, bindMarker(PageNameEntity.FIELD_END_TIME))
                .whereColumn(PageNameEntity.FIELD_BOOK).isEqualTo(bindMarker(PageNameEntity.FIELD_BOOK))
                .whereColumn(PageNameEntity.FIELD_NAME).isEqualTo(bindMarker(PageNameEntity.FIELD_NAME))
                .build()
        );
    }

    private BoundStatement buildPageStatement(PreparedStatement statement, PageEntity page) {
        return BoundStatementBuilderWrapper.builder(statement)
                .setString(PageEntity.FIELD_BOOK, page.getBook())
                .setLocalDate(PageEntity.FIELD_START_DATE, page.getStartDate())
                .setLocalTime(PageEntity.FIELD_START_TIME, page.getStartTime())
                .setString(PageEntity.FIELD_NAME, page.getName())
                .setString(PageEntity.FIELD_COMMENT, page.getComment())
                .setLocalDate(PageEntity.FIELD_END_DATE, page.getEndDate())
                .setLocalTime(PageEntity.FIELD_END_TIME, page.getEndTime())
                .setInstant(PageEntity.FIELD_UPDATED, page.getUpdated())
                .setInstant(PageEntity.FIELD_REMOVED, page.getRemoved())
                .build();
    }

    private BoundStatement buildPageNameStatement(PreparedStatement statement, PageEntity page) {
        return BoundStatementBuilderWrapper.builder(statement)
                .setString(PageNameEntity.FIELD_BOOK, page.getBook())
                .setString(PageNameEntity.FIELD_NAME, page.getName())
                .setLocalDate(PageNameEntity.FIELD_START_DATE, page.getStartDate())
                .setLocalTime(PageNameEntity.FIELD_START_TIME, page.getStartTime())
                .setString(PageNameEntity.FIELD_COMMENT, page.getComment())
                .setLocalDate(PageNameEntity.FIELD_END_DATE, page.getEndDate())
                .setLocalTime(PageNameEntity.FIELD_END_TIME, page.getEndTime())
                .build();
    }

    public ResultSet addPages(Collection<PageEntity> pages, PageEntity lastPage, Function<BatchStatementBuilder, BatchStatementBuilder> attributes) {
        BatchStatementBuilder batchBuilder = BatchStatement.builder(DefaultBatchType.LOGGED);

        for (PageEntity page: pages) batchBuilder.addStatements(
                buildPageStatement(pageInsertStatement, page),
                buildPageNameStatement(pageNameInsertStatement, page)
        );

        if (lastPage != null) batchBuilder.addStatements(
                buildPageStatement(pageUpdateStatement, lastPage),
                buildPageNameStatement(pageNameUpdateStatement, lastPage)
        );

        return session.execute(attributes.apply(batchBuilder).build());
    }
}