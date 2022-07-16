package com.exactpro.cradle.cassandra.dao.testevents;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.mapper.MapperContext;
import com.datastax.oss.driver.api.mapper.entity.EntityHelper;
import com.exactpro.cradle.Order;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


import static org.mockito.Mockito.*;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;


public class TestEventQueryProviderTest {

    private AbstractTestEventQueryProvider<TestEventEntity> queryProvider;
    private MapperContext mapperContext;
    private CqlSession session;
    private EntityHelper<TestEventEntity> entityHelper;


    @BeforeMethod
    public void prepare() {
        session = mock(CqlSession.class);
        when(session.prepare(any(SimpleStatement.class))).thenAnswer(invocation -> {
            SimpleStatement simpleStatement = (SimpleStatement) invocation.getArguments()[0];
            PreparedStatement preparedStatement = mock(PreparedStatement.class);
            when(preparedStatement.getQuery()).thenReturn(simpleStatement.getQuery());
            return preparedStatement;
        });


        mapperContext = mock(MapperContext.class);
        when(mapperContext.getSession()).thenReturn(session);

        entityHelper = mock(EntityHelper.class);
        when(entityHelper.getKeyspaceId()).thenReturn(CqlIdentifier.fromCql("\"keyspace\""));
        when(entityHelper.getTableId()).thenReturn(CqlIdentifier.fromCql("\"table\""));

        queryProvider = new AbstractTestEventQueryProvider<>(mapperContext, entityHelper) {
            @Override
            public PreparedStatement getPreparedStatement(boolean includeContent, String idFrom, String parentId, Order order) {
                return super.getPreparedStatement(includeContent, idFrom, parentId, order);
            }
        };
    }

    @Test
    void cachingWithNulls(){
        PreparedStatement preparedStatement1 = queryProvider.getPreparedStatement(false, null, null, null);
        verify(session, times(1)).prepare(any(SimpleStatement.class));

        PreparedStatement preparedStatement2 = queryProvider.getPreparedStatement(false, null, null, null);
        verify(session, times(1)).prepare(any(SimpleStatement.class));

        assertEquals(preparedStatement1, preparedStatement2);
    }

    @Test
    void cachingJustIncludeContent(){
        PreparedStatement preparedStatement1 = queryProvider.getPreparedStatement(true, null, null, null);
        verify(session, times(1)).prepare(any(SimpleStatement.class));

        PreparedStatement preparedStatement2 = queryProvider.getPreparedStatement(true, null, null, null);
        verify(session, times(1)).prepare(any(SimpleStatement.class));

        assertEquals(preparedStatement1, preparedStatement2);
    }

    @Test
    void cachingWithDifferentIdFroms(){
        PreparedStatement preparedStatement1 = queryProvider.getPreparedStatement(true, "idFrom1", null, null);
        verify(session, times(1)).prepare(any(SimpleStatement.class));

        PreparedStatement preparedStatement2 = queryProvider.getPreparedStatement(true, "idFrom2", null, null);
        verify(session, times(1)).prepare(any(SimpleStatement.class));

        assertEquals(preparedStatement1, preparedStatement2);
    }

    @Test
    void cachingWithDifferentNullArguements(){
        PreparedStatement preparedStatement1 = queryProvider.getPreparedStatement(true, "idFrom", "parentId", null);
        verify(session, times(1)).prepare(any(SimpleStatement.class));

        PreparedStatement preparedStatement2 = queryProvider.getPreparedStatement(true, "idFrom", null, null);
        verify(session, times(2)).prepare(any(SimpleStatement.class));

        assertNotEquals(preparedStatement1, preparedStatement2);
    }

}
