package com.exactpro.cradle.cassandra.dao.testevents;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.mapper.MapperContext;
import com.datastax.oss.driver.api.mapper.entity.EntityHelper;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.exactpro.cradle.Order;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.UUID;

import static org.mockito.Mockito.*;
import static org.testng.Assert.assertEquals;


public class TestEventQueryProviderTest {

    private AbstractTestEventQueryProvider<TestEventEntity> queryProvider;
    private MapperContext mapperContext;
    private CqlSession session;
    private EntityHelper<TestEventEntity> entityHelper;


    @BeforeClass
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
            protected Select getSelect(boolean includeContent, String idFrom, String parentId, Order order) {
                return super.getSelect(includeContent, idFrom, parentId, order);
            }
        };
    }

    @Test
    void cachingTest(){
        Select select1 = queryProvider.getSelect(true, null, null, null);
        Select select2 = queryProvider.getSelect(true, null, null, null);
        Select select3 = queryProvider.getSelect(true, UUID.randomUUID().toString(), null, null);

        assertEquals(select1, select2);

        PreparedStatement preparedStatement1 = queryProvider.getPreparedStatement(select1);
        verify(session, times(1)).prepare(any(SimpleStatement.class));

        PreparedStatement preparedStatement2 = queryProvider.getPreparedStatement(select2);
        verify(session, times(1)).prepare(any(SimpleStatement.class));
        assertEquals(preparedStatement1, preparedStatement2);

        PreparedStatement preparedStatement3 = queryProvider.getPreparedStatement(select3);
        verify(session, times(2)).prepare(any(SimpleStatement.class));
    }

}
