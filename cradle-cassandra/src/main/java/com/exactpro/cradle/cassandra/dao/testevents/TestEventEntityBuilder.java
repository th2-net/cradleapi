package com.exactpro.cradle.cassandra.dao.testevents;

import com.datastax.oss.driver.api.mapper.annotations.Transient;
import com.exactpro.cradle.PageId;
import com.exactpro.cradle.serialization.SerializedEntityData;
import com.exactpro.cradle.serialization.SerializedEntityMetadata;
import com.exactpro.cradle.testevents.StoredTestEventId;
import com.exactpro.cradle.testevents.TestEventToStore;
import com.exactpro.cradle.utils.CompressionUtils;
import com.exactpro.cradle.utils.TestEventUtils;
import com.exactpro.cradle.utils.TimeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.List;

public class TestEventEntityBuilder {

    private static final Logger logger = LoggerFactory.getLogger(TestEventEntityBuilder.class);
    
    private TestEventEntity entity;
    public  TestEventEntityBuilder () {
        this.entity = new TestEventEntity();
    }



    public TestEventEntityBuilder setBook(String book) {
        entity.setBook(book);
        return this;
    }

    public TestEventEntityBuilder setPage(String page) {
        entity.setPage(page);
        return this;
    }

    public TestEventEntityBuilder setScope(String scope) {
        entity.setScope(scope);
        return this;
    }

    public TestEventEntityBuilder setStartDate(LocalDate startDate) {
        entity.setStartDate(startDate);
        return this;
    }

    public TestEventEntityBuilder setStartTime(LocalTime startTime) {
        entity.setStartTime(startTime);
        return this;
    }

    public TestEventEntityBuilder setId(String id) {
        entity.setId(id);
        return this;
    }

    public TestEventEntityBuilder setName(String name) {
        entity.setName(name);
        return this;
    }

    public TestEventEntityBuilder setType(String type) {
        entity.setType(type);
        return this;
    }

    public TestEventEntityBuilder setSuccess(boolean success) {
        entity.setSuccess(success);
        return this;
    }

    public TestEventEntityBuilder setRoot(boolean root) {
        entity.setRoot(root);
        return this;
    }

    public TestEventEntityBuilder setParentId(String parentId) {
        entity.setParentId(parentId);
        return this;
    }

    public TestEventEntityBuilder setEventBatch(boolean eventBatch) {
        entity.setEventBatch(eventBatch);
        return this;
    }

    public TestEventEntityBuilder setEventCount(int eventCount) {
        entity.setEventCount(eventCount);
        return this;
    }

    public TestEventEntityBuilder setEndDate(LocalDate endDate) {
        entity.setEndDate(endDate);
        return this;
    }

    public TestEventEntityBuilder setEndTime(LocalTime endTime) {
        entity.setEndTime(endTime);
        return this;
    }

    public TestEventEntityBuilder setMessages(ByteBuffer messages) {
        entity.setMessages(messages);
        return this;
    }

    public TestEventEntityBuilder setRecDate(Instant recDate) {
        entity.setRecDate(recDate);
        return this;
    }

    public TestEventEntityBuilder setSerializedEventMetadata(List<SerializedEntityMetadata> serializedEventMetadata) {
        entity.setSerializedEventMetadata(serializedEventMetadata);
        return this;
    }

    public TestEventEntity build () {
        TestEventEntity rtn = entity;
        entity = new TestEventEntity();
        return rtn;
    }

    public TestEventEntityBuilder setStartTimestamp(Instant timestamp)
    {
        if (timestamp != null)
            setStartTimestamp(TimeUtils.toLocalTimestamp(timestamp));
        else
        {
            setStartDate(null);
            setStartTime(null);
        }

        return this;
    }

    public TestEventEntityBuilder setStartTimestamp(LocalDateTime timestamp)
    {
        if (timestamp != null)
        {
            setStartDate(timestamp.toLocalDate());
            setStartTime(timestamp.toLocalTime());
        }
        else
        {
            setStartDate(null);
            setStartTime(null);
        }

        return this;
    }

    public void setEndTimestamp(Instant timestamp)
    {
        if (timestamp != null)
        {
            LocalDateTime ldt = TimeUtils.toLocalTimestamp(timestamp);
            setEndDate(ldt.toLocalDate());
            setEndTime(ldt.toLocalTime());
        }
        else
        {
            setEndDate(null);
            setEndTime(null);
        }
    }

    public TestEventEntityBuilder setCompressed (boolean compressed) {
        entity.setCompressed(compressed);

        return this;
    }

    public TestEventEntityBuilder setContent (ByteBuffer content) {
        entity.setContent(content);

        return this;
    }

    public TestEventEntityBuilder  fromEventToStore (TestEventToStore event, PageId pageId, int maxUncompressedSize) throws IOException {
        logger.debug("Creating entity from test event '{}'", event.getId());

        SerializedEntityData serializedEventData = TestEventUtils.getTestEventContent(event);

        byte[] content = serializedEventData.getSerializedData();
        boolean compressed;
        if (content != null && content.length > maxUncompressedSize)
        {
            logger.trace("Compressing content of test event '{}'", event.getId());
            content = CompressionUtils.compressData(content);
            compressed = true;
        }
        else
            compressed = false;

        byte[] messages = TestEventUtils.serializeLinkedMessageIds(event);

        StoredTestEventId parentId = event.getParentId();
        LocalDateTime start = TimeUtils.toLocalTimestamp(event.getStartTimestamp());

        setBook(pageId.getBookId().getName());
        setPage(pageId.getName());
        setScope(event.getScope());
        setStartTimestamp(start);
        setId(event.getId().getId());

        setSuccess(event.isSuccess());
        setRoot(parentId == null);
        setEventBatch(event.isBatch());
        setName(event.getName());
        setType(event.getType());
        setParentId(parentId != null ? parentId.toString() : "");  //Empty string for absent parentId allows using index to get root events
        if (event.isBatch())
            setEventCount(event.asBatch().getTestEventsCount());
        setEndTimestamp(event.getEndTimestamp());

        if (messages != null)
            setMessages(ByteBuffer.wrap(messages));

        setCompressed(compressed);
        //TODO: this.setLabels(event.getLabels());
        if (content != null)
            setContent(ByteBuffer.wrap(content));

        setSerializedEventMetadata(serializedEventData.getSerializedEntityMetadata());

        return this;
    }

    public static TestEventEntityBuilder builder () {
        return new TestEventEntityBuilder();
    }
}