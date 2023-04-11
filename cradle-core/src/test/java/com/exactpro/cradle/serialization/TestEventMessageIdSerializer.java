package com.exactpro.cradle.serialization;

import com.exactpro.cradle.Direction;
import com.exactpro.cradle.messages.StoredMessageId;
import com.exactpro.cradle.testevents.StoredTestEventId;
import org.testng.annotations.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;

public class TestEventMessageIdSerializer {

    @Test(
            expectedExceptions = IllegalArgumentException.class,
            expectedExceptionsMessageRegExp = "prohibited sequence -9223372036854775808 for direction FIRST"
    )
    public void testSerializeBatchLinkedMessageIds() throws IOException, NoSuchFieldException, IllegalAccessException {
        var messageId = new StoredMessageId("test", Direction.FIRST, 0);
        setNegativeIndex(messageId, Long.MIN_VALUE);

        EventMessageIdSerializer.serializeBatchLinkedMessageIds(Map.of(
                new StoredTestEventId("test"), List.of(messageId)
        ));
    }

    private static void setNegativeIndex(StoredMessageId messageId, long index) throws NoSuchFieldException, IllegalAccessException {
        // prepare illegal state for message ID
        Field indexField = messageId.getClass().getDeclaredField("index");
        indexField.setAccessible(true);
        indexField.set(messageId, index);
    }
}