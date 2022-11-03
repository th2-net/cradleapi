package com.exactpro.cradle.cassandra.dao.messages;

import com.exactpro.cradle.BookId;
import com.exactpro.cradle.Direction;
import com.exactpro.cradle.PageId;
import com.exactpro.cradle.cassandra.dao.SerializedEntity;
import com.exactpro.cradle.cassandra.utils.GroupedMessageEntityUtils;
import com.exactpro.cradle.messages.*;
import com.exactpro.cradle.utils.CradleStorageException;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.recursive.comparison.RecursiveComparisonConfiguration;
import org.testng.annotations.Test;

import java.io.IOException;
import java.time.Instant;
import java.util.zip.DataFormatException;

import static com.exactpro.cradle.cassandra.TestUtils.createContent;

public class GroupedMessageBatchEntityTest {
    @Test
    public void validationTest() throws IOException, DataFormatException, CradleStorageException
    {
        PageId pageId = new PageId(new BookId("Test_Book_1"), "Test_Page_1");
        String group = "test-group";

        GroupedMessageBatchToStore batch = new GroupedMessageBatchToStore(group, 10_000);
        MessageToStore message = MessageToStore.builder()
                .bookId(pageId.getBookId())
                .sessionAlias("TEST_Session")
                .direction(Direction.FIRST)
                .timestamp(Instant.parse("2022-06-10T23:59:58.987Z"))
                .sequence(1)
                .content(createContent(40))
                .metadata("key_test", "value_test")
                .build();
        batch.addMessage(message);

        SerializedEntity<GroupedMessageBatchEntity> serializedEntity = GroupedMessageEntityUtils.toSerializedEntity(batch, pageId, 10_000);

        StoredGroupedMessageBatch storedBatch = new StoredGroupedMessageBatch(group, batch.getMessages(), pageId, null);
        StoredGroupedMessageBatch batchFromEntity = GroupedMessageEntityUtils.toStoredGroupedMessageBatch(serializedEntity.getEntity(), pageId);
        RecursiveComparisonConfiguration config = new RecursiveComparisonConfiguration();

        Assertions.assertThat(storedBatch).usingRecursiveComparison(config).isEqualTo(batchFromEntity);
    }
}