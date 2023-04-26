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

package com.exactpro.cradle.cassandra.dao.messages;

import com.exactpro.cradle.BookId;
import com.exactpro.cradle.CoreStorageSettings;
import com.exactpro.cradle.Direction;
import com.exactpro.cradle.PageId;
import com.exactpro.cradle.cassandra.dao.SerializedEntity;
import com.exactpro.cradle.cassandra.utils.MessageBatchEntityUtils;
import com.exactpro.cradle.messages.MessageBatchToStore;
import com.exactpro.cradle.messages.MessageToStore;
import com.exactpro.cradle.messages.StoredMessageBatch;
import com.exactpro.cradle.utils.CompressException;
import com.exactpro.cradle.utils.CompressionType;
import com.exactpro.cradle.utils.CradleStorageException;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.recursive.comparison.RecursiveComparisonConfiguration;
import org.testng.annotations.Test;

import java.io.IOException;
import java.time.Instant;
import java.util.zip.DataFormatException;

import static com.exactpro.cradle.cassandra.TestUtils.createContent;

public class MessageBatchEntityTest {
    @Test
    public void messageEntity() throws IOException, DataFormatException, CradleStorageException, CompressException {
        PageId pageId = new PageId(new BookId("Test_Book_1"), "Test_Page_1");
        MessageBatchToStore batch = MessageBatchToStore.singleton(MessageToStore.builder()
                .bookId(pageId.getBookId())
                .sessionAlias("TEST_Session")
                .direction(Direction.FIRST)
                .timestamp(Instant.now())
                .sequence(1)
                .content(createContent(40))
                .metadata("key_test", "value_test")
                .build(), 200, new CoreStorageSettings().calculateStoreActionRejectionThreshold());
        StoredMessageBatch storedBatch = new StoredMessageBatch(batch.getMessages(), pageId, null);

        SerializedEntity<MessageBatchEntity> serializedEntity = MessageBatchEntityUtils.toSerializedEntity(batch, pageId, CompressionType.ZLIB, 2000);
        StoredMessageBatch batchFromEntity = MessageBatchEntityUtils.toStoredMessageBatch(serializedEntity.getEntity(), pageId);

        RecursiveComparisonConfiguration config = new RecursiveComparisonConfiguration();

        Assertions.assertThat(storedBatch).usingRecursiveComparison(config).isEqualTo(batchFromEntity);
    }
}