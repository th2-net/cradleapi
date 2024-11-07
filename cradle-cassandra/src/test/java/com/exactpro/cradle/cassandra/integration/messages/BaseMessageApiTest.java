/*
 * Copyright 2023 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.cradle.cassandra.integration.messages;

import com.exactpro.cradle.CoreStorageSettings;
import com.exactpro.cradle.Direction;
import com.exactpro.cradle.cassandra.integration.BaseCradleCassandraTest;
import com.exactpro.cradle.messages.GroupedMessageBatchToStore;
import com.exactpro.cradle.utils.CradleStorageException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Set;

public class BaseMessageApiTest extends BaseCradleCassandraTest {
    private static final Logger logger = LoggerFactory.getLogger(BaseMessageApiTest.class);

    protected static final String GROUP1_NAME = "test_group1";

    protected static final String GROUP2_NAME = "test_group2";

    protected static final String GROUP3_NAME = "test_group3";

    protected static final String GROUP4_NAME = "test_group4";

    protected static final String SESSION_ALIAS1 = "test_session_alias1";

    protected static final String SESSION_ALIAS2 = "test_session_alias2";

    protected static final String SESSION_ALIAS3 = "test_session_alias3";

    protected static final String SESSION_ALIAS4 = "test_session_alias4";

    protected static final String SESSION_ALIAS5 = "test_session_alias5";

    protected static final String SESSION_ALIAS6 = "test_session_alias6";

    protected static final Set<String> allSessions = Set.of(
            SESSION_ALIAS1,
            SESSION_ALIAS2,
            SESSION_ALIAS3,
            SESSION_ALIAS4,
            SESSION_ALIAS5,
            SESSION_ALIAS6
    );

    protected static final Set<String> allGroups = Set.of(
            GROUP1_NAME,
            GROUP2_NAME,
            GROUP3_NAME,
            GROUP4_NAME
    );

    protected final long storeActionRejectionThreshold = new CoreStorageSettings().calculateStoreActionRejectionThreshold();

    @Override
    protected void generateData() {
        try {
            GroupedMessageBatchToStore b1 = new GroupedMessageBatchToStore(GROUP1_NAME, 1024, storeActionRejectionThreshold);
            // page 0 contains 3 messages from batch 1 and 1 message from batch 2.
            // contains 2 groups test_group1 and test_group2
            // contains 2 session aliases test_session_alias1 and test_session_alias2
            b1.addMessage(generateMessage(SESSION_ALIAS1, Direction.FIRST, 2, 1L));
            b1.addMessage(generateMessage(SESSION_ALIAS1, Direction.SECOND, 3, 2L));
            b1.addMessage(generateMessage(SESSION_ALIAS2, Direction.SECOND, 6, 3L));

            GroupedMessageBatchToStore b2 = new GroupedMessageBatchToStore(GROUP2_NAME, 1024, storeActionRejectionThreshold);
            b2.addMessage(generateMessage(SESSION_ALIAS2, Direction.FIRST, 5, 4L));

            // page 1 contains 2 messages from batch 2.
            // contains 1 group test_group2
            // contains 2 session aliases test_session_alias3 and test_session_alias4
            b2.addMessage(generateMessage(SESSION_ALIAS3, Direction.SECOND, 18, 5L));
            b2.addMessage(generateMessage(SESSION_ALIAS4, Direction.SECOND, 19, 6L));

            // page 2 contains 4 messages from batch 3.
            // contains 1 group test_group3
            // contains 3 session aliases test_session_alias4, test_session_alias5 and test_session_alias6
            GroupedMessageBatchToStore b3 = new GroupedMessageBatchToStore(GROUP3_NAME, 1024, storeActionRejectionThreshold);
            b3.addMessage(generateMessage(SESSION_ALIAS4, Direction.FIRST, 25, 7L));
            b3.addMessage(generateMessage(SESSION_ALIAS5, Direction.SECOND, 26, 8L));
            b3.addMessage(generateMessage(SESSION_ALIAS5, Direction.SECOND, 28, 9L));
            b3.addMessage(generateMessage(SESSION_ALIAS6, Direction.SECOND, 29, 10L));

            // page 3 contains 2 messages from batch 4.
            // contains 1 group test_group4
            // contains 1 session alias test_session_alias6
            GroupedMessageBatchToStore b4 = new GroupedMessageBatchToStore(GROUP4_NAME, 1024, storeActionRejectionThreshold);
            b4.addMessage(generateMessage(SESSION_ALIAS6, Direction.FIRST, 35, 11L));
            b4.addMessage(generateMessage(SESSION_ALIAS6, Direction.SECOND, 37, 12L));

            // page 4 contains 1 message from batch 5.
            // contains 1 group test_group3
            // contains 1 session alias test_session_alias6
            GroupedMessageBatchToStore b5 = new GroupedMessageBatchToStore(GROUP3_NAME, 1024, storeActionRejectionThreshold);
            b5.addMessage(generateMessage(SESSION_ALIAS6, Direction.FIRST, 45, 13L));

            // page 5 contains 1 message from batch 5 and 2 messages from batch 6.
            // contains 1 group test_group3
            // contains 1 session alias test_session_alias6
            b5.addMessage(generateMessage(SESSION_ALIAS6, Direction.SECOND, 52, 14L));

            GroupedMessageBatchToStore b6 = new GroupedMessageBatchToStore(GROUP3_NAME, 1024, storeActionRejectionThreshold);
            b6.addMessage(generateMessage(SESSION_ALIAS6, Direction.FIRST, 53, 15L));
            b6.addMessage(generateMessage(SESSION_ALIAS6, Direction.SECOND, 55, 16L));

            GroupedMessageBatchToStore b7 = new GroupedMessageBatchToStore(GROUP3_NAME, 1024, storeActionRejectionThreshold);
            b7.addMessage(generateMessage(SESSION_ALIAS6, Direction.FIRST, 55, 17L));
            b7.addMessage(generateMessage(SESSION_ALIAS6, Direction.SECOND, 55, 18L));

            GroupedMessageBatchToStore b8 = new GroupedMessageBatchToStore(GROUP3_NAME, 1024, storeActionRejectionThreshold);
            b8.addMessage(generateMessage(SESSION_ALIAS6, Direction.FIRST, 56, 18L));
            b8.addMessage(generateMessage(SESSION_ALIAS6, Direction.FIRST, 58, 19L));
            b8.addMessage(generateMessage(SESSION_ALIAS6, Direction.SECOND, 59, 20L));

            GroupedMessageBatchToStore b9 = new GroupedMessageBatchToStore(GROUP3_NAME, 1024, storeActionRejectionThreshold);
            b9.addMessage(generateMessage(SESSION_ALIAS6, Direction.FIRST, 60, 21L));

            List<GroupedMessageBatchToStore> data = List.of(b1, b2, b3, b4, b5, b6, b7, b8, b9);
            for (GroupedMessageBatchToStore el : data) {
                storage.storeGroupedMessageBatch(el);
            }

        } catch (CradleStorageException | IOException e) {
            logger.error(e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }
}
