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

package com.exactpro.cradle.cassandra.integration;

import com.exactpro.cradle.cassandra.CassandraCradleStorage;
import com.exactpro.cradle.cassandra.TestUtils;
import com.exactpro.cradle.cassandra.dao.testevents.TestEventEntity;
import com.exactpro.cradle.cassandra.dao.testevents.PageScopeEntity;
import com.exactpro.cradle.cassandra.dao.testevents.ScopeEntity;
import com.exactpro.cradle.cassandra.dao.messages.MessageBatchEntity;
import com.exactpro.cradle.cassandra.dao.messages.GroupedMessageBatchEntity;
import com.exactpro.cradle.cassandra.dao.messages.PageGroupEntity;
import com.exactpro.cradle.cassandra.dao.messages.PageSessionEntity;
import com.exactpro.cradle.cassandra.dao.messages.SessionEntity;
import com.exactpro.cradle.cassandra.dao.messages.GroupEntity;
import com.exactpro.cradle.cassandra.dao.books.BookEntity;
import com.exactpro.cradle.cassandra.dao.books.PageEntity;
import com.exactpro.cradle.cassandra.dao.books.PageNameEntity;
import com.exactpro.cradle.cassandra.dao.intervals.IntervalEntity;
import com.exactpro.cradle.cassandra.dao.labels.LabelEntity;
import com.exactpro.cradle.utils.CradleStorageException;
import org.testng.IInvokedMethod;
import org.testng.IInvokedMethodListener;
import org.testng.ITestResult;

public class TombstoneCounterListener implements IInvokedMethodListener {
    private final CassandraCradleStorage storage = CassandraCradleHelper.getInstance().getStorage();
    private final String[] tables = new String[] {
            TestEventEntity.TABLE_NAME,
            MessageBatchEntity.TABLE_NAME,
            GroupedMessageBatchEntity.TABLE_NAME,
            BookEntity.TABLE_NAME,
            PageEntity.TABLE_NAME,
            PageNameEntity.TABLE_NAME,
            PageGroupEntity.TABLE_NAME,
            PageScopeEntity.TABLE_NAME,
            PageSessionEntity.TABLE_NAME,
            ScopeEntity.TABLE_NAME,
            SessionEntity.TABLE_NAME,
            GroupEntity.TABLE_NAME,
            IntervalEntity.TABLE_NAME,
            LabelEntity.TABLE_NAME
    };
    private final long[] tombstonesAtStart = new long[tables.length];

    @Override
    public void beforeInvocation(IInvokedMethod method, ITestResult testResult) {
        if (isCountingNeeded(method)) {
            try {
                for (int idx = 0; idx < tables.length; idx++) {
                    tombstonesAtStart[idx] = TestUtils.countTombstones(storage, tables[idx]);
                }
            } catch (CradleStorageException e) {
                failTest(testResult, e);
            }
        }
    }

    @Override
    public void afterInvocation(IInvokedMethod method, ITestResult testResult) {
        if (isCountingNeeded(method)) {
            try {
                for (int idx = 0; idx < tables.length; idx++) {
                    long tombstonesAtEnd = TestUtils.countTombstones(storage, tables[idx]);
                    long newTombstones = tombstonesAtEnd - tombstonesAtStart[idx];
                    if (newTombstones != 0) {
                        Exception exception = new RuntimeException(
                                newTombstones + " new tombstones was created in '" + tables[idx] + "' table"
                        );
                        failTest(testResult, exception);
                        break;
                    }
                }
            } catch (CradleStorageException e) {
                testResult.setStatus(ITestResult.FAILURE);
            }
        }
    }

    // we only count tombstones created in methods marked with @BeforeClass annotation because new records created there
    // test methods itself does not produce new records
    private boolean isCountingNeeded(IInvokedMethod method) {
        return method.getTestMethod().isBeforeClassConfiguration();
    }

    private void failTest(ITestResult testResult, Throwable e) {
        testResult.setThrowable(e);
        testResult.setStatus(ITestResult.FAILURE);
    }
}