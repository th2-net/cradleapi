/*
 *  Copyright 2023-2024 Exactpro (Exactpro Systems Limited)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.exactpro.cradle.cassandra.resultset;

import com.exactpro.cradle.BookId;
import com.exactpro.cradle.PageId;
import com.exactpro.cradle.PageInfo;
import org.testng.annotations.Test;

import java.time.Instant;

import static com.exactpro.cradle.cassandra.resultset.PagesInIntervalIteratorProvider.checkInterval;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class PagesInIntervalIteratorProviderTest {

    private final BookId bookId = new BookId("test-book");

    @Test
    public void defaultPageVsIncludedInterval() {
        PageInfo pageInfo = new PageInfo(new PageId(bookId, Instant.EPOCH, "test-page"), null, null);

        assertTrue(checkInterval(pageInfo, pageInfo.getId().getStart(), Instant.now()), "in.st = p.st, in.en = p.en");
        assertTrue(checkInterval(pageInfo, pageInfo.getId().getStart().plusNanos(1), Instant.now()), "in.st = p.st + 1, in.en = p.en");
        assertTrue(checkInterval(pageInfo, pageInfo.getId().getStart(), pageInfo.getId().getStart()), "in.st = p.st, in.en = p.st");
        assertTrue(checkInterval(pageInfo, pageInfo.getId().getStart().minusNanos(1), pageInfo.getId().getStart()), "in.st = p.st - 1, in.en = p.st");
        assertTrue(checkInterval(pageInfo, Instant.now(), Instant.now()), "in.st = p.en, in.en = p.en");
        assertFalse(checkInterval(pageInfo, pageInfo.getId().getStart().minusNanos(1), pageInfo.getId().getStart().minusNanos(1)), "in.st = p.st - 1, in.en = p.st - 1");
    }

    @Test
    public void onePageVsIntervals() {
        Instant now = Instant.now();
        PageInfo pageInfo = new PageInfo(new PageId(bookId, now, "test-page"), now.plusSeconds(1), null);

        assertTrue(checkInterval(pageInfo, pageInfo.getId().getStart(), pageInfo.getEnded()), "in.st = p.st, in.en = p.en");
        assertTrue(checkInterval(pageInfo, pageInfo.getId().getStart().plusNanos(1), pageInfo.getEnded()), "in.st = p.st + 1, in.en = p.en");
        assertTrue(checkInterval(pageInfo, pageInfo.getId().getStart(), pageInfo.getEnded().minusNanos(1)), "in.st = p.st, in.en = p.en - 1");
        assertTrue(checkInterval(pageInfo, pageInfo.getId().getStart().plusNanos(1), pageInfo.getEnded().minusNanos(1)), "in.st = p.st + 1, in.en = p.en - 1");
        assertTrue(checkInterval(pageInfo, pageInfo.getId().getStart(), pageInfo.getId().getStart()), "in.st = p.st, in.en = p.st");
        assertTrue(checkInterval(pageInfo, pageInfo.getId().getStart().minusNanos(1), pageInfo.getId().getStart()), "in.st = p.st - 1, in.en = p.st");
        assertTrue(checkInterval(pageInfo, pageInfo.getEnded(), pageInfo.getEnded()), "in.st = p.en, in.en = p.en");
        assertTrue(checkInterval(pageInfo, pageInfo.getEnded(), pageInfo.getEnded().plusNanos(1)), "in.st = p.en, in.en = p.en + 1");
        assertFalse(checkInterval(pageInfo, pageInfo.getId().getStart().minusNanos(1), pageInfo.getId().getStart().minusNanos(1)), "in.st = p.st - 1, in.en = p.st - 1");
        assertFalse(checkInterval(pageInfo, pageInfo.getEnded().plusNanos(1), pageInfo.getEnded().plusNanos(1)), "in.st = p.en + 1, in.en = p.en + 1");
    }

}