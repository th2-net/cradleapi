/*
 * Copyright 2020-2021 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.cradle.intervals;

import com.exactpro.cradle.testevents.StoredTestEventBatch;
import com.exactpro.cradle.testevents.StoredTestEventId;
import com.exactpro.cradle.testevents.StoredTestEventWrapper;
import com.exactpro.cradle.testevents.TestEventBatchToStore;
import com.exactpro.cradle.utils.CradleStorageException;
import com.exactpro.cradle.utils.UpdateNotAppliedException;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class IntervalTest
{
    private static final String DUMMY_NAME = "TestCrawler";
    private static final String DUMMY_VERSION = "TestVersion";
    private static final String DUMMY_TYPE = "TestType";

    private StoredTestEventWrapper event;

    private IntervalBuilder builder;
    private List<Interval> intervals = new ArrayList<>();
    private IntervalsWorker intervalsWorkerMock;

    Instant from;
    Instant to;

    @BeforeClass
    public void prepare() throws IOException, CradleStorageException {

        builder = new IntervalBuilder();
        intervals = new ArrayList<>();
        intervalsWorkerMock = mock(IntervalsWorker.class);

        from = Instant.now();
        to = Instant.now().plus(1, ChronoUnit.MINUTES);

        event = new StoredTestEventWrapper(
                new StoredTestEventBatch
                        (TestEventBatchToStore.builder().id(
                                new StoredTestEventId("test_id")).name("test_name").type("test_type").parentId(
                                new StoredTestEventId("test_par_id")).build()));

        when(intervalsWorkerMock.getIntervals(any(Instant.class), any(Instant.class), anyString(), anyString(), anyString())).thenReturn(intervals);

        when(intervalsWorkerMock.updateRecoveryState(any(Interval.class), any(RecoveryState.class))).then(invocation -> {
            Interval interval = invocation.getArgument(0);
            RecoveryState state = invocation.getArgument(1);

            Interval storedInterval;

            Optional<Interval> optionalInterval = intervals.stream().filter(i -> i.getStartTime().equals(interval.getStartTime())
                    && i.getCrawlerName().equals(interval.getCrawlerName())
                    && i.getCrawlerVersion().equals(interval.getCrawlerVersion())
                    && i.getCrawlerType().equals(interval.getCrawlerType())).findAny();

            if (optionalInterval.isPresent()) {
                storedInterval = optionalInterval.get();

                if (storedInterval.getLastUpdateDateTime().equals(interval.getLastUpdateDateTime())) {
                    interval.setRecoveryState(state);
                    interval.setLastUpdateDateTime(Instant.now());

                    return interval;
                }
            }

            throw new UpdateNotAppliedException("Failed to updateRecoveryState");
        });

        when(intervalsWorkerMock.storeInterval(any(Interval.class))).then(invocation -> {
            Interval interval = invocation.getArgument(0);

            long res = intervals.stream().filter(i -> i.getStartTime().equals(interval.getStartTime())
                    && i.getCrawlerName().equals(interval.getCrawlerName())
                    && i.getCrawlerVersion().equals(interval.getCrawlerVersion())
                    && i.getCrawlerType().equals(interval.getCrawlerType())).count();

            if (res == 0) {
                intervals.add(interval);
                interval.setLastUpdateDateTime(Instant.now());
                return true;
            }

            return false;
        });

        when(intervalsWorkerMock.setIntervalProcessed(any(Interval.class), anyBoolean())).then(invocation -> {
            Interval interval = invocation.getArgument(0);
            int index = intervals.indexOf(interval);

            intervals.get(index).setProcessed(invocation.getArgument(1));
            interval.setLastUpdateDateTime(Instant.now());

            return interval;
        });
    }

    @Test(expectedExceptions = {IllegalArgumentException.class}, expectedExceptionsMessageRegExp = "Start time of interval cannot be after end time")
    public void intervalWithWrongStartTime()
    {
        Interval interval = builder
                .startTime(to)
                .endTime(from)
                .processed(false)
                .recoveryState(null)
                .lastUpdateTime(Instant.now())
                .crawlerName(DUMMY_NAME)
                .crawlerVersion(DUMMY_VERSION)
                .crawlerType(DUMMY_TYPE)
                .build();
    }

    @Test(expectedExceptions = {IllegalArgumentException.class}, expectedExceptionsMessageRegExp = "Time of last update of interval cannot be before start time")
    public void intervalWithWrongLastUpdateTime()
    {
        Interval interval = builder
                .startTime(from)
                .endTime(to)
                .processed(false)
                .recoveryState(null)
                .lastUpdateTime(from.minus(1, ChronoUnit.MINUTES))
                .crawlerName(DUMMY_NAME)
                .crawlerVersion(DUMMY_VERSION)
                .crawlerType(DUMMY_TYPE)
                .build();
    }

    @Test
    public void forgotProcessedField()
    {
        Interval interval = builder
                .startTime(from)
                .endTime(to)
                .recoveryState(null)
                .lastUpdateTime(Instant.now())
                .crawlerName(DUMMY_NAME)
                .crawlerVersion(DUMMY_VERSION)
                .crawlerType(DUMMY_TYPE)
                .build();

        Assert.assertFalse(interval.isProcessed());
    }

    @Test
    public void storeInterval() throws IOException
    {
        Interval interval = builder
                .startTime(from)
                .endTime(to)
                .processed(false)
                .recoveryState(new RecoveryState(new RecoveryState.InnerEvent(event), null, 10, 10))
                .lastUpdateTime(from)
                .crawlerName(DUMMY_NAME)
                .crawlerVersion(DUMMY_VERSION)
                .crawlerType(DUMMY_TYPE)
                .build();

        intervalsWorkerMock.storeInterval(interval);

        Iterable<Interval> intervals = intervalsWorkerMock.getIntervals(from, to, DUMMY_NAME, DUMMY_VERSION, DUMMY_TYPE);

        List<Interval> list = StreamSupport.stream(intervals.spliterator(), false).collect(Collectors.toList());

        Assert.assertEquals(list.get(0), interval);
    }
}
