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

import java.time.Instant;

/**
 * Builder for {@link Interval} object. After calling {@link #build()} method, the builder can be reused to build new test event
 */
public class IntervalBuilder
{
    private Interval interval;

    protected Interval createInterval() { return new Interval(); }

    private void initIfNeeded()
    {
        if (interval == null)
            interval = createInterval();
    }

    public IntervalBuilder id(String id)
    {
        initIfNeeded();
        interval.setId(id);
        return this;
    }

    public IntervalBuilder startTime(Instant startTime)
    {
        initIfNeeded();
        interval.setStartTime(startTime);
        return this;
    }

    public IntervalBuilder endTime(Instant endTime)
    {
        initIfNeeded();
        interval.setEndTime(endTime);
        return this;
    }

    public IntervalBuilder recoveryState(RecoveryState recoveryState)
    {
        initIfNeeded();
        interval.setRecoveryState(recoveryState);
        return this;
    }

    public IntervalBuilder lastUpdateTime(Instant lastUpdateTime)
    {
        initIfNeeded();
        interval.setLastUpdateDateTime(lastUpdateTime);
        return this;
    }

    public IntervalBuilder crawlerName(String crawlerName)
    {
        initIfNeeded();
        interval.setCrawlerName(crawlerName);
        return this;
    }

    public IntervalBuilder crawlerVersion(String crawlerVersion)
    {
        initIfNeeded();
        interval.setCrawlerVersion(crawlerVersion);
        return this;
    }

    public IntervalBuilder crawlerType(String crawlerType)
    {
        initIfNeeded();
        interval.setCrawlerType(crawlerType);
        return this;
    }

    public IntervalBuilder processed(boolean processed)
    {
        initIfNeeded();
        interval.setProcessed(processed);
        return this;
    }

    public Interval build()
    {
        initIfNeeded();
        Interval result = interval;
        interval = null;
        return result;
    }
}
