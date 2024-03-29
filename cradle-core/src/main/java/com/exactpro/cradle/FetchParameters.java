/*
 * Copyright 2022-2022 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.cradle;

public class FetchParameters {
    private int fetchSize;
    private long timeout;

    public FetchParameters()
    {
    }

    public FetchParameters(int fetchSize, long timeout)
    {
        this.fetchSize = fetchSize;
        this.timeout = timeout;
    }

    public int getFetchSize()
    {
        return fetchSize;
    }

    public void setFetchSize(int fetchSize)
    {
        this.fetchSize = fetchSize;
    }

    public long getTimeout()
    {
        return timeout;
    }

    public void setTimeout(long timeout)
    {
        this.timeout = timeout;
    }
}
