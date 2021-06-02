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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;

public class RecoveryState
{
    private final Instant timeOfStop;

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final Logger logger = LoggerFactory.getLogger(RecoveryState.class);

    public RecoveryState(@JsonProperty("timeOfStop") Instant timeOfStop) {
        this.timeOfStop = timeOfStop;
    }

    public Instant getTimeOfStop() { return timeOfStop; }

    public String convertToJson()
    {
        try
        {
            return MAPPER.writeValueAsString(this);
        } catch (JsonProcessingException e) {
            logger.error("Failed to convert recovery state with time of stop "+timeOfStop+" to JSON", e);
        }
        return null;
    }

    public static ObjectMapper getMAPPER() { return MAPPER; }

    @Override
    public String toString()
    {
        return new StringBuilder()
                .append("RecoveryState{")
                .append("timeOfStop=").append(timeOfStop)
                .append("}").toString();
    }
}
