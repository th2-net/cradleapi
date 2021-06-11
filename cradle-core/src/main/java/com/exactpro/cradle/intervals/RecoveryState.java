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

import com.exactpro.cradle.Direction;
import com.exactpro.cradle.messages.StoredMessage;
import com.exactpro.cradle.testevents.StoredTestEventWrapper;
import com.exactpro.cradle.utils.CompressionUtils;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.List;
import java.util.StringJoiner;

public class RecoveryState
{
    private final InnerEvent lastProcessedEvent;

    private final List<InnerMessage> lastProcessedMessages;

    private static final ObjectMapper MAPPER = new ObjectMapper().findAndRegisterModules()
            .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);

    private static final Logger logger = LoggerFactory.getLogger(RecoveryState.class);

    public RecoveryState(@JsonProperty("lastProcessedEvent") InnerEvent lastProcessedEvent,
                         @JsonProperty("lastProcessedMessages") List<InnerMessage> lastProcessedMessages)
    {
        this.lastProcessedEvent = lastProcessedEvent;
        this.lastProcessedMessages = lastProcessedMessages;
    }

    public InnerEvent getLastProcessedEvent() { return lastProcessedEvent; }

    public List<InnerMessage> getLastProcessedMessages() { return lastProcessedMessages; }

    public String convertToJson()
    {
        try
        {
            return MAPPER.writeValueAsString(this);
        } catch (JsonProcessingException e) {
            logger.error("Failed to convert recovery state to JSON", e);
        }
        return null;
    }

    public static ObjectMapper getMAPPER() { return MAPPER; }

    @Override
    public String toString()
    {
        StringJoiner joiner = new StringJoiner(", ");

        lastProcessedMessages.forEach(m -> joiner.add(m.toString()));

        return new StringBuilder()
                .append("RecoveryState{")
                .append("lastProcessedEvent=").append(lastProcessedEvent).append(CompressionUtils.EOL)
                .append("lastProcessedMessages=").append(joiner)
                .append("}").toString();
    }

    public static class InnerEvent
    {
        private final Instant startTimestamp;
        private final Instant endTimestamp;
        private final String id;

        public InnerEvent(StoredTestEventWrapper event)
        {
            this.startTimestamp = event.getStartTimestamp();
            this.endTimestamp = event.getEndTimestamp();
            this.id = event.getId().toString();
        }

        public InnerEvent()
        {
            this.startTimestamp = null;
            this.endTimestamp = null;
            this.id = null;
        }

        public Instant getStartTimestamp() { return startTimestamp; }

        public Instant getEndTimestamp() { return endTimestamp; }

        public String getId() { return id; }
    }

    public static class InnerMessage
    {
        private final String id;
        private final Instant timestamp;
        private final Direction direction;
        private final long sequence;

        public InnerMessage(StoredMessage message)
        {
            this.id = message.getId().toString();
            this.timestamp = message.getTimestamp();
            this.direction = message.getDirection();
            this.sequence = message.getIndex();
        }

        public InnerMessage()
        {
            this.id = null;
            this.timestamp = null;
            this.direction = null;
            this.sequence = 0;
        }

        public String getId() { return id; }

        public Instant getTimestamp() { return timestamp; }

        public Direction getDirection() { return direction; }

        public long getSequence() { return sequence; }
    }
}
