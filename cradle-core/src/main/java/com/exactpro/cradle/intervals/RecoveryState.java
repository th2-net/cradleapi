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
import java.util.Map;
import java.util.Optional;
import java.util.StringJoiner;

public class RecoveryState
{
    private final InnerEvent lastProcessedEvent;

    private final Map<String, InnerMessage> lastProcessedMessages;

    private final long lastNumberOfEvents;

    private final long lastNumberOfMessages;

    private static final ObjectMapper MAPPER = new ObjectMapper().findAndRegisterModules()
            .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);

    private static final Logger logger = LoggerFactory.getLogger(RecoveryState.class);

    public RecoveryState(@JsonProperty("lastProcessedEvent") InnerEvent lastProcessedEvent,
                         @JsonProperty("lastProcessedMessages") Map<String, InnerMessage> lastProcessedMessages,
                         @JsonProperty("lastNumberOfEvents") long lastNumberOfEvents,
                         @JsonProperty("lastNumberOfMessages") long lastNumberOfMessages)
    {
        this.lastProcessedEvent = Optional.ofNullable(lastProcessedEvent).orElse(null);
        this.lastProcessedMessages = Optional.ofNullable(lastProcessedMessages).orElse(null);
        this.lastNumberOfEvents = lastNumberOfEvents;
        this.lastNumberOfMessages = lastNumberOfMessages;
    }

    public InnerEvent getLastProcessedEvent() { return lastProcessedEvent; }

    public Map<String, InnerMessage> getLastProcessedMessages() { return lastProcessedMessages; }

    public long getLastNumberOfEvents() { return lastNumberOfEvents; }

    public long getLastNumberOfMessages() { return lastNumberOfMessages; }

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
        StringBuilder sb = new StringBuilder();

        sb.append("RecoveryState{").append(CompressionUtils.EOL)
                .append("lastProcessedEvent=")
                .append(lastProcessedEvent).append(CompressionUtils.EOL)
                .append("lastProcessedMessages=");

        if (lastProcessedMessages != null) {
            lastProcessedMessages.forEach((k, v) -> joiner.add(k + ": "+ v));
            sb.append(joiner);
        } else {
            sb.append("null").append(CompressionUtils.EOL);
        }

        sb.append("lastNumberOfEvents=")
                .append(lastNumberOfEvents).append(CompressionUtils.EOL)
                .append("lastNumberOfMessages=")
                .append(lastNumberOfMessages).append(CompressionUtils.EOL)
                .append("}");

        return sb.toString();
    }

    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;

        result = prime * result + (lastProcessedEvent == null ? 0 : lastProcessedEvent.hashCode());

        if (lastProcessedMessages != null)
        {

            for (Map.Entry<String, InnerMessage> entry : lastProcessedMessages.entrySet())
            {
                result = prime * result + entry.getKey().hashCode();
                result = prime * result + entry.getValue().hashCode();

            }
        }
        else
        {
            result = prime * result;
        }

        result = prime * result + (int) lastNumberOfEvents;
        result = prime * result + (int) lastNumberOfMessages;

        return result;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        RecoveryState other = (RecoveryState) obj;
        if (lastProcessedEvent != other.lastProcessedEvent)
            return false;
        if (!lastProcessedMessages.equals(other.lastProcessedMessages))
            return false;
        if (lastNumberOfEvents != other.lastNumberOfEvents)
            return false;
        if (lastNumberOfMessages != other.lastNumberOfMessages)
            return false;

        return true;
    }

    public static class InnerEvent
    {
        private final Instant startTimestamp;
        private final String id;

        public InnerEvent(StoredTestEventWrapper event)
        {
            this.startTimestamp = event.getStartTimestamp();
            this.id = event.getId().toString();
        }

        public InnerEvent()
        {
            this.startTimestamp = null;
            this.id = null;
        }

        public InnerEvent(Instant startTimestamp, String id)
        {
            this.startTimestamp = startTimestamp;
            this.id = id;
        }

        @Override
        public String toString()
        {
            return new StringBuilder()
                    .append("InnerEvent{").append(CompressionUtils.EOL)
                    .append("id=").append(id).append(CompressionUtils.EOL)
                    .append("startTimestamp=").append(startTimestamp).append(CompressionUtils.EOL)
                    .append("}")
                    .toString();
        }

        @Override
        public int hashCode()
        {
            final int prime = 31;
            int result = 1;

            result = prime * result + ((startTimestamp == null) ? 0 : startTimestamp.hashCode());
            result = prime * result + ((id == null) ? 0 : id.hashCode());

            return result;
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            InnerEvent other = (InnerEvent) obj;
            if (startTimestamp != other.startTimestamp)
                return false;
            if (!id.equals(other.id))
                return false;

            return true;
        }

        public Instant getStartTimestamp() { return startTimestamp; }

        public String getId() { return id; }
    }

    public static class InnerMessage
    {
        private final String sessionAlias;
        private final Instant timestamp;
        private final Direction direction;
        private final long sequence;

        public InnerMessage(StoredMessage message)
        {
            this.sessionAlias = message.getId().toString();
            this.timestamp = message.getTimestamp();
            this.direction = message.getDirection();
            this.sequence = message.getIndex();
        }

        public InnerMessage()
        {
            this.sessionAlias = null;
            this.timestamp = null;
            this.direction = null;
            this.sequence = 0;
        }

        public InnerMessage(String sessionAlias, Instant timestamp, Direction direction, long sequence)
        {
            this.sessionAlias = sessionAlias;
            this.timestamp = timestamp;
            this.direction = direction;
            this.sequence = sequence;
        }

        @Override
        public String toString()
        {
            return new StringBuilder()
                    .append("InnerMessage{").append(CompressionUtils.EOL)
                    .append("sessionAlias=").append(sessionAlias).append(CompressionUtils.EOL)
                    .append("timestamp=").append(timestamp).append(CompressionUtils.EOL)
                    .append("direction=").append(direction).append(CompressionUtils.EOL)
                    .append("sequence=").append(sequence).append(CompressionUtils.EOL)
                    .append("}")
                    .toString();
        }

        @Override
        public int hashCode()
        {
            final int prime = 31;
            int result = 1;

            result = prime * result + ((sessionAlias == null) ? 0 : sessionAlias.hashCode());
            result = prime * result + ((timestamp == null) ? 0 : timestamp.hashCode());
            result = prime * result + ((direction == null) ? 0 : direction.hashCode());
            result = prime * result + (int) (sequence ^ (sequence >>> 32));

            return result;
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            InnerMessage other = (InnerMessage) obj;
            if (!sessionAlias.equals(other.sessionAlias))
                return false;
            if (timestamp != other.timestamp)
                return false;
            if (direction != other.direction)
                return false;
            if (sequence != other.sequence)
                return false;

            return true;
        }

        public String getSessionAlias() { return sessionAlias; }

        public Instant getTimestamp() { return timestamp; }

        public Direction getDirection() { return direction; }

        public long getSequence() { return sequence; }
    }
}
