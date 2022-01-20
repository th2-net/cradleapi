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

package com.exactpro.cradle.serialization;

import com.exactpro.cradle.BookId;
import com.exactpro.cradle.Direction;
import com.exactpro.cradle.messages.StoredMessageId;

public class MessageCommonParams {

    private String sessionAlias;
    private Direction direction;
    private BookId bookId;

    public MessageCommonParams() {}

    public MessageCommonParams(StoredMessageId id) {
        this.sessionAlias = id.getSessionAlias();
        this.bookId = id.getBookId();
        this.direction = id.getDirection();
    }


    public void setBookId(BookId bookId) {
        this.bookId = bookId;
    }

    public void setBookName(String bookName) {
        this.bookId = new BookId(bookName);
    }


    public BookId getBookId() {
        return bookId;
    }

    public void setSessionAlias(String sessionAlias) {
        this.sessionAlias = sessionAlias;
    }

    public void setDirection(Direction direction) {
        this.direction = direction;
    }

    public String getSessionAlias() {
        return sessionAlias;
    }

    public Direction getDirection() {
        return direction != null ? direction : Direction.FIRST;
    }

    public String getBookName() {
        return bookId != null ? bookId.getName() : null;
    }
}
