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

package com.exactpro.cradle.testevents;

import java.time.Instant;

import com.exactpro.cradle.books.BookId;

/**
 * Interface to access basic metadata fields of test event
 */
public interface BasicTestEvent
{
	StoredTestEventId getId();
	String getName();
	String getType();
	StoredTestEventId getParentId();
	BookId getBookId();
	String getScope();
	Instant getStartTimestamp();
}
