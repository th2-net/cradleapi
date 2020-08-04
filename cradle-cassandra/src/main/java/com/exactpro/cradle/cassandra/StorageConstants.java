/*
 * Copyright 2020-2020 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.cradle.cassandra;

public class StorageConstants
{
	public static final String ID = "id",
			NAME = "name",
			STREAM_NAME = "stream_name",
			INSTANCE_ID = "instance_id",
			DIRECTION = "direction",
			STORED_DATE = "stored_date",
			STORED_TIME = "stored_time",
			COMPRESSED = "compressed",
			
			MESSAGE_INDEX = "message_index",
			LAST_MESSAGE_INDEX = "last_message_index",
			FIRST_MESSAGE_DATE = "first_message_date",
			FIRST_MESSAGE_TIME = "first_message_time",
			LAST_MESSAGE_DATE = "last_message_date",
			LAST_MESSAGE_TIME = "last_message_time",
			MESSAGE_COUNT = "message_count",
			MESSAGE_TIMESTAMP = "message_timestamp",
			
			START_DATE = "start_date",
			START_TIME = "start_time",
			END_DATE = "end_date",
			END_TIME = "end_time",
			EVENT_COUNT = "event_count",
			EVENT_BATCH = "event_batch",
			
			CONTENT = "z_content",  //To make this column the last one in columns list and thus faster read meta-data
			SUCCESS = "success",
			TYPE = "type",
			START_TIMESTAMP = "start_timestamp",
			END_TIMESTAMP = "end_timestamp",
			BATCH_ID = "batch_id",
			BATCH_SIZE = "batch_size",
			MESSAGES_IDS = "messages_ids",
			TEST_EVENT_ID = "test_event_id",
			ROOT = "root",
			PARENT_ID = "parent_id";
}
