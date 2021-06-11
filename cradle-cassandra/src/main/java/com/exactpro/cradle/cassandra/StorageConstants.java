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
			MESSAGE_DATE = "message_date",
			MESSAGE_TIME = "message_time",
			
			START_DATE = "start_date",
			START_TIME = "start_time",
			END_DATE = "end_date",
			END_TIME = "end_time",
			EVENT_COUNT = "event_count",
			EVENT_BATCH = "event_batch",
			EVENT_BATCH_METADATA = "event_batch_metadata",
			
			CONTENT = "z_content",  //To make this column the last one in columns list and thus faster read meta-data
			SUCCESS = "success",
			TYPE = "type",
			START_TIMESTAMP = "start_timestamp",
			END_TIMESTAMP = "end_timestamp",
			BATCH_ID = "batch_id",
			BATCH_SIZE = "batch_size",
			MESSAGE_IDS = "message_ids",
			MESSAGE_ID = "message_id",
			TEST_EVENT_ID = "test_event_id",
			RECOVERY_STATE_JSON = "recovery_state_json",
			INTERVAL_START_TIME = "interval_start_time",
			INTERVAL_END_TIME = "interval_end_time",
			INTERVAL_START_DATE = "interval_start_date",
			INTERVAL_END_DATE = "interval_end_date",
			INTERVAL_LAST_UPDATE_TIME = "interval_last_update_time",
			INTERVAL_LAST_UPDATE_DATE = "interval_last_update_date",
			INTERVAL_ID = "interval_id",
			CRAWLER_NAME = "crawler_name",
			CRAWLER_VERSION = "crawler_version",
			INTERVAL_PROCESSED = "interval_processed",
			ROOT = "root",
			PARENT_ID = "parent_id";
}
