/******************************************************************************
 * Copyright (c) 2009-2020, Exactpro Systems LLC
 * www.exactpro.com
 * Build Software to Test Software
 *
 * All rights reserved.
 * This is unpublished, licensed software, confidential and proprietary 
 * information which is the property of Exactpro Systems LLC or its licensors.
 ******************************************************************************/

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
			
			FIRST_EVENT_DATE = "first_event_date",
			FIRST_EVENT_TIME = "first_event_time",
			LAST_EVENT_DATE = "last_event_date",
			LAST_EVENT_TIME = "last_event_time",
			
			CONTENT = "content",
			SUCCESS = "success",
			TYPE = "type",
			PREV_ID = "prev_id",
			START_TIMESTAMP = "start_timestamp",
			END_TIMESTAMP = "end_timestamp",
			BATCH_ID = "batch_id",
			BATCH_SIZE = "batch_size",
			MESSAGES_IDS = "messages_ids",
			TEST_EVENT_ID = "test_event_id",
			IS_ROOT = "is_root",
			PARENT_ID = "parent_id";
}
