/*
 * Copyright 2021-2022 Exactpro (Exactpro Systems Limited)
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

public class Serialization {

	public static final String INVALID_MAGIC_NUMBER_FORMAT = "Invalid magic number for %s. Got: %s. Expected %s. Probably received inconsistent data.";
	public static final String NOT_SUPPORTED_PROTOCOL_FORMAT = "Not supported %s protocol version. Supported %s. Got: %s.";

	public static class MessageBatchConst {
		public static final int MESSAGE_BATCH_MAGIC =  0xd3b7c736;
		public static final byte MESSAGE_PROTOCOL_VER =  2;
		public static final short MESSAGE_MAGIC = (short) 0x8fcd;
	}

	public static class EventBatchConst {
		public static final int EVENT_BATCH_MAGIC =  0x39983091;
		public static final byte EVENT_BATCH_PROTOCOL_VER =  2;
		public static final short EVENT_BATCH_ENT_MAGIC = (short) 0xf3de;
	}
	
	
	public static class EventMessageIdsConst {

		public static final byte VERSION = 1;
		public static final	byte SINGLE_EVENT_LINKS = 1;
		public static final	byte BATCH_LINKS = 2;
		public static final	byte END_OF_DATA = 0;
		public static final	byte DIRECTION_FIRST = 1;
		public static final	byte DIRECTION_SECOND = 2;
		
	}	
	
}
