/*
 * Copyright 2021-2021 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.cradle.cassandra.dao;


import java.nio.ByteBuffer;
import java.util.Set;

import com.datastax.oss.driver.api.mapper.annotations.CqlName;

/**
 * Parent for main Cradle entities stored in Cassandra
 */
public abstract class CradleEntity
{
	public static final String FIELD_COMPRESSED = "compressed";
	public static final String FIELD_LABELS = "labels";
	public static final String FIELD_CONTENT = "z_content";

	@CqlName(FIELD_COMPRESSED)
	private boolean compressed;
	@CqlName(FIELD_LABELS)
	private Set<String> labels;
	@CqlName(FIELD_CONTENT)
	private ByteBuffer content;

	public CradleEntity () {
	}

	public CradleEntity (boolean compressed, Set<String> labels, ByteBuffer content) {
		this.compressed = compressed;
		this.labels = labels;
		this.content = content;
	}


	public boolean isCompressed() {
		return compressed;
	}
	
	public Set<String> getLabels() {
		return labels;
	}
	
	public ByteBuffer getContent()
	{
		return content;
	}

	public abstract static class CradleEntityBuilder <T extends CradleEntity> {
		protected boolean compressed;
		protected Set<String> labels;
		protected ByteBuffer content;

		public CradleEntityBuilder () {
		}

		public CradleEntityBuilder setCompressed (boolean compressed) {
			this.compressed = compressed;
			return this;
		}

		public CradleEntityBuilder setLabels (Set<String> labels) {
			this.labels = labels;
			return this;
		}

		public CradleEntityBuilder setContent (ByteBuffer content) {
			this.content = content;
			return this;
		}

		public abstract T build ();
	}
}