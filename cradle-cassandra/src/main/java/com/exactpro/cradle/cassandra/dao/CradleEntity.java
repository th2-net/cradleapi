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
import java.util.Objects;
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
	public static final String FIELD_CONTENT_SIZE = "z_content_size";
	public static final String FIELD_COMPRESSED_CONTENT_SIZE = "z_content_compressed_size";

	@CqlName(FIELD_COMPRESSED)
	private boolean compressed;
	@CqlName(FIELD_LABELS)
	private Set<String> labels;
	@CqlName(FIELD_CONTENT)
	private ByteBuffer content;
	@CqlName(FIELD_CONTENT_SIZE)
	private Integer contentSize;
	@CqlName(FIELD_COMPRESSED_CONTENT_SIZE)
	private Integer compressedContentSize;

	public CradleEntity () {
	}

	public CradleEntity (boolean compressed, Set<String> labels, ByteBuffer content, Integer contentSize, Integer compressedContentSize) {
		this.compressed = compressed;
		this.labels = labels;
		this.content = content;
		this.contentSize = contentSize;
		this.compressedContentSize = compressedContentSize;
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

	public Integer getContentSize() {
		return contentSize;
	}

	public Integer getCompressedContentSize() {
		return compressedContentSize;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (!(o instanceof CradleEntity)) return false;
		CradleEntity that = (CradleEntity) o;
		return isCompressed() == that.isCompressed()
				&& Objects.equals(getLabels(), that.getLabels())
				&& Objects.equals(getContent(), that.getContent())
				&& Objects.equals(getContentSize(), that.getContentSize())
				&& Objects.equals(getCompressedContentSize(), that.getCompressedContentSize());
	}

	@Override
	public int hashCode() {
		return Objects.hash(isCompressed(), getLabels(), getContent(), getContentSize(), getCompressedContentSize());
	}

	public abstract static class CradleEntityBuilder <T extends CradleEntity, B extends CradleEntityBuilder> {
		private boolean compressed;
		private Set<String> labels;
		private ByteBuffer content;
		private Integer contentSize;
		private Integer compressedContentSize;

		public CradleEntityBuilder () {
		}

		public B setCompressed (boolean compressed) {
			this.compressed = compressed;
			return (B) this;
		}

		public B setLabels (Set<String> labels) {
			this.labels = labels;
			return (B) this;
		}

		public B setContent (ByteBuffer content) {
			this.content = content;
			return (B) this;
		}

		public B setContentSize (Integer contentSize) {
			this.contentSize = contentSize;
			return (B) this;
		}

		public B setCompressedContentSize (Integer compressedContentSize) {
			this.compressedContentSize = compressedContentSize;
			return (B) this;
		}

		public boolean isCompressed() {
			return compressed;
		}

		public Set<String> getLabels() {
			return labels;
		}

		public ByteBuffer getContent() {
			return content;
		}

		public Integer getContentSize() {
			return contentSize;
		}

		public Integer getCompressedContentSize() {
			return compressedContentSize;
		}

		public abstract T build ();
	}
}