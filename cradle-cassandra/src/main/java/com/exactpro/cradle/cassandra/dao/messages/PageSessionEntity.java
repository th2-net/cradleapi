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

package com.exactpro.cradle.cassandra.dao.messages;

import com.datastax.oss.driver.api.mapper.annotations.ClusteringColumn;
import com.datastax.oss.driver.api.mapper.annotations.CqlName;
import com.datastax.oss.driver.api.mapper.annotations.Entity;
import com.datastax.oss.driver.api.mapper.annotations.PartitionKey;
import com.exactpro.cradle.PageId;
import com.exactpro.cradle.messages.StoredMessageId;



@Entity
public class PageSessionEntity
{
	public static final String FIELD_PAGE = "page",
			FIELD_SESSION_ALIAS = "session_alias",
			FIELD_DIRECTION = "direction";

	@PartitionKey(0)
	@CqlName(FIELD_PAGE)
	private String page;

	@ClusteringColumn(0)
	@CqlName(FIELD_SESSION_ALIAS)
	private String sessionAlias;

	@ClusteringColumn(1)
	@CqlName(FIELD_DIRECTION)
	private String direction;

	public PageSessionEntity()
	{
	}
	
	public PageSessionEntity(StoredMessageId messageId, PageId pageId)
	{
		setPage(pageId.getName());
		setSessionAlias(messageId.getSessionAlias());
		setDirection(messageId.getDirection().getLabel());
	}
	
	public String getPage()
	{
		return page;
	}

	public void setPage(String page)
	{
		this.page = page;
	}

	public String getSessionAlias()
	{
		return sessionAlias;
	}

	public void setSessionAlias(String sessionAlias)
	{
		this.sessionAlias = sessionAlias;
	}

	public String getDirection()
	{
		return direction;
	}

	public void setDirection(String direction)
	{
		this.direction = direction;
	}
}
